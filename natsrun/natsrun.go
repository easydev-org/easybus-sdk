// Package natsrun launches a local nats-server as a child process and
// returns its URL. It is the "embedded" deployment form of the single NATS
// transport: the agent side spawns the system-installed nats-server binary
// (memory or file JetStream storage); extensions always connect as clients.
//
// Binary resolution: Config.Binary → EASYBUS_NATS_SERVER_BIN → "nats-server"
// (PATH lookup).
package natsrun

import (
	"fmt"
	"net"
	"os"
	"os/exec"
	"sync"
	"syscall"
	"time"

	"github.com/nats-io/nats.go"
)

// Storage selects the JetStream persistence of the pre-created stores.
type Storage string

const (
	// Memory keeps everything in RAM: nothing survives a stop.
	Memory Storage = "memory"
	// File persists to StoreDir: state survives restarts.
	File Storage = "file"
)

// Config configures the spawned server.
type Config struct {
	// Binary is the nats-server path. Default: EASYBUS_NATS_SERVER_BIN or
	// "nats-server" from PATH.
	Binary string
	// Storage is Memory (default) or File. File requires StoreDir.
	Storage Storage
	// StoreDir persists JetStream state (File storage). When empty in File
	// mode, a temporary directory is used (ephemeral despite File storage).
	StoreDir string
	// Port is the client port; 0 picks a free random port.
	Port int
}

// Server is a running child nats-server.
type Server struct {
	cmd      *exec.Cmd
	url      string
	port     int
	storeDir string
	tempDir  bool
	once     sync.Once
}

// Start spawns nats-server, waits until it accepts client connections, and
// (for Memory storage) pre-creates the protocol's streams/buckets with
// memory storage so the hot paths never touch disk. Lazily created extras
// fall back to file storage inside the (temporary) store dir — still
// ephemeral across Stop.
func Start(cfg Config) (*Server, error) {
	bin := cfg.Binary
	if bin == "" {
		bin = os.Getenv("EASYBUS_NATS_SERVER_BIN")
	}
	if bin == "" {
		bin = "nats-server"
	}
	resolved, err := exec.LookPath(bin)
	if err != nil {
		return nil, fmt.Errorf("natsrun: nats-server binary not found (set EASYBUS_NATS_SERVER_BIN): %w", err)
	}

	port := cfg.Port
	if port == 0 {
		port, err = freePort()
		if err != nil {
			return nil, err
		}
	}

	s := &Server{port: port}
	args := []string{
		"-a", "127.0.0.1",
		"-p", fmt.Sprintf("%d", port),
		"-js",
	}
	if cfg.Storage == File {
		dir := cfg.StoreDir
		if dir == "" {
			dir, err = os.MkdirTemp("", "easybus-nats-*")
			if err != nil {
				return nil, err
			}
			s.tempDir = true
		} else {
			if err := os.MkdirAll(dir, 0o755); err != nil {
				return nil, err
			}
		}
		s.storeDir = dir
		args = append(args, "-sd", dir)
	} else {
		// Memory mode: still pass a temp store_dir because lazily created
		// file-storage buckets need somewhere to live; removed on Stop.
		dir, err := os.MkdirTemp("", "easybus-nats-mem-*")
		if err != nil {
			return nil, err
		}
		s.storeDir = dir
		s.tempDir = true
		args = append(args, "-sd", dir)
	}

	s.cmd = exec.Command(resolved, args...)
	out, err := s.cmd.StderrPipe()
	if err != nil {
		return nil, err
	}
	if err := s.cmd.Start(); err != nil {
		return nil, fmt.Errorf("natsrun: start %s: %w", resolved, err)
	}
	_ = out // keep the pipe so the child never blocks on log writes

	s.url = fmt.Sprintf("nats://127.0.0.1:%d", port)
	if err := s.waitReady(10 * time.Second); err != nil {
		_ = s.Stop()
		return nil, err
	}

	if cfg.Storage == Memory {
		if err := s.precreateMemoryStream(); err != nil {
			_ = s.Stop()
			return nil, err
		}
	}
	return s, nil
}

// URL is the client connect URL (nats://127.0.0.1:<port>).
func (s *Server) URL() string { return s.url }

// Port is the bound client port.
func (s *Server) Port() int { return s.port }

// Stop terminates the child process and removes a temporary store dir.
func (s *Server) Stop() error {
	var firstErr error
	s.once.Do(func() {
		if s.cmd != nil && s.cmd.Process != nil {
			_ = s.cmd.Process.Signal(syscall.SIGTERM)
			done := make(chan struct{})
			go func() { _ = s.cmd.Wait(); close(done) }()
			select {
			case <-done:
			case <-time.After(3 * time.Second):
				_ = s.cmd.Process.Kill()
				<-done
			}
		}
		if s.tempDir && s.storeDir != "" {
			if err := os.RemoveAll(s.storeDir); err != nil {
				firstErr = err
			}
		}
	})
	return firstErr
}

func (s *Server) waitReady(timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", s.port), 200*time.Millisecond)
		if err == nil {
			_ = conn.Close()
			return nil
		}
		time.Sleep(50 * time.Millisecond)
	}
	return fmt.Errorf("natsrun: server on port %d not ready within %s", s.port, timeout)
}

// precreateMemoryStream creates the protocol's mailbox/events stream with
// MemoryStorage so the hot queue path never touches disk in Memory mode.
// KV/object buckets are created lazily by the transport (file storage inside
// the temporary store dir — still ephemeral across Stop).
func (s *Server) precreateMemoryStream() error {
	nc, err := nats.Connect(s.url)
	if err != nil {
		return err
	}
	defer nc.Close()
	js, err := nc.JetStream()
	if err != nil {
		return err
	}
	for _, sc := range []*nats.StreamConfig{
		{Name: "EASYBUS_MAILBOX", Subjects: []string{"easybus.*.mailbox.>"}, Storage: nats.MemoryStorage, MaxAge: 24 * time.Hour},
		{Name: "EASYBUS_EVENTS", Subjects: []string{"easybus.*.session.events.>"}, Storage: nats.MemoryStorage, MaxAge: 24 * time.Hour},
		{Name: "EASYBUS_DLQ", Subjects: []string{"easybus.*.dlq.>"}, Storage: nats.MemoryStorage, MaxAge: 24 * time.Hour},
	} {
		if _, err := js.AddStream(sc); err != nil {
			return err
		}
	}
	return nil
}

func freePort() (int, error) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return 0, err
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port, nil
}
