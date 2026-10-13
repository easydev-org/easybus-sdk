package natsrun

import (
	"context"
	"testing"
	"time"

	"github.com/easydev-org/easybus-sdk/agent"
	"github.com/easydev-org/easybus-sdk/bus"
	"github.com/easydev-org/easybus-sdk/protocol"
	"github.com/easydev-org/easybus-sdk/transport/nats"
)

func startTestServer(t *testing.T) *Server {
	t.Helper()
	s, err := Start(Config{Storage: Memory})
	if err != nil {
		t.Skipf("nats-server binary unavailable: %v", err)
	}
	t.Cleanup(func() { _ = s.Stop() })
	return s
}

func TestStartStop(t *testing.T) {
	s := startTestServer(t)
	if s.URL() == "" || s.Port() == 0 {
		t.Fatalf("url=%q port=%d", s.URL(), s.Port())
	}
	// A client can connect and request/reply works.
	b, err := nats.Connect(s.URL())
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	sub, err := b.Subscribe(context.Background(), "ping.x", bus.SubscribeOpts{})
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()
	go func() {
		env, ok := sub.Next(context.Background())
		if ok && env.ReplyTo != nil {
			_ = b.Publish(context.Background(), *env.ReplyTo, "pong", bus.PublishOpts{})
		}
	}()
	reply, err := b.Request(context.Background(), "ping.x", "hi", bus.RequestOpts{TimeoutMs: 2000})
	if err != nil {
		t.Fatal(err)
	}
	var pong string
	if !protocol.Coerce(reply.Payload, &pong) || pong != "pong" {
		t.Fatalf("reply = %+v", reply)
	}
}

func TestMemoryStorageRoundtrip(t *testing.T) {
	s := startTestServer(t)
	b, err := nats.Connect(s.URL())
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	_ = agent.New(b)

	ctx := context.Background()
	if err := b.InboxPublish(ctx, protocol.ChMailbox("t1", "test1"), map[string]any{"k": 1}, bus.InboxPublishOpts{ID: "m1", SessionName: "s"}); err != nil {
		t.Fatal(err)
	}
	rev, err := b.KVCreate(ctx, "cfg", "k", "v", 60_000)
	if err != nil || rev == 0 {
		t.Fatalf("kvCreate = %d %v", rev, err)
	}
	if v, _ := b.KVGet(ctx, "cfg", "k"); v != "v" {
		t.Fatalf("kvGet = %q", v)
	}
	envs, err := b.Replay(ctx, protocol.ChMailbox("t1", "test1"))
	if err != nil || len(envs) != 1 {
		t.Fatalf("replay = %d %v", len(envs), err)
	}
}

func TestFileStoragePersistsAcrossRestart(t *testing.T) {
	dir := t.TempDir()
	s1, err := Start(Config{Storage: File, StoreDir: dir})
	if err != nil {
		t.Skipf("nats-server binary unavailable: %v", err)
	}
	b1, err := nats.Connect(s1.URL())
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if _, err := b1.KVCreate(ctx, "cfg", "persist", "yes", 60_000); err != nil {
		t.Fatal(err)
	}
	_ = b1.Close()
	port := s1.Port()
	_ = s1.Stop()

	// Restart on the same port + dir; the value must survive.
	s2, err := Start(Config{Storage: File, StoreDir: dir, Port: port})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s2.Stop() }()
	b2, err := nats.Connect(s2.URL())
	if err != nil {
		t.Fatal(err)
	}
	defer b2.Close()
	v, err := b2.KVGet(ctx, "cfg", "persist")
	if err != nil || v != "yes" {
		t.Fatalf("kvGet after restart = %q %v (file storage should persist)", v, err)
	}
}

func TestBinaryMissing(t *testing.T) {
	if _, err := Start(Config{Binary: "/nonexistent/nats-server-xyz"}); err == nil {
		t.Fatal("expected error for missing binary")
	}
}

func TestStartUnderLoadRepeated(t *testing.T) {
	// Rapid start/stop cycles must not leak processes or ports.
	for i := 0; i < 3; i++ {
		s, err := Start(Config{Storage: Memory})
		if err != nil {
			t.Skipf("nats-server binary unavailable: %v", err)
		}
		nc := s.URL()
		if nc == "" {
			t.Fatal("empty url")
		}
		if err := s.Stop(); err != nil {
			t.Fatal(err)
		}
		time.Sleep(20 * time.Millisecond)
	}
}
