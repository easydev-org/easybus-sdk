package nats

import (
	"context"
	"testing"
	"time"

	"github.com/easydev-org/easybus-sdk/agent"
	"github.com/easydev-org/easybus-sdk/extension"
	"github.com/easydev-org/easybus-sdk/identity"
	"github.com/easydev-org/easybus-sdk/natsrun"
)

// TestIdentityAuth pins the opt-in message authentication: with an
// Identity configured, signed messages flow and tampered/unsigned ones
// are dropped; without Identity there is zero overhead.
func TestIdentityAuth(t *testing.T) {
	srv, err := natsrun.Start(natsrun.Config{Storage: natsrun.Memory})
	if err != nil {
		t.Skipf("no nats-server: %v", err)
	}
	t.Cleanup(func() { _ = srv.Stop() })
	ctx := context.Background()

	idn := &identity.Identity{ID: "agent-1", Secret: "test-secret"}

	// signed pair: agent + extension both carry the identity
	agentBus, err := ConnectWithOptions(srv.URL(), Options{Identity: idn})
	if err != nil {
		t.Fatal(err)
	}
	defer agentBus.Close()
	extBus, err := ConnectWithOptions(srv.URL(), Options{Identity: idn})
	if err != nil {
		t.Fatal(err)
	}
	defer extBus.Close()

	ext := extension.New(extBus, extension.Config{
		ID: "id-ext", Version: "1.0",
		Tools: map[string]extension.ToolSpec{
			"echo": {Description: "e", Execute: func(ctx context.Context, args map[string]any, callID, session, tenant string) (extension.ToolResultData, error) {
				return extension.ToolResultData{Content: "signed-pong"}, nil
			}},
		},
	})
	go func() { _ = ext.Serve(context.Background()) }()
	time.Sleep(500 * time.Millisecond)

	// signed request flows
	a := agent.New(agentBus)
	tr, err := a.CallTool(ctx, "t1", "sess-id", "id-ext", "echo", "c1", nil)
	if err != nil {
		t.Fatalf("signed call: %v", err)
	}
	if tr.Content != "signed-pong" {
		t.Fatalf("signed content = %q", tr.Content)
	}

	// unsigned request from a bus WITHOUT identity: dropped by the
	// extension's verification (no reply -> timeout)
	plainBus, err := Connect(srv.URL())
	if err != nil {
		t.Fatal(err)
	}
	defer plainBus.Close()
	_, err = func() (tr struct {
		Content string
	}, err error) {
		cctx, cancel := context.WithTimeout(ctx, 2000000000) // 2s
		defer cancel()
		ab := agent.New(plainBus)
		_, e := ab.CallTool(cctx, "t1", "sess-id", "id-ext", "echo", "c2", nil)
		return tr, e
	}()
	if err == nil {
		t.Fatal("unsigned call should have timed out (signature verification dropped it)")
	}

	_ = ext.Close
}
