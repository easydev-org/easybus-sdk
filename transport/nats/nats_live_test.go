package nats

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/easydev-org/easybus-sdk/agent"
	"github.com/easydev-org/easybus-sdk/extension"
	"github.com/easydev-org/easybus-sdk/natsrun"
)

// TestEchoFlowOverNats runs against EASYBUS_NATS_URL when set (live-cluster
// opt-in); otherwise an ephemeral local broker.
func TestEchoFlowOverNats(t *testing.T) {
	url := os.Getenv("EASYBUS_NATS_URL")
	if url == "" {
		s, err := natsrun.Start(natsrun.Config{Storage: natsrun.Memory})
		if err != nil {
			t.Skipf("no nats-server: %v", err)
		}
		t.Cleanup(func() { _ = s.Stop() })
		url = s.URL()
	}
	bus, err := Connect(url)
	if err != nil {
		t.Skipf("nats unavailable: %v", err)
	}
	defer bus.Close()

	extBus, err := Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	defer extBus.Close()

	ext := extension.New(extBus, extension.Config{
		ID:      "nats-ext",
		Version: "1.0",
		Tools: map[string]extension.ToolSpec{
			"echo": {
				Description: "echo a message",
				Execute: func(ctx context.Context, args map[string]any, callID, session, tenant string) (extension.ToolResultData, error) {
					msg, _ := args["msg"].(string)
					return extension.ToolResultData{Content: "echo: " + msg}, nil
				},
			},
		},
	})
	ctx := context.Background()
	if err := ext.Serve(ctx); err != nil {
		t.Fatal(err)
	}
	defer ext.Close()

	time.Sleep(200 * time.Millisecond)

	a := agent.New(bus)
	tr, err := a.CallTool(ctx, "t1", "s1", "nats-ext", "echo", "c1", map[string]any{"msg": "hi"})
	if err != nil {
		t.Fatal(err)
	}
	if tr.Content != "echo: hi" {
		t.Fatalf("result = %+v", tr)
	}
}
