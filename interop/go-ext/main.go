// Go-side interop extension: a plain extension that serves an `echo` tool and
// a `session` tool over NATS, for cross-language interop tests.
//
//	go run ./interop/go-ext
//
// Requires NATS_URL (defaults to the shared dev cluster).
package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"

	easybus "github.com/easydev-org/easybus-sdk"
	"github.com/easydev-org/easybus-sdk/extension"
	"github.com/easydev-org/easybus-sdk/transport/nats"
)

func main() {
	bus, err := nats.Connect(natsURL())
	if err != nil {
		fmt.Println("[go-ext] connect failed:", err)
		os.Exit(1)
	}

	ext := extension.New(bus, extension.Config{
		ID:      "go-ext",
		Version: "1.0",
		Config: map[string]extension.ConfigSpec{
			"poll-interval": {
				Description: "seconds between polls",
				Type:        "number",
				Default:     float64(30),
			},
		},
		OnConfigChange: func(ctx context.Context, name string, value any, session string, get func(name, session string) any) error {
			fmt.Printf("[go-ext] config applied: %s=%v\n", name, value)
			return nil
		},
		Tools: map[string]extension.ToolSpec{
			"echo": {
				Description: "echo content back",
				Execute: func(ctx context.Context, args map[string]any, callID, session, tenant string) (extension.ToolResultData, error) {
					msg, _ := args["msg"].(string)
					return extension.ToolResultData{Content: "go-ext echo: " + msg}, nil
				},
			},
			"session": {
				Description: "echo the session name",
				Execute: func(ctx context.Context, args map[string]any, callID, session, tenant string) (extension.ToolResultData, error) {
					return extension.ToolResultData{Content: "session=" + session}, nil
				},
			},
			"fail": {
				Description: "always returns a business error",
				Execute: func(ctx context.Context, args map[string]any, callID, session, tenant string) (extension.ToolResultData, error) {
					return extension.ToolResultData{}, &extension.TypedError{
						Code:    easybus.ToolResultErrorCodeBusiness,
						Message: "deliberate failure",
					}
				},
			},
		},
		EventHooks: []string{"interop.event"},
		OnEventHook: func(ctx context.Context, hook, session string, payload any, tenant string) error {
			fmt.Printf("[go-ext] event hook=%s session=%s tenant=%s payload=%v\n", hook, session, tenant, payload)
			return nil
		},
	})

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	if err := ext.Serve(ctx); err != nil {
		fmt.Println("[go-ext] serve failed:", err)
		os.Exit(1)
	}
	fmt.Println("[go-ext] serving go-ext over", natsURL())
	<-ctx.Done()
	fmt.Println("[go-ext] shutting down")
}

func natsURL() string {
	if u := os.Getenv("NATS_URL"); u != "" {
		return u
	}
	return "nats://nats.develop.svc.cluster.local:4222"
}
