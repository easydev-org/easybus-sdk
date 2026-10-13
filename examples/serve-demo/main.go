// wdbidi-style sample built on the easybus SDK — the migration template
// for the four rucoder-neo extensions. Compare with
// rucoder-neo/wdbidi-extension/main.go (54 lines on abep-sdk): the shape is
// identical, only the import paths and option names differ.
//
// This file doubles as a compile/run check of the migration surface:
//
//	go run ./examples/serve-demo
package main

import (
	"context"
	_ "embed"
	"log/slog"
	"os"

	easybus "github.com/easydev-org/easybus-sdk"
	"github.com/easydev-org/easybus-sdk/extension"
	"github.com/easydev-org/easybus-sdk/manifest"
	"github.com/easydev-org/easybus-sdk/transport/nats"
)

//go:embed demo.manifest.yaml
var manifestYaml []byte

func main() {
	log := slog.Default().With("svc", "demo-ext")

	nbusAddr := envOr("NATS_URL", "nats://nats.develop.svc.cluster.local:4222")
	nbus, err := nats.Connect(nbusAddr)
	if err != nil {
		log.Error("nats connect failed", "err", err)
		os.Exit(1)
	}

	m, err := manifest.ParseManifest(manifestYaml)
	if err != nil {
		log.Error("load manifest failed", "err", err)
		os.Exit(1)
	}

	cfg := m.BuildConfig(manifest.Bindings{
		Handlers: map[string]extension.ToolSpec{
			"ping": {
				Execute: func(ctx context.Context, args map[string]any, callID, session, tenant string) (extension.ToolResultData, error) {
					return extension.ToolResultData{Content: "pong " + easybus.ArgString(args, "msg")}, nil
				},
			},
		},
		OnLifecycle: func(ctx context.Context, ev easybus.LifecycleEvent, tenant string) error {
			log.Info("lifecycle", "kind", string(ev.Kind), "session", ev.SessionName)
			return nil
		},
	})

	if err := extension.Serve(extension.New(nbus, cfg), extension.ServeOptions{
		Run: func(ctx context.Context, ext *extension.Extension) {
			log.Info("listening", "nats", nbusAddr)
			_ = ext.PublishSessionEvent(ctx, "demo", "demo", "started", nil)
		},
	}); err != nil {
		log.Error("serve failed", "err", err)
		os.Exit(1)
	}
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
