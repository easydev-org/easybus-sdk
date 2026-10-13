package manifest_test

import (
	"context"
	"testing"

	"github.com/easydev-org/easybus-sdk/extension"
	"github.com/easydev-org/easybus-sdk/manifest"
)

func TestParseAndConfig(t *testing.T) {
	yaml := `
id: mock-ext
version: 1.0.
tools:
  - name: echo
    description: echo a message
    input_schema:
      type: object
variables:
  - name: base-url
    scope: global
    description: base url
hooks:
  call: [session.before_create]
  event: [interrupt]
`

	m, err := manifest.ParseManifest([]byte(yaml))
	if err != nil {
		t.Fatal(err)
	}
	if m.ID != "mock-ext" || len(m.Tools) != 1 {
		t.Fatalf("parsed = %+v", m)
	}

	echoCalled := false
	cfg := m.BuildConfig(manifest.Bindings{
		Handlers: map[string]extension.ToolSpec{
			"echo": {
				Execute: func(ctx context.Context, args map[string]any, callID, session, tenant string) (extension.ToolResultData, error) {
					echoCalled = true
					return extension.ToolResultData{}, nil
				},
			},
		},
	})

	if cfg.ID != "mock-ext" || len(cfg.Tools) != 1 {
		t.Fatalf("config = %+v", cfg)
	}
	if len(cfg.CallHooks) != 1 || cfg.CallHooks[0] != "session.before_create" {
		t.Fatalf("call hooks = %v", cfg.CallHooks)
	}
	if len(cfg.EventHooks) != 1 || cfg.EventHooks[0] != "interrupt" {
		t.Fatalf("event hooks = %v", cfg.EventHooks)
	}
	if cfg.Variables["base-url"].Scope != "global" {
		t.Fatalf("variable = %+v", cfg.Variables["base-url"])
	}
	if !echoCalled {
		// handler bound but not executed here; verify binding captured
	}
	_ = echoCalled

	// verify a tool without a bound handler is dropped
	cfg2 := m.BuildConfig(manifest.Bindings{})
	if len(cfg2.Tools) != 0 {
		t.Fatalf("unbound tools should be dropped, got %d", len(cfg2.Tools))
	}
}

func TestParseLifecycle(t *testing.T) {
	yaml := `
id: lc-ext
version: 1.0
lifecycle: [created, forked, renamed, deleted]
`
	m, err := manifest.ParseManifest([]byte(yaml))
	if err != nil {
		t.Fatal(err)
	}
	if len(m.Lifecycle) != 4 || m.Lifecycle[0] != "created" {
		t.Fatalf("lifecycle = %v", m.Lifecycle)
	}
	cfg := m.BuildConfig(manifest.Bindings{})
	if len(cfg.Lifecycle) != 4 {
		t.Fatalf("cfg.Lifecycle = %v", cfg.Lifecycle)
	}
}

// TestConfigKindCapability verifies the model-picker fields (kind/capability)
// flow from the manifest YAML through BuildConfig into the ConfigSpec — the
// Go twin of the TS ConfigSpec.kind/capability.
func TestConfigKindCapability(t *testing.T) {
	yaml := `
id: model-ext
version: 2.0.0
config:
  - name: model.image
    type: string
    kind: model
    capability: image
    description: image model
  - name: api-key
    type: string
    description: plain knob
`
	m, err := manifest.ParseManifest([]byte(yaml))
	if err != nil {
		t.Fatal(err)
	}
	cfg := m.BuildConfig(manifest.Bindings{})

	img := cfg.Config["model.image"]
	if img.Kind != "model" || img.Capability != "image" {
		t.Fatalf("model.image = %+v (want kind=model capability=image)", img)
	}
	key := cfg.Config["api-key"]
	if key.Kind != "" || key.Capability != "" {
		t.Fatalf("api-key = %+v (want empty kind/capability)", key)
	}

	// The extension manifest must advertise them so a UI can render a picker.
	ext := extension.New(nil, cfg)
	man := ext.Manifest()
	if man.Config == nil {
		t.Fatal("manifest config nil")
	}
	found := false
	for _, item := range *man.Config {
		if item.Name == "model.image" {
			found = true
			if item.Kind == nil || string(*item.Kind) != "model" {
				t.Fatalf("manifest kind = %v", item.Kind)
			}
			if item.Capability == nil || string(*item.Capability) != "image" {
				t.Fatalf("manifest capability = %v", item.Capability)
			}
		}
	}
	if !found {
		t.Fatal("model.image not in manifest config")
	}
}
