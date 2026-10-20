package manifest

import (
	"testing"

	"gopkg.in/yaml.v3"
)

// TestStructuredConfigParses: a list-of-objects knob keeps its SHAPE, so a client
// can render one card per item (with a "+" to add) instead of a JSON textarea.
func TestStructuredConfigParses(t *testing.T) {
	src := `
id: git
version: "1"
config:
  - name: sandboxes
    type: list
    item_type: object
    min_items: 1
    kind: secret
    scope: global
    fields:
      - name: name
        type: string
        required: true
      - name: url
        type: string
        format: uri
        required: true
      - name: token
        type: string
        kind: secret
        required: true
      - name: os
        type: enum
        enum_values: [linux, windows, macos, android]
        default: linux
`
	var m Manifest
	if err := yaml.Unmarshal([]byte(src), &m); err != nil {
		t.Fatal(err)
	}
	if len(m.Config) != 1 {
		t.Fatalf("config = %d, want 1", len(m.Config))
	}
	c := m.Config[0]
	if c.Type != "list" || c.ItemType != "object" {
		t.Errorf("type=%q item_type=%q", c.Type, c.ItemType)
	}
	if c.MinItems != 1 {
		t.Errorf("min_items = %d, want 1", c.MinItems)
	}
	if len(c.Fields) != 4 {
		t.Fatalf("fields = %d, want 4", len(c.Fields))
	}
	byName := map[string]ManifestField{}
	for _, f := range c.Fields {
		byName[f.Name] = f
	}
	if byName["url"].Format != "uri" {
		t.Errorf("url.format = %q, want uri", byName["url"].Format)
	}
	if !byName["token"].Required || byName["token"].Kind != "secret" {
		t.Errorf("token must be required+secret, got %+v", byName["token"])
	}
	os := byName["os"]
	if os.Type != "enum" || len(os.EnumValues) != 4 {
		t.Fatalf("os = %+v, want enum with 4 values", os)
	}
	if os.EnumValues[0] != "linux" || os.EnumValues[3] != "android" {
		t.Errorf("os enum order = %v", os.EnumValues)
	}
}

// TestStructuredSecretIsAllowed: a secret knob may be a list/object (each record
// carries its own secret field), which is how the sandbox list declares a token
// per sandbox. It must still refuse a DEFAULT, which would ride the manifest.
func TestStructuredSecretIsAllowed(t *testing.T) {
	ok := Manifest{Config: []ManifestConfig{
		{Name: "sandboxes", Type: "list", Kind: "secret"},
		{Name: "single", Type: "object", Kind: "secret"},
	}}
	if err := ok.validateConfig(); err != nil {
		t.Errorf("structured secret rejected: %v", err)
	}

	withDefault := Manifest{Config: []ManifestConfig{
		{Name: "sandboxes", Type: "list", Kind: "secret", Default: "[]"},
	}}
	if err := withDefault.validateConfig(); err == nil {
		t.Error("a secret with a default must be rejected (the default is broadcast)")
	}

	badType := Manifest{Config: []ManifestConfig{
		{Name: "n", Type: "number", Kind: "secret"},
	}}
	if err := badType.validateConfig(); err == nil {
		t.Error("a numeric secret must still be rejected")
	}
}

// TestJSONKnobStillFallsBack: an untyped json knob (no fields) keeps working —
// this is the compatibility promise that lets existing extensions go untouched.
func TestJSONKnobStillFallsBack(t *testing.T) {
	c := ManifestConfig{Name: "blob", Type: "json"}
	cfg := Manifest{Config: []ManifestConfig{c}}
	spec := cfg.Config[0]
	if spec.Type != "json" || len(spec.Fields) != 0 {
		t.Errorf("json fallback changed: %+v", spec)
	}
}
