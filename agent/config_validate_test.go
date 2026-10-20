package agent

import (
	"testing"

	easybus "github.com/easydev-org/easybus-sdk"
)

// TestValidateConfigValueStructured covers the `list` / `object` knob types.
//
// These were MISSING from the validator, so every structured knob — sandboxes,
// servers — could not be set at all through SetConfig: it failed with
// `unknown config type "list"`, which names the manifest instead of this
// function and sent the reader hunting in the wrong file.
func TestValidateConfigValueStructured(t *testing.T) {
	itemType := "object"
	list := &easybus.ExtensionConfigItem{Type: "list", ItemType: &itemType}
	obj := &easybus.ExtensionConfigItem{Type: "object"}

	// JSON text is what a caller holds after reading a value back.
	if msg := validateConfigValue(list, `[{"name":"pod-a","url":"http://w"}]`); msg != "" {
		t.Errorf("a JSON list must be accepted: %s", msg)
	}
	// A decoded slice must work too.
	if msg := validateConfigValue(list, []any{map[string]any{"name": "a"}}); msg != "" {
		t.Errorf("a decoded slice must be accepted: %s", msg)
	}
	if msg := validateConfigValue(obj, `{"host":"gitea.example.com"}`); msg != "" {
		t.Errorf("a JSON object must be accepted: %s", msg)
	}

	// Garbage must still be refused, and the message must say what was wrong.
	if msg := validateConfigValue(list, "not json"); msg == "" {
		t.Error("an unparseable string must be rejected")
	}
	if msg := validateConfigValue(list, nil); msg == "" {
		t.Error("nil must be rejected")
	}
}
