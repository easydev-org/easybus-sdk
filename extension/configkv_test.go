package extension

import (
	"testing"

	"github.com/easydev-org/easybus-sdk/bus"
)

// TestApplyConfigKVBareValue is the regression test for configuration that was
// "applied" and never stored.

// The watch delivered each entry, applyConfigKV ran, and nothing landed in the
// store: the value-decoding step tried `json.Unmarshal` on the raw text, and a URL
// or a token is not JSON. It returned early, silently, so the extension reported
// every knob unset while the agent and the console both insisted the value was
// saved.

// A config value is ultimately a string. JSON is only one of the ways it may be
// encoded, so a bare value must be taken verbatim rather than dropped.
func TestApplyConfigKVBareValue(t *testing.T) {
	cases := []struct {
		name  string
		value string
		want  any
	}{
		{"bare url", "http://easyops.dev.svc.cluster.local:80", "http://easyops.dev.svc.cluster.local:80"},
		{"bare token", "dev-service-token", "dev-service-token"},
		{"bare plain word", "dev", "dev"},
		{"json string", `"quoted-value"`, "quoted-value"},
		{"envelope", `{"r":7,"v":"enveloped"}`, "enveloped"},
		{"json number", `42`, float64(42)},
	}

	for _, c := range cases {
		e := &Extension{cfg: Config{ID: "ops"}, configStore: newConfigStore()}
		e.applyConfigKV(bus.KvEvent{Key: "t.alice.ops.easyops-url", Value: c.value})

		got, ok := e.configStore.global["alice"]["easyops-url"]
		if !ok {
			t.Errorf("%s: value not stored (watch reported the key as applied)", c.name)
			continue
		}
		if got != c.want {
			t.Errorf("%s: stored %#v, want %#v", c.name, got, c.want)
		}
	}
}
