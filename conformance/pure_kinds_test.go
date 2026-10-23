package conformance

import (
	"testing"

	"github.com/easydev-org/easybus-sdk/extension"
	"github.com/easydev-org/easybus-sdk/protocol"
)

// Runners for the pure-computation shared kinds.
//
// These need no broker: they compare this SDK's functions to values pinned by the reference
// implementation. That is deliberate — a subject or matching-rule divergence is otherwise
// invisible until a message fails to arrive, and waiting for an integration run to notice is
// waiting too long.

func runPermissionMatchingCase(t *testing.T, c sharedCase) {
	t.Helper()
	if len(c.Cases) == 0 {
		t.Fatalf("%s: no cases", c.ID)
	}
	for i, mc := range c.Cases {
		grant := stringMap(mc["grant"])
		target := stringMap(mc["target"])
		want, _ := mc["want"].(bool)
		if got := extension.Matches(grant, target); got != want {
			t.Errorf("case %d (%v): matches(%v, %v) = %v, want %v",
				i, mc["why"], grant, target, got, want)
		}
	}
}

func runChannelMatchingCase(t *testing.T, c sharedCase) {
	t.Helper()
	if len(c.Cases) == 0 {
		t.Fatalf("%s: no cases", c.ID)
	}
	for i, mc := range c.Cases {
		pattern, _ := mc["pattern"].(string)
		topic, _ := mc["topic"].(string)
		want, _ := mc["want"].(bool)
		if got := protocol.MatchSubject(pattern, topic); got != want {
			t.Errorf("case %d (%v): match(%q, %q) = %v, want %v",
				i, mc["why"], pattern, topic, got, want)
		}
	}
}

func runChannelSubjectsCase(t *testing.T, c sharedCase) {
	t.Helper()
	if len(c.Cases) == 0 {
		t.Fatalf("%s: no cases", c.ID)
	}
	for i, mc := range c.Cases {
		why := mc["why"]
		if b, ok := mc["build"].(map[string]any); ok {
			got, err := protocol.ChChannel(str(b["tenant"]), str(b["channel"]), str(b["topic"]))
			if err != nil {
				t.Errorf("case %d (%v): unexpected error %v", i, why, err)
				continue
			}
			if want := str(mc["want"]); got != want {
				t.Errorf("case %d (%v): ChChannel = %q, want %q", i, why, got, want)
			}
			continue
		}
		if p, ok := mc["parse"].(string); ok {
			tenant, channel, topic, ok2 := protocol.ParseChannelSubject(p)
			if mc["want"] == nil {
				if ok2 {
					t.Errorf("case %d (%v): parse(%q) should be nil, got %q/%q/%q",
						i, why, p, tenant, channel, topic)
				}
				continue
			}
			want := mc["want"].([]any)
			got := []string{tenant, channel, topic}
			for j := range want {
				if got[j] != str(want[j]) {
					t.Errorf("case %d (%v): parse(%q)[%d] = %q, want %v",
						i, why, p, j, got[j], want[j])
				}
			}
			continue
		}
		if r, ok := mc["reject"].(map[string]any); ok {
			if _, err := protocol.ChChannel(str(r["tenant"]), str(r["channel"]), str(r["topic"])); err == nil {
				t.Errorf("case %d (%v): ChChannel(%q,%q,%q) should be refused",
					i, why, str(r["tenant"]), str(r["channel"]), str(r["topic"]))
			}
		}
	}
}

func runProtocolSubjectsCase(t *testing.T, c sharedCase) {
	t.Helper()
	if len(c.Cases) == 0 {
		t.Fatalf("%s: no cases", c.ID)
	}
	for i, mc := range c.Cases {
		fn, _ := mc["fn"].(string)
		args, _ := mc["args"].(map[string]any)
		want, _ := mc["want"].(string)
		got, _ := protocol.EvalSubject(fn, args).(string)
		if got != want {
			t.Errorf("case %d (%s, %v): got %q, want %q", i, fn, mc["why"], got, want)
		}
	}
}

func stringMap(v any) map[string]string {
	m, _ := v.(map[string]any)
	out := make(map[string]string, len(m))
	for k, val := range m {
		out[k] = str(val)
	}
	return out
}

func str(v any) string {
	s, _ := v.(string)
	return s
}
