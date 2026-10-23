package extension

import (
	"context"
	"strings"
	"testing"

	easybus "github.com/easydev-org/easybus-sdk"
)

// grant builds a Grant the way the agent would (params are opaque to it).
func grant(key string, params map[string]any) *easybus.Grant {
	return &easybus.Grant{Key: key, Params: easybus.Struct(params)}
}

func TestMatchesExact(t *testing.T) {
	g := map[string]string{"repo": "agentdev/mrflow"}
	if !Matches(g, map[string]string{"repo": "agentdev/mrflow"}) {
		t.Error("an exact match must be allowed")
	}
	if Matches(g, map[string]string{"repo": "agentdev/other"}) {
		t.Error("a different repository must not be covered")
	}
}

// The wildcard is the whole reason a user can say "all my repositories"; without it
// every repo would need its own grant.
func TestMatchesGlob(t *testing.T) {
	cases := []struct {
		grant  string
		target string
		want   bool
	}{
		{"agentdev/*", "agentdev/mrflow", true},
		{"agentdev/*", "easydev-org/x", false},
		{"*", "anything/at/all", true},
		{"agentdev/*.go", "agentdev/main.go", true},
		{"agentdev/*.go", "agentdev/main.md", false},
		// A wildcard must not match across the separator it was anchored to.
		{"agentdev/*", "agentdev", false},
	}
	for _, c := range cases {
		got := Matches(map[string]string{"repo": c.grant}, map[string]string{"repo": c.target})
		if got != c.want {
			t.Errorf("grant %q vs target %q = %v, want %v", c.grant, c.target, got, c.want)
		}
	}
}

// Every declared param must match (AND). A grant that names two params is narrower than
// one that names one, which is what a reader expects.
func TestMatchesRequiresAllParams(t *testing.T) {
	g := map[string]string{"namespace": "dev", "service": "api"}
	if !Matches(g, map[string]string{"namespace": "dev", "service": "api"}) {
		t.Error("both params match, must be allowed")
	}
	if Matches(g, map[string]string{"namespace": "dev", "service": "web"}) {
		t.Error("service differs, must not be covered")
	}
	if Matches(g, map[string]string{"namespace": "dev"}) {
		t.Error("the target omits a declared param, must not be covered")
	}
}

// Params the grant does not declare are not compared: the extension decides which parts
// of the target matter.
func TestMatchesIgnoresUndeclaredTargetParams(t *testing.T) {
	g := map[string]string{"repo": "agentdev/*"}
	target := map[string]string{"repo": "agentdev/mrflow", "branch": "feature/x"}
	if !Matches(g, target) {
		t.Error("an extra target param must not block coverage")
	}
}

// A boolean permission (no params) is covered by any grant of its key.
func TestMatchesBooleanPermission(t *testing.T) {
	ctx := WithGrants(context.Background(), []*easybus.Grant{grant("exec", nil)})
	if !Allowed(ctx, "exec", nil) {
		t.Error("a paramless grant must allow a paramless target")
	}
}

func TestAllowedWithMultipleInstances(t *testing.T) {
	// Two authorisations of the same key, different params — the common shape.
	ctx := WithGrants(context.Background(), []*easybus.Grant{
		grant("mr.merge", map[string]any{"repo": "agentdev/mrflow"}),
		grant("mr.merge", map[string]any{"repo": "scratch/*"}),
	})
	if !Allowed(ctx, "mr.merge", map[string]string{"repo": "agentdev/mrflow"}) {
		t.Error("first instance must match")
	}
	if !Allowed(ctx, "mr.merge", map[string]string{"repo": "scratch/anything"}) {
		t.Error("second instance must match")
	}
	if Allowed(ctx, "mr.merge", map[string]string{"repo": "easydev-org/easyagent"}) {
		t.Error("neither instance covers this repository")
	}
}

// Grants for one key must not leak into another: holding mr.merge says nothing about
// repo.delete.
func TestKeysAreIndependent(t *testing.T) {
	ctx := WithGrants(context.Background(), []*easybus.Grant{
		grant("mr.merge", map[string]any{"repo": "*"}),
	})
	if Allowed(ctx, "repo.delete", map[string]string{"repo": "anything"}) {
		t.Error("a merge grant must not authorise a delete")
	}
}

func TestRequireReportsTheGap(t *testing.T) {
	ctx := WithGrants(context.Background(), []*easybus.Grant{
		grant("mr.merge", map[string]any{"repo": "agentdev/mrflow"}),
	})
	err := Require(ctx, "mr.merge", map[string]string{"repo": "easydev-org/easyagent"})
	if err == nil {
		t.Fatal("a non-covering target must be refused")
	}
	denied, ok := err.(*PermissionDenied)
	if !ok {
		t.Fatalf("want *PermissionDenied, got %T", err)
	}
	// The message has to name BOTH what was wanted and what is held, or a caller cannot
	// tell the difference between "no permission" and "wrong repository".
	msg := denied.Error()
	if !strings.Contains(msg, "easydev-org/easyagent") {
		t.Errorf("message must name the attempted target: %q", msg)
	}
	if !strings.Contains(msg, "agentdev/mrflow") {
		t.Errorf("message must name what is held: %q", msg)
	}
}

func TestRequireWithNoGrantSaysSo(t *testing.T) {
	err := Require(context.Background(), "mr.merge", map[string]string{"repo": "x/y"})
	if err == nil {
		t.Fatal("no grant at all must be refused")
	}
	if !strings.Contains(err.Error(), "no grant held") {
		t.Errorf("message must distinguish 'none held' from 'held but not covering': %q", err.Error())
	}
}

func TestRequirePassesWhenCovered(t *testing.T) {
	ctx := WithGrants(context.Background(), []*easybus.Grant{
		grant("mr.merge", map[string]any{"repo": "agentdev/*"}),
	})
	if err := Require(ctx, "mr.merge", map[string]string{"repo": "agentdev/mrflow"}); err != nil {
		t.Fatalf("covered target must pass, got %v", err)
	}
}

// Grants arrive from the agent as protobuf Structs, which is where numbers and booleans
// get their types back. A param declared as a number must still compare.
func TestParamTypesAreStringified(t *testing.T) {
	ctx := WithGrants(context.Background(), []*easybus.Grant{
		grant("scale", map[string]any{"replicas": float64(3)}),
	})
	if !Allowed(ctx, "scale", map[string]string{"replicas": "3"}) {
		t.Error("a numeric param must compare as its text form")
	}
}

// Grants on the context are per-call. An empty context must simply have none, not panic.
func TestNoGrantsIsNotPanic(t *testing.T) {
	if len(GrantsFrom(context.Background())) != 0 {
		t.Error("an empty context must carry no grants")
	}
	if Allowed(context.Background(), "anything", nil) {
		t.Error("with no grants nothing is allowed")
	}
}
