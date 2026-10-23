package extension

import (
	"context"
	"fmt"
	"sort"
	"strings"

	easybus "github.com/easydev-org/easybus-sdk"
)

// Permission enforcement, extension side.
//
// The agent stores what a session MAY do and hands those grants over on every call. This
// file is the other half: deciding whether a grant COVERS the call about to happen.
//
// WHY the decision lives here: coverage depends on the call's target — this repository,
// that sandbox, that namespace — and turning the arguments into that target is domain
// knowledge only the extension has ("http://host/o/r.git" -> "o/r" is a git rule, not a
// protocol rule). Keeping it here is what lets the same mechanism serve an extension that
// guards repositories and one that guards sandboxes.
//
// The agent's own check is an optimisation, not a boundary: it can say "this session has
// no merge permission at all" and save a round trip, but it cannot say "this grant covers
// THIS pull request". Only this file can, and only this file is the security boundary —
// the extension is what holds the credential and performs the action.

// PermissionDenied is the error a tool returns when a required permission is absent or
// does not cover the call.
//
// It carries the target that was attempted, so the caller can say WHICH repository was
// refused rather than "permission denied" — and so an audit log records what was tried,
// not just that something was.
type PermissionDenied struct {
	// Key is the permission that was required.
	Key string
	// Target is what the extension resolved the call to touch. Opaque to everyone but
	// this extension, but it is the whole point: without it a refusal is unactionable.
	Target map[string]string
	// Granted lists the params this session DOES hold for Key, so the caller can see the
	// gap ("granted: repo=agentdev/mrflow, wanted: repo=easydev-org/easyagent").
	Granted []map[string]string
}

func (e *PermissionDenied) Error() string {
	var b strings.Builder
	fmt.Fprintf(&b, "permission denied: %s", e.Key)
	if len(e.Target) > 0 {
		fmt.Fprintf(&b, " on %s", formatParams(e.Target))
	}
	if len(e.Granted) == 0 {
		b.WriteString(" (no grant held for this permission)")
		return b.String()
	}
	parts := make([]string, 0, len(e.Granted))
	for _, g := range e.Granted {
		parts = append(parts, formatParams(g))
	}
	fmt.Fprintf(&b, " (held: %s)", strings.Join(parts, "; "))
	return b.String()
}

// formatParams renders params deterministically, so two refusals for the same gap read
// the same and a test can assert on the text.
func formatParams(p map[string]string) string {
	keys := make([]string, 0, len(p))
	for k := range p {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, k := range keys {
		parts = append(parts, k+"="+p[k])
	}
	return strings.Join(parts, ",")
}

// grantsKey carries a call's grants on the context.
type grantsKey struct{}

// WithGrants attaches the caller's grants to a tool invocation.
//
// Called by the agent before dispatching a tool call. A tool that declares no `requires`
// ignores it entirely.
func WithGrants(ctx context.Context, grants []*easybus.Grant) context.Context {
	if len(grants) == 0 {
		return ctx
	}
	return context.WithValue(ctx, grantsKey{}, grants)
}

// GrantsFrom returns the grants attached by the agent.
func GrantsFrom(ctx context.Context) []*easybus.Grant {
	g, _ := ctx.Value(grantsKey{}).([]*easybus.Grant)
	return g
}

// GrantsFor returns the params of every grant held for one key.
//
// A session may hold SEVERAL grants of the same key with different params — "merge in
// agentdev/mrflow" and "merge in scratch/*" are two authorisations, not one. Coverage
// means at least one of them matches.
func GrantsFor(ctx context.Context, key string) []map[string]string {
	var out []map[string]string
	for _, g := range GrantsFrom(ctx) {
		if g == nil || g.GetKey() != key {
			continue
		}
		out = append(out, structToStrings(g.GetParams().AsMap()))
	}
	return out
}

// Allowed reports whether the caller may perform key against target.
//
// A grant covers the call when EVERY param it declares matches the target (AND across
// keys), and at least one grant covers it. Params the target does not mention are simply
// not compared — an extension decides which keys of the target to resolve.
func Allowed(ctx context.Context, key string, target map[string]string) bool {
	for _, granted := range GrantsFor(ctx, key) {
		if Matches(granted, target) {
			return true
		}
	}
	return false
}

// Require returns a PermissionDenied when the call is not covered, and nil when it is.
//
// The expected shape inside a tool:
//
//	target := map[string]string{"repo": slugOf(argStr(args, "remote"))}
//	if err := perm.Require(ctx, "mr.merge", target); err != nil {
//	    return err
//	}
func Require(ctx context.Context, key string, target map[string]string) error {
	if Allowed(ctx, key, target) {
		return nil
	}
	return &PermissionDenied{Key: key, Target: target, Granted: GrantsFor(ctx, key)}
}

// Matches reports whether a grant's params cover a target.
//
// Semantics, deliberately small:
//
//   - every key in the GRANT must appear in the target and match (AND across keys);
//   - a value may use `*` as a wildcard: "agentdev/*" covers "agentdev/mrflow",
//     and "*" covers anything;
//   - otherwise it is an exact string comparison.
//
// WHY not regex or ranges: a permission a reader cannot predict is a permission they
// cannot reason about. `*` is the smallest thing that makes "all my repositories"
// expressible, and a client can explain it in one sentence. An extension that needs more
// can ignore these params and enforce its own rule inside its tool.
func Matches(granted, target map[string]string) bool {
	for k, want := range granted {
		got, ok := target[k]
		if !ok {
			return false
		}
		if !matchValue(want, got) {
			return false
		}
	}
	return true
}

// matchValue compares one param value, honouring `*`.
func matchValue(pattern, value string) bool {
	if pattern == "*" {
		return true
	}
	// A single wildcard covers a prefix/suffix pair: "agentdev/*" and "agentdev/*.go"
	// both behave as the reader expects from a glob.
	if i := strings.IndexByte(pattern, '*'); i >= 0 {
		prefix, suffix := pattern[:i], pattern[i+1:]
		if len(value) < len(prefix)+len(suffix) {
			return false
		}
		return strings.HasPrefix(value, prefix) && strings.HasSuffix(value, suffix)
	}
	return pattern == value
}

// structToStrings flattens a param struct to strings.
//
// Params are declared with the config schema, whose leaves are scalars; anything else
// (a number written as a JSON number, a bool) is stringified so one comparison handles
// every declared type. A nested object would not compare meaningfully, so it is rendered
// with %v — still comparable, and visibly wrong if an extension ever declares one.
func structToStrings(m map[string]any) map[string]string {
	out := make(map[string]string, len(m))
	for k, v := range m {
		switch t := v.(type) {
		case string:
			out[k] = t
		case nil:
			out[k] = ""
		default:
			out[k] = fmt.Sprintf("%v", t)
		}
	}
	return out
}
