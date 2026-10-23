package protocol

import "testing"

// MatchSubject decides which sessions receive a channel message, so a mistake here is a
// delivery bug: too loose and a session hears things it never subscribed to, too tight and
// an event silently reaches nobody.
//
// The cases mirror NATS subject semantics exactly, because a user writes their subscription
// as a subject filter and must get what they expect.
func TestMatchSubject(t *testing.T) {
	cases := []struct {
		pattern string
		topic   string
		want    bool
	}{
		// Exact.
		{"tag.v1", "tag.v1", true},
		{"tag.v1", "tag.v2", false},

		// "*" is exactly one segment.
		{"tag.*", "tag.v1", true},
		{"tag.*", "tag.v1.2", false},
		{"tag.*.urgent", "tag.v1.urgent", true},
		{"tag.*.urgent", "tag.v1.2.urgent", false},
		{"*", "anything", true},
		{"*", "two.segments", false},

		// ">" swallows the remainder, including nothing.
		{"tag.>", "tag.v1", true},
		{"tag.>", "tag.v1.2.3", true},
		{"tag.>", "tag", false}, // ">" needs at least the boundary before it
		{">", "anything.at.all", true},
		{"agentdev/mrflow.>", "agentdev/mrflow.v1.0", true},

		// A trailing ">" must not let a shorter topic through a different prefix.
		{"gitea.tag.>", "gitea.push.v1", false},

		// Empty pattern matches nothing: a subscription without a pattern is meaningless,
		// and defaulting it to "everything" would leak.
		{"", "anything", false},

		// Topic shorter than the pattern.
		{"a.b.c", "a.b", false},
		// Topic longer than the pattern, with no ">" to absorb it.
		{"a.b", "a.b.c", false},
	}
	for _, c := range cases {
		if got := MatchSubject(c.pattern, c.topic); got != c.want {
			t.Errorf("MatchSubject(%q, %q) = %v, want %v", c.pattern, c.topic, got, c.want)
		}
	}
}

// The channel subject carries the tenant, and the caller never supplies it: that is what
// makes cross-tenant delivery structurally impossible.
func TestChChannelIsTenantScoped(t *testing.T) {
	got := ChChannel("alice", "gitea", "tag.v1")
	if got != "easybus.alice.channel.gitea.tag.v1" {
		t.Fatalf("ChChannel = %q", got)
	}
	// Two tenants produce different subjects for the same channel and topic.
	other := ChChannel("bob", "gitea", "tag.v1")
	if got == other {
		t.Fatal("two tenants must not share a channel subject")
	}
}

// The wildcard the agent consumes is deliberately cross-tenant; isolation happens at the
// distribution step, which knows the tenant from the envelope.
func TestChChannelAllSpansTenants(t *testing.T) {
	if ChChannelAll != "easybus.*.channel.>" {
		t.Fatalf("ChChannelAll = %q", ChChannelAll)
	}
	if !MatchSubject("*.channel.>", "alice.channel.gitea.tag.v1") {
		t.Error("the all-tenants wildcard must match a tenant's channel subject")
	}
}

func TestSubscriptionKVKey(t *testing.T) {
	got := SubscriptionKVKey("alice", "my-session", "sub-1")
	if got != "t.alice.s.my-session.sub-1" {
		t.Fatalf("SubscriptionKVKey = %q", got)
	}
	// A session name containing a dot must be escaped, or the key would gain a segment and
	// the parse on the read side would disagree with the write.
	escaped := SubscriptionKVKey("alice", "a.b", "sub-1")
	if escaped == "t.alice.s.a.b.sub-1" {
		t.Fatal("a dotted session name must be escaped, not left to split the key")
	}
}

func TestSubscriptionKVPatternCoversEveryTenant(t *testing.T) {
	key := SubscriptionKVKey("alice", "s1", "sub-1")
	if !MatchSubject("*.s.>", key[len("t."):]) {
		t.Errorf("subscription key %q not matched by the watch pattern", key)
	}
}
