package protocol

import "testing"

// The consumer derives the tenant from the SUBJECT, not the payload, because the subject is
// the one thing a publisher cannot forge — the broker routes on it. So this parse has to be
// exact: a lenient parser that accepts a malformed subject would distribute a message into
// the wrong tenant.
func TestParseChannelSubject(t *testing.T) {
	ok := []struct {
		subject string
		tenant  string
		channel string
		topic   string
	}{
		{"easybus.alice.channel.gitea.tag.v1", "alice", "gitea", "tag.v1"},
		{"easybus.alice.channel.single-chat.perm-parent.hi", "alice", "single-chat", "perm-parent.hi"},
		{"easybus.bob.channel.group-chat.eng.general", "bob", "group-chat", "eng.general"},
		// A topic containing dots and slashes survives intact: only the first dot after the
		// channel separates them.
		{"easybus.a.channel.gitea.tag.agentdev/mrflow.v1.2.3", "a", "gitea", "tag.agentdev/mrflow.v1.2.3"},
	}
	for _, c := range ok {
		tenant, channel, topic, got := ParseChannelSubject(c.subject)
		if !got || tenant != c.tenant || channel != c.channel || topic != c.topic {
			t.Errorf("ParseChannelSubject(%q) = (%q,%q,%q,%v), want (%q,%q,%q,true)",
				c.subject, tenant, channel, topic, got, c.tenant, c.channel, c.topic)
		}
	}

	bad := []string{
		"",
		"not-easybus.alice.channel.gitea.tag",
		"easybus.channel.gitea.tag",    // no tenant
		"easybus.alice.mailbox.abc",    // not a channel
		"easybus.alice.channel.gitea",  // no topic
		"easybus.alice.channel.gitea.", // empty topic
		"easybus.alice.channel..tag",   // empty channel
	}
	for _, s := range bad {
		if _, _, _, ok := ParseChannelSubject(s); ok {
			t.Errorf("ParseChannelSubject(%q) should have failed", s)
		}
	}
}

// Round trip: what ChChannel builds, ParseChannelSubject must read back.
func TestChannelSubjectRoundTrip(t *testing.T) {
	subj, err := ChChannel("alice", "gitea", "tag.agentdev/mrflow.v1.2.3")
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	tenant, channel, topic, ok := ParseChannelSubject(subj)
	if !ok || tenant != "alice" || channel != "gitea" || topic != "tag.agentdev/mrflow.v1.2.3" {
		t.Fatalf("round trip failed: subject=%q got=(%q,%q,%q,%v)", subj, tenant, channel, topic, ok)
	}
}
