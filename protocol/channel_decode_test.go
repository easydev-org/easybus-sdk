package protocol

import "testing"

// Everything the channel consumer does to an arriving message, checked against the exact strings
// the publisher produces.
//
// Written because a published channel event was acked without ever being distributed, and the
// consumer's failures are silent: decode() returns an error, Next() reports it as "nothing to
// read", and the loop continues. Each step is asserted separately so a break is attributable.
func TestChannelSubjectSurvivesEveryConsumerStep(t *testing.T) {
	const (
		tenant  = "alice"
		channel = "gitea"
		topic   = "tag.agentdev/mrflow.v1.2.3"
	)

	subj, err := ChChannel(tenant, channel, topic)
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	t.Logf("published subject: %s", subj)

	// Step 1: the consumer's tenant check in decode(). If SubjectTenant disagrees with the
	// envelope's tenant field, decode REJECTS the message and the consumer sees nothing.
	if got := SubjectTenant(subj); got != tenant {
		t.Fatalf("SubjectTenant(%q) = %q, want %q — decode() would reject this message", subj, got, tenant)
	}

	// Step 2: ParseChannelSubject, which the consumer uses to derive tenant/channel/topic.
	gotTenant, gotChannel, gotTopic, ok := ParseChannelSubject(subj)
	if !ok {
		t.Fatalf("ParseChannelSubject(%q) failed — the consumer would ack and drop the message", subj)
	}
	if gotTenant != tenant || gotChannel != channel || gotTopic != topic {
		t.Fatalf("parsed (%q,%q,%q), want (%q,%q,%q)", gotTenant, gotChannel, gotTopic, tenant, channel, topic)
	}

	// Step 3: the subscription index matches on (tenant, channel, topic) against the pattern a
	// session stored. If this fails the message reaches the distributor and reaches nobody.
	if !MatchSubject("tag.agentdev/mrflow.>", gotTopic) {
		t.Fatalf("a session following tag.agentdev/mrflow.> would NOT match topic %q", gotTopic)
	}

	// Step 4: the wildcard the consumer subscribes to must actually cover the subject.
	if !MatchSubject(ChChannelAll, subj) {
		t.Fatalf("the consumer's own wildcard %q does not match %q", ChChannelAll, subj)
	}
}
