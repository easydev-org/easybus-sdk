package protocol

import (
	"fmt"
	"strings"
)

// Channel subject layout.
//
// A channel is a PUBLISH/SUBSCRIBE topic, distinct from the mailbox, which is a point-to-
// point command queue for one session. Keeping them separate matters: a mailbox message
// drives one session, while a channel message may interest none or many.
//
//	easybus.<tenant>.channel.<channel>.<topic...>
//
// The tenant segment is not optional and not caller-supplied: publishers name a channel,
// never a tenant. That is what makes cross-tenant delivery structurally impossible rather
// than merely forbidden by a check.

// ChannelKVBucket holds session subscriptions.
//
// A separate bucket from config on purpose: subscriptions change far more often than
// configuration, and keeping them apart means a config edit never invalidates a
// subscription watch (or vice versa).
const ChannelKVBucket = "subs"

// ChChannel builds the subject for one channel message.
//
// Channel and topic are VALIDATED, not escaped. A caller that smuggles a wildcard into either
// would turn one publish into a broadcast — the extension meant to notify about one repository
// would notify every subscriber. The other three SDKs have always rejected this; Go accepted it,
// which is the divergence this signature fixes.
func ChChannel(tenant, channel, topic string) (string, error) {
	for _, v := range []string{channel, topic} {
		if !isLiteralSegment(v) {
			return "", fmt.Errorf("channel and topic must be literal subject segments: %q", v)
		}
	}
	return TenantPrefix(tenant) + "channel." + channel + "." + topic, nil
}

// isLiteralSegment reports whether v can be a literal (non-wildcard) subject segment.
func isLiteralSegment(v string) bool {
	if v == "" || strings.HasPrefix(v, ".") || strings.HasSuffix(v, ".") {
		return false
	}
	for _, r := range v {
		switch {
		case r >= 'a' && r <= 'z',
			r >= 'A' && r <= 'Z',
			r >= '0' && r <= '9',
			r == '.' || r == '_' || r == '-' || r == '/':
		default:
			return false
		}
	}
	return true
}

// ChChannelAll matches every channel message in every tenant.
//
// Used by the agent's single fan-out consumer. Cross-tenant by construction, like the
// mailbox wildcard: the isolation guarantee lives in the distribution step (which looks up
// the tenant from the envelope), not in the subscription.
const ChChannelAll = "easybus.*.channel.>"

// ChChannelTenant matches every channel message for one tenant.
func ChChannelTenant(tenant string) string {
	return TenantPrefix(tenant) + "channel.>"
}

// ParseChannelSubject splits a channel subject back into its parts.
//
// The consumer derives the tenant, channel and topic from the SUBJECT rather than from the
// payload: a publisher that lied in its payload must not be able to deliver into another
// tenant's channel. The subject is the one thing a publisher cannot forge, because the
// broker routes on it.
//
//	"easybus.alice.channel.gitea.tag.v1.2" -> ("alice", "gitea", "tag.v1.2", true)
func ParseChannelSubject(subject string) (tenant, channel, topic string, ok bool) {
	const prefix = "easybus."
	if !strings.HasPrefix(subject, prefix) {
		return "", "", "", false
	}
	rest := subject[len(prefix):]
	// rest = <tenant>.channel.<channel>.<topic...>
	i := strings.IndexByte(rest, '.')
	if i <= 0 {
		return "", "", "", false
	}
	tenant = rest[:i]
	rest = rest[i+1:]
	if !strings.HasPrefix(rest, "channel.") {
		return "", "", "", false
	}
	rest = rest[len("channel."):]
	// rest = <channel>.<topic...>
	j := strings.IndexByte(rest, '.')
	if j <= 0 || j == len(rest)-1 {
		return "", "", "", false
	}
	return tenant, rest[:j], rest[j+1:], true
}

// SubscriptionKVKey builds the KV key for one subscription.
//
// Layout: t.<tenant>.s.<session>.<subscription-id>
//
// The ID is the last segment rather than the pattern, because a pattern contains dots,
// wildcards and often slashes — it cannot be a key segment without escaping, and two
// subscriptions to the same channel would then collide.
func SubscriptionKVKey(tenant, session, id string) string {
	return "t." + tenant + ".s." + EscapeKVSegment(session) + "." + id
}

// SubscriptionKVPattern matches every subscription of every tenant.
const SubscriptionKVPattern = "t.*.s.>"

// MatchSubject reports whether a topic matches a subscription pattern, with NATS subject
// semantics:
//
//	"*" matches exactly one segment
//	">" matches the rest, and must be last
//	anything else is a literal
//
// Implemented here because the agent must do this matching itself: one consumer receives
// every channel message, and routing it to sessions means comparing against their
// patterns locally. Using NATS' own rules keeps a subscription's meaning identical to what
// a user would write in a subject filter.
func MatchSubject(pattern, topic string) bool {
	if pattern == "" {
		return false
	}
	p := strings.Split(pattern, ".")
	t := strings.Split(topic, ".")
	for i, seg := range p {
		if seg == ">" {
			// ">" swallows the remainder, but must itself cover at least one segment.
			// Verified against a real NATS server rather than assumed: subscribing to
			// "probe.tag.>" delivers probe.tag.v1 but NOT probe.tag. Getting this wrong
			// would deliver a parent topic that no user asked for.
			return len(t) > i
		}
		if i >= len(t) {
			return false
		}
		if seg != "*" && seg != t[i] {
			return false
		}
	}
	// No ">" was seen, so the topic must not be longer than the pattern either.
	return len(p) == len(t)
}
