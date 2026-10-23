package bus

import (
	"context"
	"errors"
	"fmt"
	"regexp"

	"github.com/easydev-org/easybus-sdk/protocol"
)

// Channels: publish/subscribe topics, distinct from the mailbox.
//
// A mailbox message is a COMMAND addressed to one session; it drives that session's turn.
// A channel message is an EVENT that any number of sessions may care about — a new tag in
// a repository, a message in a group chat. They share the same transport (JetStream, tenant
// prefix, durable retention) but not the same delivery shape: the mailbox is point-to-point
// and the channel fans out.

// segmentRe accepts one subject segment: letters, digits, and a few separators that
// channels and topics genuinely use (like a repository slug's "/" and ".").
//
// Wildcards are REFUSED. A publisher that could put "*" in a channel name would turn one
// publish into a broadcast to every subscriber, which is not what "publish to a channel"
// means and would be very hard to notice.
var segmentRe = regexp.MustCompile(`^[A-Za-z0-9._/-]+$`)

// ErrBadChannelSubject reports a channel or topic that cannot be a subject segment.
var ErrBadChannelSubject = errors.New("channel and topic must be literal subject segments")

// validateChannelSubject refuses anything that would change the meaning of the subject.
//
// Tenant isolation depends on this: the subject is built from the caller's own tenant plus
// these two values, so a caller that could inject a dot would be choosing its own subject.
// A dot is allowed INSIDE a value (repository names have them) but a leading or trailing one
// is not, and an empty segment is not.
func validateChannelSubject(channel, topic string) error {
	for _, v := range []string{channel, topic} {
		if v == "" {
			return ErrBadChannelSubject
		}
		if !segmentRe.MatchString(v) {
			return fmt.Errorf("%w: %q contains characters outside [A-Za-z0-9._/-]", ErrBadChannelSubject, v)
		}
		if v[0] == '.' || v[len(v)-1] == '.' {
			return fmt.Errorf("%w: %q must not start or end with '.'", ErrBadChannelSubject, v)
		}
	}
	return nil
}

// Channels wraps a Bus with channel publish/subscribe.
//
// A separate type rather than more methods on Bus: Bus is the transport-agnostic interface
// that every implementation must satisfy, and channels are a POLICY on top of it
// (tenant-scoped subjects, fan-out routing). Keeping them apart means an in-memory test bus
// does not have to reimplement channel semantics, and the channel code has exactly one
// implementation.
type Channels struct {
	bus Bus
}

// NewChannels wraps a bus.
func NewChannels(b Bus) *Channels { return &Channels{bus: b} }

// PublishChannel publishes one event to a channel.
//
// The tenant is NOT a parameter: it is the caller's, and it is the only tenant the subject
// will name. Cross-tenant delivery is therefore impossible by construction rather than by a
// check that could be missed.
func (c *Channels) PublishChannel(ctx context.Context, tenant, channel, topic string, payload any, source string) error {
	if err := validateChannelSubject(channel, topic); err != nil {
		return err
	}
	subj := protocol.ChChannel(tenant, channel, topic)
	// Plain Publish, not InboxPublish: a channel event does NOT need a per-message dedup id
	// or a session column. Retention comes from the stream, and the consumer is the agent's
	// single fan-out — see ConsumeChannels.
	// The source travels INSIDE the payload envelope rather than in a header: the envelope
	// is the only metadata channel this transport carries, and a header would need a
	// transport change for one string.
	return c.bus.Publish(ctx, subj, channelPayload{Payload: payload, Source: source}, PublishOpts{
		Tenant: tenant,
	})
}

// channelPayload is the wire shape of one channel message.
//
// The source is carried next to the payload instead of in a header, and it is
// ADVISORY: the authoritative channel and tenant come from the subject. A publisher that
// lies about its source mislabels itself; it cannot change where the message goes.
type channelPayload struct {
	Payload any    `json:"payload,omitempty"`
	Source  string `json:"source,omitempty"`
}

// ChannelMessage is a consumed channel envelope (transport-agnostic).
type ChannelMessage struct {
	Tenant  string
	Channel string
	Topic   string
	Payload any
	Source  string
}

// ConsumeChannels subscribes once to EVERY channel of every tenant and hands each message
// to the handler.
//
// One consumer for all channels, not one per subscription. That is the whole design: a
// per-subscription consumer would have to be rebuilt whenever a user changed what they
// follow (a consumer's filter is fixed at creation), and it would multiply the idle pull
// traffic by the number of subscriptions. Here the filter is always `channel.>`, so user
// subscriptions cost nothing at the broker — the handler routes by consulting the
// subscription index.
//
// The subscription is cross-tenant on purpose, like the mailbox wildcard. Isolation is
// enforced in the handler, which reads the tenant from the subject and only distributes to
// that tenant's subscriptions.
func (c *Channels) ConsumeChannels(ctx context.Context, queue string, handler func(ChannelMessage) error) (func(), error) {
	sub, err := c.bus.Subscribe(ctx, protocol.ChChannelAll, SubscribeOpts{Queue: queue})
	if err != nil {
		return nil, err
	}
	go func() {
		defer sub.Close()
		for {
			env, ok := sub.Next(ctx)
			if !ok {
				return
			}
			// Derive everything from the SUBJECT, never from the payload: a publisher must
			// not be able to claim a different channel or tenant than the one it published
			// on.
			tenant, channel, topic, ok := protocol.ParseChannelSubject(env.Ch)
			if !ok {
				continue
			}
			var body channelPayload
			_ = protocol.Coerce(env.Payload, &body)
			if err := handler(ChannelMessage{
				Tenant: tenant, Channel: channel, Topic: topic,
				Payload: body.Payload,
				Source:  body.Source,
			}); err != nil {
				// A failed distribution must not kill the consumer: one bad event should
				// not silence every channel for every tenant.
				continue
			}
		}
	}()
	return func() { _ = sub.Close() }, nil
}
