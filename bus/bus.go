package bus

import (
	"context"

	easybus "github.com/easydev-org/easybus-sdk"
)

// RequestOpts tunes a request or broadcast.
type RequestOpts struct {
	// TimeoutMs bounds a 1:1 request; 0 means no timeout.
	TimeoutMs int
	// MaxWaitMs bounds a 1:N collect.
	MaxWaitMs int
	// SessionName rides the envelope's first-class session_name field.
	SessionName string
	// Tenant rides the envelope's first-class tenant field. When empty the
	// transport derives it from the subject's tenant segment.
	Tenant string
}

// SubscribeOpts tunes a subscription.
type SubscribeOpts struct {
	Queue string
}

type InboxConsumeOpts struct {
	Subject string
}

type InboxPublishOpts struct {
	ID string
	// SessionName rides the envelope's session_name so the consumer can
	// route the message back to its logical session.
	SessionName string
	// Tenant rides the envelope's tenant field.
	Tenant string
}

// PublishOpts tunes a fire-and-forget publish.
type PublishOpts struct {
	// ReplyTo is the transport-internal reply address (set by request handling).
	ReplyTo string
	// Tenant rides the envelope's tenant field. When empty the transport
	// derives it from the subject's tenant segment.
	Tenant string
}

// Subscription is a live channel subscription.
type Subscription interface {
	// Next blocks until the next envelope or (ctx done / closed).
	Next(ctx context.Context) (*easybus.Envelope, bool)
	Close() error
}

// KvEvent is one KV watch delivery. The initial snapshot arrives first
// (IsUpdate=false), then live updates.
type KvEvent struct {
	Key      string
	Value    string
	Revision uint64
	Deleted  bool
	IsUpdate bool
	// Done marks the snapshot/initial-state boundary: it is set on exactly
	// one synthetic event (empty value) delivered after the retained
	// snapshot; everything after is live. Best-effort on some clients
	// (may arrive together with the last snapshot entry).
	Done bool
}

// InboxMsg is a durable-inbox message with explicit ack/nak/term.
type InboxMsg struct {
	Envelope *easybus.Envelope
	ack      func()
	nak      func(delayMs int)
	term     func()
	termRaw  func()
}

// SetHandlers wires transport-specific ack/nak/term handlers. The optional
// fourth handler is a discard-without-dead-letter term.
func (m *InboxMsg) SetHandlers(ack func(), nak func(int), term func(), termNoDLQ ...func()) {
	m.ack, m.nak, m.term = ack, nak, term
	if len(termNoDLQ) > 0 {
		m.termRaw = termNoDLQ[0]
	}
}

func (m *InboxMsg) Ack() {
	if m.ack != nil {
		m.ack()
	}
}
func (m *InboxMsg) Nak(delayMs int) {
	if m.nak != nil {
		m.nak(delayMs)
	}
}

// Term terminates delivery and copies the message to the dead-letter
// stream (easybus.dlq.<token>) for inspection.
func (m *InboxMsg) Term() {
	if m.term != nil {
		m.term()
	}
}

// TermNoDLQ terminates delivery without the dead-letter copy.
func (m *InboxMsg) TermNoDLQ() {
	if m.termRaw != nil {
		m.termRaw()
	}
}

// InboxSubscription consumes durable-inbox messages.
type InboxSubscription interface {
	Next(ctx context.Context) (*InboxMsg, bool)
	Close() error
}

// ObjectStore is transport-agnostic object storage (bytes). The NATS bus
// implements it natively; a deployment may inject an S3-compatible store so
// DURABLE file bytes never sit in NATS. One backend per class, no fallback:
// the durable methods below may delegate to an injected store.
type ObjectStore interface {
	ObjectPut(ctx context.Context, name string, data []byte) error
	ObjectGet(ctx context.Context, name string) ([]byte, error)
	ObjectPutPersistent(ctx context.Context, name string, data []byte) error
	ObjectGetPersistent(ctx context.Context, name string) ([]byte, error)
}

// Bus is the transport-agnostic message bus. There is exactly one
// transport (NATS); every listed capability is always available (JetStream).
type Bus interface {
	Request(ctx context.Context, ch string, payload any, opts RequestOpts) (*easybus.Envelope, error)
	RequestMany(ctx context.Context, ch string, payload any, opts RequestOpts) ([]*easybus.Envelope, error)
	Publish(ctx context.Context, ch string, payload any, opts PublishOpts) error
	Subscribe(ctx context.Context, ch string, opts SubscribeOpts) (Subscription, error)

	InboxPublish(ctx context.Context, ch string, payload any, opts InboxPublishOpts) error
	InboxConsume(ctx context.Context, opts InboxConsumeOpts) (InboxSubscription, error)

	// Replay returns the retained (durably queued) envelopes for a channel,
	// oldest first. Retention is a transport property (NATS: stream
	// max_age; inproc/ws: in-memory log) — the protocol promise is only
	// that a bounded recent window is replayable for events channels.
	Replay(ctx context.Context, ch string) ([]*easybus.Envelope, error)

	ObjectPut(ctx context.Context, name string, data []byte) error
	ObjectGet(ctx context.Context, name string) ([]byte, error)

	// ObjectPutPersistent stores bytes in the durable (no-TTL) object bucket.
	// Tool payloads use ObjectPut (transient); durable file bytes use this.
	ObjectPutPersistent(ctx context.Context, name string, data []byte) error
	ObjectGetPersistent(ctx context.Context, name string) ([]byte, error)
	// KvWatch streams bucket entries matching keys (NATS wildcard). The
	// initial snapshot arrives first, then live updates.
	KvWatch(ctx context.Context, bucket, keys string) (<-chan KvEvent, func(), error)

	KVCreate(ctx context.Context, bucket, key, value string, ttlMs int64) (int64, error)
	KVPut(ctx context.Context, bucket, key, value string, ttlMs int64) error
	KVGet(ctx context.Context, bucket, key string) (string, error)
	KVCas(ctx context.Context, bucket, key, value string, revision int64) (int64, error)
	KVDelete(ctx context.Context, bucket, key string) error

	Close() error
}
