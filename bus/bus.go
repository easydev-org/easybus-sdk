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
	// Durable names the consumer so it survives a restart. Empty derives a name from the
	// subject, which is what per-session mailbox consumers want; a caller consuming a SHARED
	// stream (channels) supplies its own so the name is independent of the subject.
	Durable string
	// Queue splits delivery across replicas. Without it every replica would receive every
	// message, which is right for per-session consumers and wrong for a shared fan-in.
	Queue string
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

/**
 * THE CAPABILITIES, SPLIT SO A CALLER DEPENDS ON THE ONE IT USES.
 *
 * `Bus` below still exists and still works; this is not a breaking change. What it changes is what a
 * package can ASK FOR. Every consumer previously took `bus.Bus` — all nineteen methods — whether it
 * needed one (the subscription index, which reads a KV watch) or five. Two costs came with that:
 *
 *   - a test had to supply a nineteen-method fake to exercise a three-method component, so in
 *     practice such tests were skipped;
 *   - "what does this package actually touch" was not answerable from its signature. `mailbox`
 *     looked like it could publish, consume, store objects and watch keys, and it does none of them.
 *
 * THE SPLIT IS BY TRANSPORT CAPABILITY, NOT BY CALLER. These describe the guarantees the broker
 * makes; a package composes the ones it needs. Splitting by caller instead would produce one
 * interface per consumer and no shared vocabulary.
 */

// Messenger is the live, non-durable side: request/reply and fire-and-forget pub/sub.
//
// NOTHING HERE SURVIVES A RESTART. A publish nobody is listening to is dropped, which is why anything
// that must not be lost uses Durable instead.
type Messenger interface {
	Request(ctx context.Context, ch string, payload any, opts RequestOpts) (*easybus.Envelope, error)
	RequestMany(ctx context.Context, ch string, payload any, opts RequestOpts) ([]*easybus.Envelope, error)
	Publish(ctx context.Context, ch string, payload any, opts PublishOpts) error
	Subscribe(ctx context.Context, ch string, opts SubscribeOpts) (Subscription, error)
}

// Durable is the JetStream side: messages are retained until a consumer acks, so a restart resumes
// rather than losing what arrived while nothing was running.
type Durable interface {
	InboxPublish(ctx context.Context, ch string, payload any, opts InboxPublishOpts) error
	InboxConsume(ctx context.Context, opts InboxConsumeOpts) (InboxSubscription, error)

	// Replay returns the retained envelopes for a channel, oldest first, bounded by the stream's
	// retention. It backs "resume a session's event stream" and reads whatever window the stream
	// still holds rather than promising the whole history.
	Replay(ctx context.Context, ch string) ([]*easybus.Envelope, error)
}

// KeyValue is the KV side: buckets addressed by key, with watch, compare-and-swap and TTL.
type KeyValue interface {
	// KvWatch streams bucket entries matching keys (NATS wildcard). The initial snapshot arrives
	// first, then live updates.
	KvWatch(ctx context.Context, bucket, keys string) (<-chan KvEvent, func(), error)

	KVCreate(ctx context.Context, bucket, key, value string, ttlMs int64) (int64, error)
	KVPut(ctx context.Context, bucket, key, value string, ttlMs int64) error
	KVGet(ctx context.Context, bucket, key string) (string, error)
	KVCas(ctx context.Context, bucket, key, value string, revision int64) (int64, error)
	KVDelete(ctx context.Context, bucket, key string) error
}

// Purger discards a channel's retained messages. Its own interface because it is DESTRUCTIVE with no
// undo, and a component that consumes a queue has no business being able to empty it: the only
// callers are a test that must not inherit a shared broker's history and an operator clearing a
// poisoned queue after triage.
type Purger interface {
	InboxPurge(ctx context.Context, ch string) error
}

/**
 * TWO NAMED COMBINATIONS, for the two shapes that recur.
 *
 * An anonymous `interface{ bus.Messenger; bus.Durable }` in a signature compiles just as well and is
 * unreadable in a doc comment, which is where these get referenced.
 */

// EventBus publishes and consumes events: both the live hop (an event nobody is listening to is
// fine) and the durable log (a lifecycle notice a restarting consumer must still see).
type EventBus interface {
	Messenger
	Durable
}

// Queue is the subset a DURABLE CONSUMER needs: take messages, ack them, read back the window it may
// have missed.
type Queue interface {
	Durable
}

// AgentBus is what the agent role needs: talk to extensions, keep its own state, store bytes.
//
// NOT Purger. An agent consumes queues; emptying one is an operator's act, and a role that can also
// erase its own input is a role where a bug has no undo.
type AgentBus interface {
	Messenger
	Durable
	ObjectStore
	KeyValue
	Close() error
}

// ExtensionBus is what an extension role needs: answer tool calls, publish events, read its config,
// write variables, and hand a message to a session.
//
// IT HAS InboxPublish BUT NOT InboxConsume. The asymmetry is the design: an extension ANNOUNCES (a
// durable delivery the agent's bridge turns into a queue row) and never CONSUMES a session's queue.
// The former is how a background job reports back; the latter would be a second path into the
// mailbox, which is what the wake/mailbox split exists to remove.
//
// NOT Purger either — emptying a queue is an operator's act.
type ExtensionBus interface {
	Messenger
	ObjectStore
	KeyValue
	InboxPublish(ctx context.Context, ch string, payload any, opts InboxPublishOpts) error
	Close() error
}

// ConfigBus is the config store's view: a request to push a value, and the bucket to remember it in.
type ConfigBus interface {
	KeyValue
	Request(ctx context.Context, ch string, payload any, opts RequestOpts) (*easybus.Envelope, error)
}

// FileRPCBus is the file-transfer client's view: a request hop plus the transient object bucket.
//
// THE BYTES GO THROUGH THE OBJECT STORE AND THE REFERENCE THROUGH THE REQUEST, which is why both
// halves are needed and nothing else is: this is not a general-purpose bus dependency.
type FileRPCBus interface {
	Request(ctx context.Context, ch string, payload any, opts RequestOpts) (*easybus.Envelope, error)
	ObjectPut(ctx context.Context, name string, data []byte) error
	ObjectGet(ctx context.Context, name string) ([]byte, error)
}

// Bus is every capability at once, plus Close.
//
// IT IS WHAT A TRANSPORT IMPLEMENTS, not what a consumer should ask for. Take Messenger, Durable,
// ObjectStore, KeyValue or Purger in a signature and the compiler will keep the next change from
// quietly reaching for a method that package never used.
type Bus interface {
	Messenger
	Durable
	ObjectStore
	Purger
	KeyValue

	Close() error
}
