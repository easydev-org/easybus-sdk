// Package mailbox is the durable per-session delivery queue, held in a KV bucket.
//
// IT REPLACES A POSTGRES TABLE, and the shape of the replacement is what makes it simpler. The table
// needed two phases to deliver a message — mark then fetch, in one transaction — because a crash between
// them would either lose a message or deliver it twice. Here, a message CHANGES STATE rather than moving
// between tables: draining reads the pending rows and rewrites them as consumed, and a crash in the
// middle leaves some rows pending, which the next drain picks up.
//
// DELIVERY MAY THEREFORE HAPPEN TWICE, and that is safe on purpose: the runtime folds a message under an
// id the producer chose, and folding the same id twice is a no-op. Idempotence at the fold is worth more
// than exactly-once in the queue, because the queue cannot promise it across a crash and the fold can.
package mailbox

import (
	"context"
	"encoding/json"
	"sort"
	"strings"
	"time"

	"github.com/easydev-org/easybus-sdk/bus"
	"github.com/easydev-org/easybus-sdk/protocol"
	"github.com/easydev-org/easybus-sdk/sessionstore"
)

// DefaultBucket is where undelivered and recently-consumed messages live.
const DefaultBucket = "mailbox"

// ONE LIFETIME FOR BOTH STATES, BECAUSE NATS KV HAS ONE.
//
// ITS TTL IS A BUCKET SETTING — applied once at creation, and reset to that same value by Update — so a
// pending message and a consumed one cannot expire on different clocks. Writing 7 days for the queue and
// 24 hours for the receipt looked right and was not expressible.
//
// THE ROLE SPLIT IS WHAT MAKES ONE NUMBER ENOUGH. `Status` says whether a message has been taken; the
// TTL only says when the store forgets it. A day is long enough for somebody to check that what they
// sent was picked up, and long enough that a conversation nobody is running still has its messages when
// it wakes — which is the case this window actually has to cover, since a trigger left unread does not
// need a week if its session is dormant for a week either.
//
// A MESSAGE THAT MUST OUTLIVE THE BUCKET NEEDS A DIFFERENT STORE, not a longer TTL: once the key is gone,
// nothing distinguishes "nobody looked" from "nothing was sent".
const TTL = 24 * time.Hour

// TTLMs is TTL in the unit the bus takes, exported so callers stop re-deriving it.
func TTLMs() int64 { return TTL.Milliseconds() }

// Message is one mailbox entry.
type Message struct {
	ID      string `json:"id"`
	Type    string `json:"type"`
	Source  string `json:"source"`
	Payload any    `json:"payload"`
	// Status is "pending" or "consumed".
	Status string `json:"status"`
	// CreatedAt and ConsumedAt are Unix milliseconds; ConsumedAt is 0 while pending.
	CreatedAt  int64 `json:"createdAt"`
	ConsumedAt int64 `json:"consumedAt,omitempty"`
}

// IsPending reports whether the message is still waiting to be taken.
func (m Message) IsPending() bool { return m.Status != "consumed" }

// Queue reads and writes one deployment's mailbox.
type Queue struct{ kv *sessionstore.Store }

// New returns a queue over the given bus.
func New(b bus.Bus) *Queue { return &Queue{kv: sessionstore.New(b)} }

// Enqueue adds a message.
//
// THE ID COMES FROM THE PRODUCER when it has one, so a redelivery by a publisher that retried lands on
// the same key and replaces itself rather than arriving twice. An empty id is minted here.
func (q *Queue) Enqueue(ctx context.Context, tenant, session, id, typ string, payload any, source string) (string, error) {
	if id == "" {
		id = newID()
	}
	m := Message{
		ID: id, Type: typ, Source: source, Payload: payload,
		Status: "pending", CreatedAt: time.Now().UnixMilli(),
	}
	return id, q.kv.Put(ctx, DefaultBucket, sessionstore.Key(tenant, session, id), m, TTL)
}

// Drain takes every pending message for one session, oldest first, CLAIMING each by compare-and-swap.
//
// THE CLAIM IS WHAT MAKES DELIVERY EXACTLY-ONCE, and it cannot be a plain write. Two replicas can both
// list the same pending rows and both decide to consume them; with a put, the second overwrites the
// first and the message is delivered twice. The CAS makes the list a race with one winner: a lost swap
// means somebody else already took it, so this caller skips it rather than delivering it again.
//
// A MESSAGE IS MARKED, NOT DELETED — it stays readable for TTL so a UI can answer "did my
// message get picked up". Deleting on read would make the queue a black hole.
//
// A CRASH MID-DRAIN LEAVES THE REST PENDING, which the next drain takes. Nothing is lost, and nothing
// that was claimed is claimed twice.
func (q *Queue) Drain(ctx context.Context, tenant, session string) ([]Message, error) {
	rows, err := q.kv.List(ctx, DefaultBucket, sessionstore.Prefix(tenant, session))
	if err != nil {
		return nil, err
	}
	type pendingRow struct {
		msg Message
		rev uint64
	}
	var pending []pendingRow
	for _, row := range rows {
		var m Message
		if unmarshalMessage(row.Value, &m) != nil || !m.IsPending() {
			continue
		}
		pending = append(pending, pendingRow{msg: m, rev: row.Revision})
	}
	// OLDEST FIRST before claiming, so the messages a caller receives are in the order they arrived even
	// though the swap loop below may skip some.
	sort.Slice(pending, func(i, j int) bool { return pending[i].msg.CreatedAt < pending[j].msg.CreatedAt })

	now := time.Now().UnixMilli()
	out := make([]Message, 0, len(pending))
	for _, p := range pending {
		claimed := p.msg
		claimed.Status = "consumed"
		claimed.ConsumedAt = now
		won, cerr := q.kv.CompareAndSwap(ctx, DefaultBucket, sessionstore.Key(tenant, session, p.msg.ID), claimed, p.rev)
		if cerr != nil {
			// A FAILED CLAIM IS NOT A FAILED DRAIN: the message stays pending and the next drain tries
			// again. Abandoning the batch would strand the rest behind one unreachable key.
			continue
		}
		if !won {
			// SOMEBODY ELSE CLAIMED IT — the correct outcome for the loser, and the reason the message is
			// not delivered twice.
			continue
		}
		out = append(out, claimed)
	}
	return out, nil
}

// Pending returns the undelivered messages, oldest first.
//
// OLDEST FIRST because a queue is a queue: a message that arrived earlier asked for something earlier,
// and taking them out of order would let a later instruction overtake the one it was answering.
func (q *Queue) Pending(ctx context.Context, tenant, session string) ([]Message, error) {
	msgs, err := q.list(ctx, tenant, session)
	if err != nil {
		return nil, err
	}
	out := make([]Message, 0, len(msgs))
	for _, m := range msgs {
		if m.IsPending() {
			out = append(out, m)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].CreatedAt < out[j].CreatedAt })
	return out, nil
}

// Page returns one conversation's messages, newest first.
//
// IT IS A SNAPSHOT, NOT A STREAM. A mailbox UI reads a page and offers a refresh; a caller that wants
// changes under it should watch the bucket.
func (q *Queue) Page(ctx context.Context, tenant, session string) ([]Message, error) {
	msgs, err := q.list(ctx, tenant, session)
	if err != nil {
		return nil, err
	}
	sort.Slice(msgs, func(i, j int) bool { return msgs[i].CreatedAt > msgs[j].CreatedAt })
	return msgs, nil
}

// SessionsWithPending lists the conversations that have work waiting.
//
// THIS IS THE ONE OPERATION THE OLD TABLE DID WITH A GROUP BY, and it is the reason the boot backstop
// exists: after a restart, somebody has to find the sessions whose messages arrived while nothing was
// running. It scans the whole bucket, which is affordable at this size and would not be at a large one —
// the cost of no longer having an index.
//
// TYPES NARROW IT to the messages that actually wake a turn, so a bucket full of consumed rows does not
// produce a boot-time list of every session that ever did anything.
func (q *Queue) SessionsWithPending(ctx context.Context, types []string) ([]struct{ Tenant, SessionName string }, error) {
	rows, err := q.kv.List(ctx, DefaultBucket, ">")
	if err != nil {
		return nil, err
	}
	want := map[string]bool{}
	for _, t := range types {
		want[t] = true
	}
	seen := map[string]bool{}
	var out []struct{ Tenant, SessionName string }
	for _, row := range rows {
		var m Message
		if unmarshalMessage(row.Value, &m) != nil || !m.IsPending() {
			continue
		}
		if len(want) > 0 && !want[m.Type] {
			continue
		}
		tenant, session, ok := parseKey(row.Key)
		if !ok {
			continue
		}
		dedupe := tenant + "\x00" + session
		if seen[dedupe] {
			continue
		}
		seen[dedupe] = true
		out = append(out, struct{ Tenant, SessionName string }{tenant, session})
	}
	return out, nil
}

// HasInterrupt reports whether an interrupt is waiting — the one message type that has to be noticed
// before a turn starts rather than within it.
func (q *Queue) HasInterrupt(ctx context.Context, tenant, session string) (bool, error) {
	msgs, err := q.Pending(ctx, tenant, session)
	if err != nil {
		return false, err
	}
	for _, m := range msgs {
		if m.Type == "interrupt" {
			return true, nil
		}
	}
	return false, nil
}

// list reads the session's rows and decodes them, skipping any this version cannot read.
func (q *Queue) list(ctx context.Context, tenant, session string) ([]Message, error) {
	rows, err := q.kv.List(ctx, DefaultBucket, sessionstore.Prefix(tenant, session))
	if err != nil {
		return nil, err
	}
	out := make([]Message, 0, len(rows))
	for _, row := range rows {
		var m Message
		// ONE UNREADABLE ROW MUST NOT EMPTY THE MAILBOX: a neighbour release may have written it, and a
		// person waiting for their own message would be shown nothing instead of everything else.
		if unmarshalMessage(row.Value, &m) == nil {
			out = append(out, m)
		}
	}
	return out, nil
}

// newID mints a message id when the producer did not supply one.
func newID() string { return protocol.NewID() }

// unmarshalMessage is the ONE decoder for a mailbox message, so a producer and a reader in different
// releases cannot drift on field names.
func unmarshalMessage(raw string, m *Message) error { return json.Unmarshal([]byte(raw), m) }

// parseKey recovers the tenant and session from `t.<tenant>.s.<session>.<id>`.
//
// BOTH ARE UNESCAPED, because sessionstore escaped them on the way in: a session called `a.b` is stored
// as one token and must come back as the name it was, or the boot backstop would try to wake a session
// nobody has.
func parseKey(key string) (tenant, session string, ok bool) {
	parts := strings.Split(key, ".")
	if len(parts) < 5 || parts[0] != "t" || parts[2] != "s" {
		return "", "", false
	}
	return protocol.UnescapeKVSegment(parts[1]), protocol.UnescapeKVSegment(parts[3]), true
}
