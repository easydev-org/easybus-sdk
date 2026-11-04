package mailbox

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"
)

// Memory is a Queue that keeps everything in this process.
//
// IT EXISTS SO "NO QUEUE" HAS A SHAPE. A runtime handed no queue still has to answer a wake, and the
// honest answer is an empty one — the wake finds nothing and ends, which is what an idle conversation
// does. Installing this makes that a queue rather than a nil check at every call site, and a nil check
// somebody forgets is a panic in the middle of a turn.
//
// NOT FOR PRODUCTION: messages do not survive the process and a second replica cannot see them. A
// deployment that wires this has a queue that lies about durability, so it is for tests and in-memory
// mode rather than a fallback.
type Memory struct {
	mu   sync.Mutex
	msgs map[string]Message
	// meta keeps the tenant and session beside each message, so listing sessions never has to parse a
	// key — the one place this implementation could quietly disagree with the durable one.
	meta map[string]struct{ Tenant, Session string }
	seq  int64
	// watchers is keyed by tenant\x00session, so a watch reports only its own conversation's
	// changes. A fan-out list would make every turn pay for every other turn's traffic.
	watchers map[string][]chan Message
}

// NewMemory returns an in-process queue.
func NewMemory() *Memory {
	return &Memory{
		msgs:     map[string]Message{},
		meta:     map[string]struct{ Tenant, Session string }{},
		watchers: map[string][]chan Message{},
	}
}

// Enqueue adds a message; an existing id is replaced, matching the durable queue.
func (m *Memory) Enqueue(_ context.Context, tenant, session, id string, typ Type, payload any, source string) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if id == "" {
		m.seq++
		id = fmt.Sprintf("mem-%d", m.seq)
	}
	key := tenant + "\x00" + session + "\x00" + id
	m.msgs[key] = Message{
		ID: id, Type: typ, Source: source, Payload: payload,
		Status: Pending, CreatedAt: time.Now().UnixMilli(),
	}
	m.meta[key] = struct{ Tenant, Session string }{tenant, session}
	// Notify watchers with the value as written. A NON-BLOCKING send: the durable queue's watch can
	// drop an update for a slow reader, and a queue whose producer blocks on its own observers is a
	// queue that can deadlock against the turn draining it.
	for _, ch := range m.watchers[tenant+"\x00"+session] {
		select {
		case ch <- m.msgs[key]:
		default:
		}
	}
	return id, nil
}

// Drain marks the session's pending messages consumed and returns them, oldest first.
func (m *Memory) Drain(_ context.Context, tenant, session string) ([]Message, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	now := time.Now().UnixMilli()
	var out []Message
	for key, meta := range m.meta {
		if meta.Tenant != tenant || meta.Session != session {
			continue
		}
		msg := m.msgs[key]
		if !msg.IsPending() {
			continue
		}
		msg.Status = Consumed
		msg.ConsumedAt = now
		m.msgs[key] = msg
		out = append(out, msg)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].CreatedAt < out[j].CreatedAt })
	return out, nil
}

// Pending returns the session's undelivered messages, oldest first.
func (m *Memory) Pending(_ context.Context, tenant, session string) ([]Message, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []Message
	for key, meta := range m.meta {
		if meta.Tenant == tenant && meta.Session == session && m.msgs[key].IsPending() {
			out = append(out, m.msgs[key])
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].CreatedAt < out[j].CreatedAt })
	return out, nil
}

// Page returns the session's messages, newest first.
func (m *Memory) Page(_ context.Context, tenant, session string) ([]Message, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []Message
	for key, meta := range m.meta {
		if meta.Tenant == tenant && meta.Session == session {
			out = append(out, m.msgs[key])
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].CreatedAt > out[j].CreatedAt })
	return out, nil
}

// SessionsWithPending lists conversations with work waiting, narrowed to the given types.
func (m *Memory) SessionsWithPending(_ context.Context, types []Type) ([]struct{ Tenant, SessionName string }, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	want := map[Type]bool{}
	for _, t := range types {
		want[t] = true
	}
	seen := map[string]bool{}
	var out []struct{ Tenant, SessionName string }
	for key, meta := range m.meta {
		msg := m.msgs[key]
		if !msg.IsPending() || (len(want) > 0 && !want[msg.Type]) {
			continue
		}
		dedupe := meta.Tenant + "\x00" + meta.Session
		if seen[dedupe] {
			continue
		}
		seen[dedupe] = true
		out = append(out, struct{ Tenant, SessionName string }{meta.Tenant, meta.Session})
	}
	return out, nil
}

// HasInterrupt reports whether an interrupt is waiting.
func (m *Memory) HasInterrupt(ctx context.Context, tenant, session string) (bool, error) {
	pending, err := m.Pending(ctx, tenant, session)
	if err != nil {
		return false, err
	}
	for _, msg := range pending {
		if msg.Type == "interrupt" {
			return true, nil
		}
	}
	return false, nil
}

// Watch streams this conversation's messages as they are enqueued.
//
// THE SNAPSHOT THEN THE STREAM, matching the durable queue: a caller that starts a watch gets what
// already exists before it hears about changes. There is no explicit boundary marker because this
// implementation can hand over the whole snapshot in one pass — but a caller that cannot tell the
// two apart still works, because it only ever asks "is there an interrupt waiting".
//
// CONSUMED ROWS ARE NOT RE-SENT. Enqueue is the only event, and a drain does not wake a watcher: the
// point of the watch is to catch a cancel arriving WHILE a turn runs.
func (m *Memory) Watch(_ context.Context, tenant, session string) (<-chan Message, func(), error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	ch := make(chan Message, 16)
	key := tenant + "\x00" + session
	// Snapshot first, so a caller sees what was queued before it started watching.
	for k, meta := range m.meta {
		if meta.Tenant != tenant || meta.Session != session {
			continue
		}
		select {
		case ch <- m.msgs[k]:
		default:
		}
	}
	m.watchers[key] = append(m.watchers[key], ch)
	stop := func() {
		m.mu.Lock()
		defer m.mu.Unlock()
		kept := m.watchers[key][:0]
		for _, c := range m.watchers[key] {
			if c != ch {
				kept = append(kept, c)
			}
		}
		if len(kept) == 0 {
			delete(m.watchers, key)
		} else {
			m.watchers[key] = kept
		}
		close(ch)
	}
	return ch, stop, nil
}
