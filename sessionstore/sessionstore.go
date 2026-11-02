// Package sessionstore is the KV bucket shape every per-session list in this SDK uses.
//
// WHY IT EXISTS. `job` and `denial` are two lists that answer different questions and are stored the
// same way: a bucket, keys ordered `t.<tenant>.s.<session>.<rest>`, a TTL, and a watch whose initial
// snapshot is the current list. Written separately, they were also WRONG separately — the same two
// mistakes appeared twice:
//
//   - a prefix ending in `*` instead of `>`, which matches one subject token and so finds nothing
//     when the rest of the key contains dots
//   - `KVCreate` followed by `KVPut` on failure, where a create reports an existing key as SUCCESS and
//     the put therefore never runs
//
// Both are properties of the storage shape, not of jobs or of denials, so they belong here — where a
// third list will get them right by construction instead of by reading a comment.
package sessionstore

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/easydev-org/easybus-sdk/bus"
	"github.com/easydev-org/easybus-sdk/protocol"
)

// Store reads and writes per-session lists.
type Store struct{ bus bus.Bus }

// New returns a store over the given bus.
func New(b bus.Bus) *Store { return &Store{bus: b} }

/**
 * Key builds `t.<tenant>.s.<session>.<rest...>`.
 *
 * THE SEGMENTS ARE THERE TO BE WATCHED, not merely to be unique. Ordering from least to most specific
 * is what lets one conversation list its own entries with a single wildcard.
 *
 * THE SESSION IS ESCAPED, because a name containing a dot would otherwise add a segment: a session
 * called `a.b` would be written as `t.alice.s.a.b.<id>` and read back as session `a`, id `b.<id>` —
 * the write and the parse disagreeing about where the session ends. `protocol` already had the answer
 * and its own test; this is the same function, not a second opinion about it.
 *
 * THE REST IS NOT ESCAPED, and that is deliberate: those segments are the caller's ids, and a job id
 * like `sandbox-job.392f82df` is expected to be several tokens. It is exactly why the read prefix ends
 * in `>` — see Prefix.
 */
func Key(tenant, session string, rest ...string) string {
	parts := append([]string{"t", protocol.EscapeKVSegment(tenant), "s", protocol.EscapeKVSegment(session)}, rest...)
	return strings.Join(parts, ".")
}

/**
 * Prefix is the wildcard for everything belonging to one conversation.
 *
 * IT ENDS IN `>`, NOT `*`, AND THAT IS THE POINT. NATS subject wildcards are per TOKEN: `*` matches
 * exactly one, `>` matches one or more of the remaining. An entry whose id contains dots —
 * `sandbox-job.392f82df` — is two tokens, so a `*` prefix matched the first and then expected the
 * subject to end. The watch stayed live, the entries were written, and the list came back empty.
 */
func Prefix(tenant, session string) string {
	return Key(tenant, session) + ".>"
}

/**
 * Put writes a value with a TTL, creating or replacing.
 *
 * IT IS A PUT AND NOT "CREATE, THEN PUT ON FAILURE". `KVCreate` reports an existing key as `(0, nil)`
 * — the normal losing side of a claim race — so the fall-through never fires, the first write lands,
 * and every subsequent one is discarded in silence. For a list with a heartbeat that reads as "the
 * entry was written once and never renewed"; for a list without one it reads as "the count stopped
 * going up".
 */
func (s *Store) Put(ctx context.Context, bucket, key string, v any, ttl time.Duration) error {
	body, err := json.Marshal(v)
	if err != nil {
		return fmt.Errorf("sessionstore: marshal %s: %w", key, err)
	}
	return s.bus.KVPut(ctx, bucket, key, string(body), ttl.Milliseconds())
}

// Get reads one value into v. It reports false when the key is absent.
//
// AN ABSENT KEY IS NOT AN ERROR — it is the normal state of a session that has not done the thing
// yet, and a caller forced to distinguish "missing" from "broken" by string-matching an error will
// eventually get it wrong.
func (s *Store) Get(ctx context.Context, bucket, key string, v any) (bool, error) {
	raw, err := s.bus.KVGet(ctx, bucket, key)
	if err != nil {
		return false, err
	}
	if raw == "" {
		return false, nil
	}
	if err := json.Unmarshal([]byte(raw), v); err != nil {
		return false, fmt.Errorf("sessionstore: decode %s: %w", key, err)
	}
	return true, nil
}

// Delete removes one key. Deleting a key that is not there is not an error.
func (s *Store) Delete(ctx context.Context, bucket, key string) error {
	return s.bus.KVDelete(ctx, bucket, key)
}

/**
 * CompareAndSwap replaces a value ONLY IF it is still at the revision the caller read.
 *
 * IT IS THE ONE OPERATION A LIST NEEDS THAT PUT CANNOT DO, and the reason is a race no amount of care at
 * the call site removes: two replicas both list the pending rows, both decide to consume them, and both
 * write. Put lets the second overwrite the first and the message is delivered TWICE — the exact thing a
 * queue must not do.
 *
 * It reports false for a lost race, which is not an error: somebody else claimed it, and that is the
 * correct outcome for the loser.
 */
func (s *Store) CompareAndSwap(ctx context.Context, bucket, key string, v any, revision uint64) (bool, error) {
	body, err := json.Marshal(v)
	if err != nil {
		return false, fmt.Errorf("sessionstore: marshal %s: %w", key, err)
	}
	// The bus reports a lost race as revision 0 with no error; anything else that went wrong is an
	// error. Both are handled here so callers do not have to know which integer means which.
	rev, err := s.bus.KVCas(ctx, bucket, key, string(body), int64(revision))
	if err != nil {
		return false, err
	}
	return rev != 0, nil
}

/**
 * Entry is one row read back from a list.
 *
 * THE VALUE IS LEFT ENCODED. Decoding belongs to the package that owns the type: this one knows the
 * bucket shape and nothing about what a job or a refusal means, and a store that decoded would need a
 * registry of every payload anyone might put in it.
 */
type Entry struct {
	Key      string
	Value    string
	Revision uint64
	Deleted  bool
	// SnapshotDone marks the boundary between "what already exists" and "what changed".
	SnapshotDone bool
}

/**
 * Watch streams a conversation's entries: the current list first, then changes.
 *
 * THE INITIAL SNAPSHOT IS WHAT MAKES A PANEL OPEN INSTANTLY. A watch that delivered only changes would
 * show an empty list until something moved — which reads as "there is nothing" when the truth is
 * "several, all steady".
 *
 * THE BOUNDARY IS PRESERVED rather than swallowed. A caller needs to know when "here is what exists"
 * becomes "here is what changed", because only before that point is an ABSENT key evidence of
 * absence. Without the marker a client either renders a half-filled list or guesses how long to wait.
 */
func (s *Store) Watch(ctx context.Context, bucket, prefix string) (<-chan Entry, func(), error) {
	ch, cancel, err := s.bus.KvWatch(ctx, bucket, prefix)
	if err != nil {
		return nil, nil, err
	}
	out := make(chan Entry)
	go func() {
		defer close(out)
		for ev := range ch {
			e := Entry{
				Key: ev.Key, Value: ev.Value, Revision: ev.Revision,
				Deleted: ev.Deleted, SnapshotDone: ev.Done,
			}
			select {
			case <-ctx.Done():
				return
			case out <- e:
			}
		}
	}()
	return out, cancel, nil
}

/**
 * List returns the snapshot and stops — the entries that exist now, not a subscription.
 *
 * FOR A CALLER THAT ANSWERS ONE QUESTION. A page asking "what is running" once does not want a stream
 * it has to tear down; a page that wants the list to change under it calls Watch.
 *
 * THE SNAPSHOT ENDS THE LOOP, so a busy bucket cannot keep this call from returning.
 */
func (s *Store) List(ctx context.Context, bucket, prefix string) ([]Entry, error) {
	ch, cancel, err := s.Watch(ctx, bucket, prefix)
	if err != nil {
		return nil, err
	}
	defer cancel()
	var out []Entry
	for e := range ch {
		if e.SnapshotDone {
			break
		}
		if e.Deleted || e.Value == "" {
			continue
		}
		out = append(out, e)
	}
	return out, nil
}

