// Package denial remembers which permissions a session was refused, so a reader can grant them.
//
// THE PROBLEM IT SOLVES IS ONE OF DISCOVERY. A tool is refused because the session holds no grant for
// a capability it needs. The refusal is correct and the model reads it, but the PERSON who could fix
// it is not looking at that turn — and by the time they are, the refusal is buried in a transcript,
// several tool calls back. Without a list, granting a permission means already knowing which one.
//
// A RECORD IS NOT A SECURITY DECISION. It grants nothing and blocks nothing; it is a note that the
// gate said no, kept where somebody can find it. The gate remains the extension and the store.
package denial

import (
	"context"
	"encoding/json"
	"time"

	"github.com/easydev-org/easybus-sdk/bus"
	"github.com/easydev-org/easybus-sdk/sessionstore"
)

// DefaultBucket is where denials accumulate.
//
// SEPARATE FROM THE JOB BUCKET because the lifetimes differ: a denial is worth remembering for a day
// so a reader can act on it, and a finished job is worth remembering for a day so a reader can read
// it. Same number, different reasons — and a shared bucket would make one retention policy serve two.
const DefaultBucket = "denials"

// DefaultTTL is how long a refusal stays visible.
//
// A DAY, BECAUSE IT IS A REMINDER RATHER THAN A LOG. Long enough to survive "I will fix that
// tomorrow", short enough that a permission granted this morning does not leave a stale complaint
// sitting in the panel a month later. A refusal that matters for longer than a day is one somebody
// should have noticed already.
const DefaultTTL = 24 * time.Hour

// Record is one refused capability, aggregated.
//
// BY CAPABILITY, NOT BY ATTEMPT. The same tool refused twenty times is one thing the reader has to
// decide about, and twenty rows would bury it. `Count` and `LastAt` keep the repetition legible
// without turning the list into a log.
type Record struct {
	ExtID      string `json:"extId"`
	Capability string `json:"capability"`
	// Tool is one tool that was refused for this capability — the most recent, because a reader
	// recognises a tool name faster than a capability key.
	Tool string `json:"tool"`
	// FirstAt and LastAt are Unix milliseconds.
	FirstAt int64 `json:"firstAt"`
	LastAt  int64 `json:"lastAt"`
	// Count of refusals folded into this record since FirstAt.
	Count int `json:"count"`
}

// Registry is the denial list for one deployment.
type Registry struct {
	kv     *sessionstore.Store
	bucket string
	ttl    time.Duration
}

// NewRegistry returns a registry over the shared denials bucket.
func NewRegistry(b bus.Bus) *Registry {
	return &Registry{kv: sessionstore.New(b), bucket: DefaultBucket, ttl: DefaultTTL}
}

// Key is the bucket key for one refused capability. The shape lives in sessionstore.
func Key(tenant, session, extID, capability string) string {
	return sessionstore.Key(tenant, session, extID, capability)
}

// SessionPrefix is the wildcard for one conversation's refusals.
func SessionPrefix(tenant, session string) string {
	return sessionstore.Prefix(tenant, session)
}

// Record notes one refusal.
//
// IT NEVER FAILS THE CALL IT IS ABOUT. The gate has already decided to refuse; a registry that
// returned an error here would turn "you may not do this" into "something broke", which is a worse
// answer for everybody. Errors are the caller's to log.
//
// THE COUNT IS READ-MODIFY-WRITE, and a lost increment is acceptable: two refusals racing produce a
// count off by one, and the reader is deciding whether to grant a permission, not auditing a number.
// Making this exact would need a compare-and-set retry loop on a note nobody bills against.
func (r *Registry) Record(ctx context.Context, tenant, session, extID, capability, tool string) error {
	now := time.Now().UnixMilli()
	key := Key(tenant, session, extID, capability)

	rec := Record{ExtID: extID, Capability: capability, Tool: tool, FirstAt: now, LastAt: now, Count: 1}
	var prev Record
	if ok, err := r.kv.Get(ctx, r.bucket, key, &prev); err == nil && ok {
		// KEEP THE ORIGINAL first sighting: "when did this start" is the question a reader asks, and
		// overwriting it would make a week-old refusal look like it happened just now.
		rec.FirstAt = prev.FirstAt
		rec.Count = prev.Count + 1
	}
	// A PUT, NOT A CREATE — the rule and its reasons live in sessionstore, which learned them the hard
	// way on behalf of the job heartbeat.
	return r.kv.Put(ctx, r.bucket, key, rec, r.ttl)
}

// List returns one conversation's refusals, newest first.
func (r *Registry) List(ctx context.Context, tenant, session string) ([]Record, error) {
	rows, err := r.kv.List(ctx, r.bucket, SessionPrefix(tenant, session))
	if err != nil {
		return nil, err
	}
	out := make([]Record, 0, len(rows))
	for _, row := range rows {
		var rec Record
		// A ROW THIS VERSION CANNOT READ IS SKIPPED, not fatal: a neighbour release may have written
		// it, and dropping the whole list over one row would hide the refusals that do parse.
		if unmarshalRecord(row.Value, &rec) == nil {
			out = append(out, rec)
		}
	}
	return out, nil
}

// Clear forgets one capability's refusals.
//
// CALLED WHEN THE GRANT LANDS. A refusal list that keeps complaining about a permission the reader
// just granted is worse than no list: it teaches them the list is noise.
func (r *Registry) Clear(ctx context.Context, tenant, session, extID, capability string) error {
	return r.kv.Delete(ctx, r.bucket, Key(tenant, session, extID, capability))
}

// unmarshalRecord is the ONLY place a refusal becomes bytes or back — one decoder, so a writer and a
// reader in different releases cannot drift on field names.
func unmarshalRecord(raw string, rec *Record) error { return json.Unmarshal([]byte(raw), rec) }
