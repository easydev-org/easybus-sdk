package agent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	easybus "github.com/easydev-org/easybus-sdk"
	"github.com/easydev-org/easybus-sdk/bus"
	"github.com/easydev-org/easybus-sdk/extension"
	"github.com/easydev-org/easybus-sdk/protocol"
)

// ToolResult is a resolved tool result. Content is the natural-language
// text ("" when absent — mirroring the TS side, where content is optional
// rather than nullable); Data is the structured result; Object/Error match
// the wire offload / in-band error shapes; Metadata is the opaque custom
// JSON the tool may attach (carried through verbatim).
type ToolResult struct {
	Content  string
	Data     any
	Object   *easybus.ObjectRef
	Error    *easybus.ErrorPayload
	Metadata any
}

// MailboxMessageResolved is a delivered mailbox message.
type MailboxMessageResolved struct {
	ID          string
	Tenant      string
	SessionName string
	Type        string
	Payload     any
	// Source is the message origin (see PublishMailbox).
	Source string
}

// Agent is the agent-side role.
type Agent struct {
	b               bus.Bus
	configAuthority *ConfigAuthority
	// manifestCache is keyed by extId (one manifest per extension). With N
	// replicas, presence contributes N KV keys under `<extId>.`; the first live
	// replica populates the entry, siblings refresh it, and it is dropped only
	// when the LAST replica's key expires.
	manifestCache map[string]*easybus.ExtensionManifest
	// livePresenceKeys holds the unexpired presence KV keys per extId, so an
	// extId stays "online" while any replica key remains and goes offline once
	// all have expired (or been deleted).
	livePresenceKeys map[string]map[string]bool
	presenceMu       sync.Mutex
	// presenceStartMu guards presenceStarted, which is set once the presence
	// watcher is running. It is NOT a sync.Once: a failed start must be
	// retryable, and reassigning a sync.Once inside its own Do() panics
	// ("sync: unlock of unlocked mutex") because Do holds the Once's mutex.
	presenceStartMu sync.Mutex
	presenceStarted bool
}

func New(b bus.Bus) *Agent {
	return &Agent{b: b, manifestCache: map[string]*easybus.ExtensionManifest{}, livePresenceKeys: map[string]map[string]bool{}}
}

func (a *Agent) Bus() bus.Bus { return a.b }

// Discover collects every reachable extension manifest.
// ExtSupports reports whether a discovered extension advertises a protocol
// feature (absent = pre-0.3 baseline). Call Discover first.
func (a *Agent) ExtSupports(extID, feature string) bool {
	m := a.manifest(extID)
	if m == nil {
		return false
	}
	for _, f := range m.Features {
		if f == feature {
			return true
		}
	}
	return false
}

// Manifest returns the cached discovery manifest for extID, or nil when the
// extension is unknown or currently offline. Call Discover first (or let the
// presence watcher warm the cache). With replicas, this is the single manifest
// shared by the extId while ANY replica is alive.
func (a *Agent) Manifest(extID string) *easybus.ExtensionManifest { return a.manifest(extID) }

func (a *Agent) Discover(ctx context.Context, maxWaitMs int) ([]*easybus.ExtensionManifest, error) {
	// Presence-first: extensions heartbeat their manifests into the
	// easybus-presence KV bucket; the watcher keeps the cache live (offline
	// extensions drop out via key TTL). Only a cold cache falls back to the
	// broadcast.
	a.ensurePresence(ctx)
	a.presenceMu.Lock()
	n := len(a.manifestCache)
	a.presenceMu.Unlock()
	if n > 0 {
		// One manifest per extId (the cache is keyed by id, so this is already
		// de-duplicated across replicas).
		out := make([]*easybus.ExtensionManifest, 0, n)
		a.presenceMu.Lock()
		for _, m := range a.manifestCache {
			out = append(out, m)
		}
		a.presenceMu.Unlock()
		return out, nil
	}
	replies, err := a.b.RequestMany(ctx, protocol.ChDiscover, map[string]any{}, bus.RequestOpts{MaxWaitMs: maxWaitMs, Tenant: protocol.GlobalTenant})
	if err != nil {
		return nil, err
	}
	seen := map[string]bool{}
	out := []*easybus.ExtensionManifest{}
	for _, r := range replies {
		m := &easybus.ExtensionManifest{}
		if !protocol.Coerce(r.Payload, m) {
			continue
		}
		if seen[m.Id] {
			continue
		}
		seen[m.Id] = true
		a.cacheManifest(m)
		out = append(out, m)
	}
	return out, nil
}

// ensurePresence starts the easybus-presence watcher once per agent. It fills
// manifestCache from the bucket snapshot and live updates (puts and TTL
// expiries both arrive), so Discover serves from cache without polling.
//
// Presence is per REPLICA: the bucket holds one key `<extId>.<replicaId>` per
// replica. The cache is keyed by extId, and an extId is offline only once the
// LAST of its replica keys has expired — a single replica going away must not
// evict the extension while siblings are still alive.
func (a *Agent) ensurePresence(ctx context.Context) {
	a.presenceStartMu.Lock()
	defer a.presenceStartMu.Unlock()
	if a.presenceStarted {
		return
	}
	ch, stop, err := a.b.KvWatch(ctx, extension.PresenceBucket, ">")
	if err != nil {
		// Leave presenceStarted false so a later Discover retries — e.g. once
		// the presence bucket exists. (Holding the lock across KvWatch keeps a
		// second caller from starting a duplicate watcher.)
		return
	}
	a.presenceStarted = true
	go func() {
		for ev := range ch {
			if ev.Key == "" {
				continue
			}
			a.applyPresenceEvent(ev)
		}
	}()
	_ = stop
}

// applyPresenceEvent folds one presence KV event into the cache, tracking
// per-extId replica-key counts so the extension drops out only when its last
// key expires. The synthetic snapshot-boundary event (Done, empty key) is
// ignored by the caller's `ev.Key == ""` guard.
func (a *Agent) applyPresenceEvent(ev bus.KvEvent) {
	extID := extension.PresenceExtID(ev.Key)
	a.presenceMu.Lock()
	defer a.presenceMu.Unlock()
	if a.livePresenceKeys == nil {
		a.livePresenceKeys = map[string]map[string]bool{}
	}
	if ev.Deleted {
		a.forgetPresenceKey(extID, ev.Key)
		return
	}
	var m easybus.ExtensionManifest
	if json.Unmarshal([]byte(ev.Value), &m) != nil || m.Id == "" {
		return
	}
	a.trackPresenceKey(extID, ev.Key)
	a.manifestCache[m.Id] = &m
}

// trackPresenceKey records that key is live for extID (idempotent).
func (a *Agent) trackPresenceKey(extID, key string) {
	// The per-key set makes this idempotent: repeated puts of the same replica
	// key do not inflate the count, and deletes of unknown keys are no-ops.
	if a.livePresenceKeys[extID] == nil {
		a.livePresenceKeys[extID] = map[string]bool{}
	}
	a.livePresenceKeys[extID][key] = true
}

// forgetPresenceKey drops one replica key; the extId goes offline only when
// its last key is gone.
func (a *Agent) forgetPresenceKey(extID, key string) {
	set := a.livePresenceKeys[extID]
	if set == nil {
		return
	}
	delete(set, key)
	if len(set) == 0 {
		delete(a.livePresenceKeys, extID)
		// Last replica gone: the extension is offline.
		delete(a.manifestCache, extID)
	}
}

// CallTool invokes a tool and returns its single terminal result. The tenant
// and sessionName ride the envelope's first-class fields.
func (a *Agent) CallTool(ctx context.Context, tenant, sessionName, extID, tool, callID string, args map[string]any) (ToolResult, error) {
	reply, err := a.b.Request(ctx, protocol.ChToolCall(tenant, extID, tool), easybus.ToolCallEnvelope{CallId: callID, Arguments: easybus.Struct(args)}, bus.RequestOpts{SessionName: sessionName, Tenant: tenant})
	if err != nil {
		return ToolResult{}, err
	}
	var tr easybus.ToolResult
	if !protocol.Coerce(reply.Payload, &tr) {
		return ToolResult{}, fmt.Errorf("tool %q: invalid result", tool)
	}
	r := ToolResult{Data: easybus.ValueInterface(tr.Data), Metadata: easybus.ValueInterface(tr.Metadata)}
	if tr.Content != nil {
		r.Content = *tr.Content
	}
	if tr.Error != nil {
		r.Error = &easybus.ErrorPayload{Code: easybus.ErrorPayloadCode(tr.Error.Code), Message: tr.Error.Message}
	}
	if tr.Object != nil {
		ct := tr.Object.ContentType
		r.Object = &easybus.ObjectRef{Id: tr.Object.Id}
		if ct != nil {
			r.Object.ContentType = ct
		}
	}
	return r, nil
}

// ResolveVariable resolves a template variable. KV-first: a cached value in
// the vars bucket wins (extensions cache resolved values there); on a miss
// the lazy resolver request carries session_name so session-scoped resolvers
// work, and extensions typically write the result back to the KV cache.
func (a *Agent) ResolveVariable(ctx context.Context, tenant, sessionName, provider, name string) (string, bool) {
	cached, err := a.b.KVGet(ctx, protocol.VarsBucket, protocol.SessionVarKey(tenant, provider, sessionName, name))
	if err == nil && cached != "" {
		return cached, true
	}
	cachedGlobal, err := a.b.KVGet(ctx, protocol.VarsBucket, protocol.VarKey(tenant, provider, name))
	if err == nil && cachedGlobal != "" {
		return cachedGlobal, true
	}
	reply, err := a.b.Request(ctx, protocol.ChVariable(tenant, provider, name), map[string]any{"name": name}, bus.RequestOpts{TimeoutMs: 2000, SessionName: sessionName, Tenant: tenant})
	if err != nil {
		return "", false
	}
	var v easybus.ExtensionVariableValue
	if !protocol.Coerce(reply.Payload, &v) {
		return "", false
	}
	return v.Value, true
}

// CallHook fires a sync call-hook.
func (a *Agent) CallHook(ctx context.Context, tenant, extID, hook, sessionName string, args map[string]any) (*easybus.HookResponse, error) {
	reply, err := a.b.Request(ctx, protocol.ChHookCall(tenant, extID, hook), easybus.HookCall{Hook: hook, SessionName: sessionName, Arguments: easybus.Struct(args)}, bus.RequestOpts{Tenant: tenant})
	if err != nil {
		return nil, err
	}
	var hr easybus.HookResponse
	if !protocol.Coerce(reply.Payload, &hr) {
		return nil, fmt.Errorf("hook %q: invalid response", hook)
	}
	return &hr, nil
}

// Interrupt asks an extension to interrupt in-flight work.
// Interrupt signals the extension to abort ALL in-flight work (broadcast:
// no session scoping). For one session use InterruptSession.
func (a *Agent) Interrupt(ctx context.Context, tenant, extID string) error {
	return a.b.Publish(ctx, protocol.ChInterrupt(tenant, extID), &easybus.InterruptSignal{}, bus.PublishOpts{Tenant: tenant})
}

// InterruptSession aborts only the in-flight tool calls of one session.
func (a *Agent) InterruptSession(ctx context.Context, tenant, extID, sessionName string, reason ...string) error {
	sig := &easybus.InterruptSignal{SessionName: &sessionName}
	if len(reason) > 0 {
		sig.Reason = &reason[0]
	}
	return a.b.Publish(ctx, protocol.ChInterrupt(tenant, extID), sig, bus.PublishOpts{Tenant: tenant})
}

// PublishLifecycleEvent announces a session lifecycle change on
// easybus.session.lifecycle.<kind> (created / forked / renamed / deleted).
// forked carries parent, renamed carries from/to (to == sessionName).
// Extensions that declared the kind receive it; session-scoped config
// overrides are cleaned up on "deleted".
func (a *Agent) PublishLifecycleEvent(ctx context.Context, tenant, kind, sessionName string, payload any) error {
	ev := &easybus.LifecycleEvent{
		Kind:        easybus.LifecycleKindFromString(kind),
		SessionName: sessionName,
		Payload:     easybus.Value(payload),
	}
	if kind == "renamed" {
		if from, ok := payload.(map[string]any); ok {
			if f, ok := from["from"].(string); ok {
				fv := f
				ev.From = &fv
			}
			if t, ok := from["to"].(string); ok {
				tv := t
				ev.To = &tv
			}
		}
	}
	if err := a.b.Publish(ctx, protocol.ChLifecycle(tenant, kind), ev, bus.PublishOpts{Tenant: tenant}); err != nil {
		return err
	}
	if kind == "deleted" {
		// Best-effort: drop this session's config overrides for every
		// extension we know about (manifest cache), plus any cached vars.
		for extID := range a.manifestCache {
			a.DropSessionConfig(ctx, tenant, extID, sessionName)
		}
	}
	return nil
}

// PublishEventHook fires an async event-hook.
func (a *Agent) PublishEventHook(ctx context.Context, tenant, hook, sessionName string, payload any) error {
	return a.b.Publish(ctx, protocol.ChHookEvent(tenant, hook), easybus.HookEvent{Hook: hook, SessionName: sessionName, Payload: easybus.Value(payload)}, bus.PublishOpts{Tenant: tenant})
}

// SubscribeProgress subscribes to in-flight progress telemetry for a tool
// call (one-way `pub` on easybus.tool.progress.<callId>). Consumed by the
// orchestration layer / UI, never fed into the LLM context.
func (a *Agent) SubscribeProgress(ctx context.Context, tenant, callID string) (bus.Subscription, error) {
	return a.b.Subscribe(ctx, protocol.ChToolProgress(tenant, callID), bus.SubscribeOpts{})
}

// PublishMailbox drops a message into a session's durable mailbox.
//
// Type is `trigger` (drives a turn), `interrupt`, or `event` (context only);
// it is a free-form string on the wire. Source records the ORIGIN: `user`
// (a human prompt), `session:{session}` (another session), `system:{name}`
// (automation), or an extension-defined value; pass "" to omit it.
func (a *Agent) PublishMailbox(ctx context.Context, tenant, sessionName, typ string, payload any, source ...string) error {
	id := protocol.NewID()
	var src *string
	if len(source) > 0 && source[0] != "" {
		s := source[0]
		src = &s
	}
	return a.b.InboxPublish(ctx, protocol.ChMailbox(tenant, sessionName), easybus.MailboxMessage{Id: id, Type: typ, Payload: easybus.Value(payload), Source: src}, bus.InboxPublishOpts{ID: id, SessionName: sessionName, Tenant: tenant})
}

// DefaultMaxNaksBeforeTerm is the poison-message escalation threshold:
// a handler that keeps failing gets the message redelivered this many
// times; after that ConsumeMailbox terms it into the dead-letter stream
// instead of nak-looping forever.
const DefaultMaxNaksBeforeTerm = 5

// TermError is returned by a mailbox handler to terminate a message:
// delivery stops and (unless NoDLQ) the message is copied to the
// dead-letter stream for triage.
type TermError struct{ NoDLQ bool }

func (e *TermError) Error() string { return "terminated by consumer" }

// ConsumeMailbox consumes the durable mailbox wildcard with explicit
// ack/nak/term, resolving each message to its session. Handler errors nak
// (redelivery after delayMs); *TermError terms (optionally without the
// dead-letter copy); malformed messages are acked and skipped.
// The returned cancel func closes the subscription.
func (a *Agent) ConsumeMailbox(ctx context.Context, handler func(MailboxMessageResolved) error, nakDelayMs int, maxNaks ...int) (func(), error) {
	limit := DefaultMaxNaksBeforeTerm
	if len(maxNaks) > 0 && maxNaks[0] > 0 {
		limit = maxNaks[0]
	}
	sub, err := a.b.InboxConsume(ctx, bus.InboxConsumeOpts{Subject: protocol.MailboxWildcardAll + ">"})
	if err != nil {
		return nil, err
	}
	var nakMu sync.Mutex
	nakCount := map[string]int{}
	forget := func(id string) {
		nakMu.Lock()
		delete(nakCount, id)
		nakMu.Unlock()
	}
	go func() {
		for {
			msg, ok := sub.Next(ctx)
			if !ok {
				return
			}
			env := msg.Envelope
			sessionName := ""
			if env.SessionName != nil {
				sessionName = *env.SessionName
			}
			tenant := env.Tenant
			if tenant == "" {
				tenant = protocol.SubjectTenant(env.Ch)
			}
			var m easybus.MailboxMessage
			if sessionName == "" || tenant == "" || env.Id == nil || *env.Id == "" || !protocol.Coerce(env.Payload, &m) {
				msg.Ack()
				continue
			}
			src := ""
			if m.Source != nil {
				src = *m.Source
			}
			switch err := handler(MailboxMessageResolved{ID: m.Id, Tenant: tenant, SessionName: sessionName, Type: m.Type, Payload: m.Payload, Source: src}); {
			case err == nil:
				forget(m.Id)
				msg.Ack()
			default:
				var te *TermError
				if errors.As(err, &te) {
					forget(m.Id)
					if te.NoDLQ {
						msg.TermNoDLQ()
					} else {
						msg.Term()
					}
					break
				}
				nakMu.Lock()
				nakCount[m.Id]++
				n := nakCount[m.Id]
				nakMu.Unlock()
				if n >= limit {
					// poison: stop the redelivery loop, park it in the DLQ
					forget(m.Id)
					msg.Term()
					break
				}
				msg.Nak(nakDelayMs)
			}
		}
	}()
	return func() { _ = sub.Close() }, nil
}

// ConsumeDLQ consumes the dead-letter stream (easybus.dlq.<token>): messages
// that a consumer Term()'d, kept in their original shape for inspection.
// This is the ops surface for poison-message triage.
func (a *Agent) ConsumeDLQ(ctx context.Context, handler func(MailboxMessageResolved) error) (func(), error) {
	sub, err := a.b.InboxConsume(ctx, bus.InboxConsumeOpts{Subject: "easybus.*.dlq.>"})
	if err != nil {
		return nil, err
	}
	go func() {
		for {
			msg, ok := sub.Next(ctx)
			if !ok {
				return
			}
			env := msg.Envelope
			sessionName := ""
			if env.SessionName != nil {
				sessionName = *env.SessionName
			}
			tenant := env.Tenant
			if tenant == "" {
				tenant = protocol.SubjectTenant(env.Ch)
			}
			var m easybus.MailboxMessage
			if sessionName == "" || tenant == "" || env.Id == nil || !protocol.Coerce(env.Payload, &m) {
				msg.TermNoDLQ() // dead letters of dead letters help nobody
				continue
			}
			if handler(MailboxMessageResolved{ID: m.Id, Tenant: tenant, SessionName: sessionName, Type: m.Type, Payload: m.Payload}) != nil {
				msg.Nak(5000)
			} else {
				msg.Ack()
			}
		}
	}()
	return func() { _ = sub.Close() }, nil
}

// RequeueDLQ takes one dead-lettered message (by its original id) and
// publishes it back onto the session's mailbox with a fresh id, acking the
// dead-letter copy. This is the return path for triage: fix the consumer,
// then requeue the parked payloads.
func (a *Agent) RequeueDLQ(ctx context.Context, id string) (bool, error) {
	sub, err := a.b.InboxConsume(ctx, bus.InboxConsumeOpts{Subject: "easybus.*.dlq.>"})
	if err != nil {
		return false, err
	}
	defer func() { _ = sub.Close() }()
	deadline := time.After(10 * time.Second)
	for {
		select {
		case <-deadline:
			return false, nil
		default:
		}
		cctx, cancel := context.WithTimeout(ctx, 2*time.Second)
		msg, ok := sub.Next(cctx)
		cancel()
		if !ok {
			continue
		}
		env := msg.Envelope
		sessionName := ""
		if env.SessionName != nil {
			sessionName = *env.SessionName
		}
		tenant := env.Tenant
		if tenant == "" {
			tenant = protocol.SubjectTenant(env.Ch)
		}
		var m easybus.MailboxMessage
		if sessionName == "" || tenant == "" || !protocol.Coerce(env.Payload, &m) || m.Id != id {
			// not ours: put it back in the DLQ after a short delay
			msg.Nak(500)
			continue
		}
		newID := protocol.NewID()
		if err := a.b.InboxPublish(ctx, protocol.ChMailbox(tenant, sessionName), easybus.MailboxMessage{Id: newID, Type: m.Type, Payload: m.Payload}, bus.InboxPublishOpts{ID: newID, SessionName: sessionName, Tenant: tenant}); err != nil {
			msg.Nak(500)
			return false, err
		}
		msg.Ack()
		return true, nil
	}
}

// PutObject / GetObject proxy the tenant-scoped object store.
func (a *Agent) PutObject(ctx context.Context, tenant, name string, data []byte) error {
	return a.b.ObjectPut(ctx, protocol.TenantObjectName(tenant, name), data)
}
func (a *Agent) GetObject(ctx context.Context, tenant, name string) ([]byte, error) {
	return a.b.ObjectGet(ctx, protocol.TenantObjectName(tenant, name))
}

// PutObjectPersistent stores bytes in the durable (no-TTL) object bucket.
func (a *Agent) PutObjectPersistent(ctx context.Context, tenant, name string, data []byte) error {
	return a.b.ObjectPutPersistent(ctx, protocol.TenantObjectName(tenant, name), data)
}
func (a *Agent) GetObjectPersistent(ctx context.Context, tenant, name string) ([]byte, error) {
	return a.b.ObjectGetPersistent(ctx, protocol.TenantObjectName(tenant, name))
}

func (a *Agent) Close() error { return a.b.Close() }
