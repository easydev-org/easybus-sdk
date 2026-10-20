package extension

import (
	"context"
	"encoding/json"
	"strings"
	"sync"

	easybus "github.com/easydev-org/easybus-sdk"
	"github.com/easydev-org/easybus-sdk/bus"
	"github.com/easydev-org/easybus-sdk/protocol"
)

// Config knob kinds (Kind is an open string; these are the special values).
const (
	ConfigKindValue  = "value"
	ConfigKindModel  = "model"
	ConfigKindSecret = "secret"
)

// ConfigSpec declares one config knob in Extension.Config.
type ConfigSpec struct {
	Description string
	// Descriptions is the localized map (locale → text); Description is the
	// fallback, same convention as tool descriptions.
	Descriptions map[string]string
	Type         string // string | number | boolean | enum | json
	// Kind distinguishes an ordinary knob ("value", default) from a model
	// reference ("model") or a credential ("secret"). A model's value is a
	// `provider_id/model_id` reference (a picker scoped to Capability); a
	// secret's value is NEVER returned to a client (the agent withholds it and
	// reports only whether it is set). Mirrors the TS ConfigSpec.kind.
	Kind string // "value" | "model" | "secret"
	// Capability is REQUIRED when Kind == "model": the modality the reference
	// must match (text | image | video | speech | transcription | embedding |
	// rerank | realtime).
	Capability string
	EnumValues []string
	Default    any
	Scope      string // "global" | "session" (default global)
	// Fields types a STRUCTURED knob (Type "list" or "object"): the shape of each
	// object / each list item. Empty means the knob is untyped — a `json` blob,
	// which is the backward-compatible fallback every existing extension relies
	// on. With Fields a client can render a real form (one card per list item, a
	// "+" to add, a picker for an enum field) instead of a JSON textarea.
	Fields []FieldSpec
	// ItemType is the element type when Type == "list": "object" | "string" |
	// "number" | "boolean".
	ItemType string
	// MinItems / MaxItems bound a list (0 = unbounded).
	MinItems int
	MaxItems int
	// Namespace scopes this knob's stored value to one namespace of the
	// extension. Empty = extension-level (shared by all its namespaces).
	Namespace string
	// Required gates tools that depend on this config: an agent may refuse to
	// expose them until the value is set.
	Required bool
}

// FieldSpec types ONE key of a structured config value (or one list item's key).
//
// It exists so a client can render a real form instead of a JSON textarea: an
// `enum` becomes a picker, `kind: secret` a masked input, `format: uri` a URL
// field, and a `list` of objects becomes repeatable cards with a "+" to add.
// The SERVER validates against the same shape, so bypassing the UI cannot save
// a malformed value.
type FieldSpec struct {
	Name string
	// Type is string | number | boolean | enum | json | object.
	Type string
	// Kind is "value" (default) or "secret" (never echoed back to a client).
	Kind string
	// Format narrows a string: uri | host | path | email | duration | semver.
	// A client uses it for a keyboard/input type and a hint; the server uses it
	// to validate.
	Format       string
	EnumValues   []string
	Default      any
	Required     bool
	Description  string
	Descriptions map[string]string
	// Fields makes this field an object (one level of nesting: enough for every
	// current knob, and it keeps the renderer honest).
	Fields []FieldSpec
}

// OnConfigChangeFunc receives applied config changes. Returning an error (or
// a non-nil error) REJECTS the change: the agent keeps the old value.
// `get` reads the effective value of any knob.
type OnConfigChangeFunc func(ctx context.Context, name string, value any, sessionName string, get func(name, sessionName string) any) error

// ConfigStore keeps applied values per tenant: global set + per-session
// overrides. Mutated from TWO goroutines (live easybus.config reqs and the cfg KV
// watch), so every access goes through mu.
type ConfigStore struct {
	mu sync.Mutex
	// tenant -> name -> value
	global map[string]map[string]any
	// tenant -> session -> name -> value
	session map[string]map[string]map[string]any
}

func newConfigStore() *ConfigStore {
	return &ConfigStore{
		global:  map[string]map[string]any{},
		session: map[string]map[string]map[string]any{},
	}
}

// Get resolves session override > global set > declared default for a tenant.
func (s *ConfigStore) Get(specs map[string]ConfigSpec, tenant, name, sessionName string) any {
	s.mu.Lock()
	defer s.mu.Unlock()
	if sessionName != "" {
		if v, ok := s.session[tenant][sessionName][name]; ok {
			return v
		}
	}
	if v, ok := s.global[tenant][name]; ok {
		return v
	}
	if spec, ok := specs[name]; ok {
		return spec.Default
	}
	return nil
}

func (e *Extension) applyConfigSet(set *easybus.ConfigSet) {
	tenant := ""
	if set.Tenant != nil {
		tenant = *set.Tenant
	}
	e.configStore.mu.Lock()
	defer e.configStore.mu.Unlock()
	if set.Scope == easybus.ConfigSetScopeSession && set.SessionName != nil {
		if e.configStore.session[tenant] == nil {
			e.configStore.session[tenant] = map[string]map[string]any{}
		}
		if e.configStore.session[tenant][*set.SessionName] == nil {
			e.configStore.session[tenant][*set.SessionName] = map[string]any{}
		}
		e.configStore.session[tenant][*set.SessionName][set.Name] = set.Value.AsInterface()
		return
	}
	if e.configStore.global[tenant] == nil {
		e.configStore.global[tenant] = map[string]any{}
	}
	e.configStore.global[tenant][set.Name] = set.Value.AsInterface()
}

func (e *Extension) rollbackConfigSet(set *easybus.ConfigSet) {
	tenant := ""
	if set.Tenant != nil {
		tenant = *set.Tenant
	}
	e.configStore.mu.Lock()
	defer e.configStore.mu.Unlock()
	if set.Scope == easybus.ConfigSetScopeSession && set.SessionName != nil {
		delete(e.configStore.session[tenant][*set.SessionName], set.Name)
		return
	}
	delete(e.configStore.global[tenant], set.Name)
}

// serveConfig subscribes easybus.*.config.<id> (live sets with ack/reject) and
// recovers state from the cfg KV bucket — the watch delivers the snapshot
// at startup and live updates afterwards, so no agent needs to be online.
func (e *Extension) serveConfig(ctx context.Context) error {
	if len(e.cfg.Config) == 0 {
		return nil
	}
	if ch, stop, err := e.b.KvWatch(ctx, protocol.ConfigKVBucket, "t.*."+e.cfg.ID+".>"); err == nil {
		go func() {
			for ev := range ch {
				e.applyConfigKV(ev)
			}
		}()
		e.subs = append(e.subs, kvWatchStopper{stop})
	}
	sub, err := e.b.Subscribe(ctx, "easybus.*.config."+e.cfg.ID, bus.SubscribeOpts{Queue: e.cfg.ID})
	if err != nil {
		return err
	}
	e.subs = append(e.subs, sub)
	go func() {
		for {
			env, ok := sub.Next(ctx)
			if !ok {
				return
			}
			if env.ReplyTo == nil {
				continue
			}
			replyTo := *env.ReplyTo
			var set easybus.ConfigSet
			if !protocol.Coerce(env.Payload, &set) {
				continue
			}
			tenant := env.Tenant
			if tenant == "" {
				tenant = protocol.SubjectTenant(env.Ch)
			}
			if tenant != "" && set.Tenant == nil {
				t := tenant
				set.Tenant = &t
			}
			e.applyConfigSet(&set)
			var rejectErr error
			if e.cfg.OnConfigChange != nil {
				rejectErr = e.cfg.OnConfigChange(ctx, set.Name, set.Value.AsInterface(), deref(set.SessionName), func(name, sessionName string) any {
					return e.configStore.Get(e.cfg.Config, tenant, name, sessionName)
				})
			}
			if rejectErr != nil {
				e.rollbackConfigSet(&set)
			}
			if set.Ack != nil && *set.Ack {
				var res easybus.HookResponse
				if rejectErr != nil {
					res = easybus.HookResponse{Ok: false, Error: &easybus.ToolError{Code: easybus.HookResponseErrorCodeInternal, Message: rejectErr.Error()}}
				} else {
					res = easybus.HookResponse{Ok: true}
				}
				_ = e.b.Publish(ctx, replyTo, &res, bus.PublishOpts{Tenant: tenant})
			}
		}
	}()
	return nil
}

var _ = json.Marshal // reserved

// GetConfig exposes the effective config value (session > global > default)
// for a tenant.
func (e *Extension) GetConfig(tenant, name, sessionName string) any {
	return e.configStore.Get(e.cfg.Config, tenant, name, sessionName)
}

// ---------------------------------------------------------------------------
// Session-facing helpers (session events, mailbox, variables, objects).

// PublishSessionEvent pushes one SSE event onto the session's durable event
// stream (easybus.session.events.<token>) — the same channel the agent's SSE
// handler replays and live-tails. Extensions use it to notify UI listeners
// about side effects, e.g. "todos-updated" after a todowrite.
func (e *Extension) PublishSessionEvent(ctx context.Context, tenant, sessionName, event string, params any) error {
	id := protocol.NewID()
	return e.b.InboxPublish(ctx, protocol.ChSessionEvents(tenant, sessionName), map[string]any{
		"event":  event,
		"params": params,
		"eid":    id,
	}, bus.InboxPublishOpts{ID: id, SessionName: sessionName, Tenant: tenant})
}

// PublishMailboxEvent publishes a message to a session's durable mailbox
// (visible to the agent's ConsumeMailbox loop and any UI tailing it).
//
// eventType is `trigger` (drives a turn), `event` (context only), or
// `interrupt`; source records the origin ("" omits it).
func (e *Extension) PublishMailboxEvent(ctx context.Context, tenant, sessionName, eventType string, payload any, source ...string) error {
	if eventType == "" {
		eventType = "event"
	}
	var src *string
	if len(source) > 0 && source[0] != "" {
		s := source[0]
		src = &s
	}
	id := protocol.NewID()
	return e.b.InboxPublish(ctx, protocol.ChMailbox(tenant, sessionName), easybus.MailboxMessage{
		Id:      id,
		Type:    eventType,
		Payload: easybus.Value(payload),
		Source:  src,
	}, bus.InboxPublishOpts{ID: id, SessionName: sessionName, Tenant: tenant})
}

// PutObject stores a (potentially large) object.
func (e *Extension) PutObject(ctx context.Context, tenant, name string, data []byte) error {
	return e.b.ObjectPut(ctx, protocol.TenantObjectName(tenant, name), data)
}

// GetObject fetches a stored object (nil bytes when absent).
func (e *Extension) GetObject(ctx context.Context, tenant, name string) ([]byte, error) {
	return e.b.ObjectGet(ctx, protocol.TenantObjectName(tenant, name))
}

// PutObjectPersistent stores bytes in the durable (no-TTL) object bucket.
func (e *Extension) PutObjectPersistent(ctx context.Context, tenant, name string, data []byte) error {
	return e.b.ObjectPutPersistent(ctx, protocol.TenantObjectName(tenant, name), data)
}

// GetObjectPersistent fetches bytes from the durable (no-TTL) object bucket.
func (e *Extension) GetObjectPersistent(ctx context.Context, tenant, name string) ([]byte, error) {
	return e.b.ObjectGetPersistent(ctx, protocol.TenantObjectName(tenant, name))
}

// SetVariable stores a global variable (vars.<extId>.<name>).
func (e *Extension) SetVariable(ctx context.Context, tenant, name, value string) error {
	return e.b.KVPut(ctx, protocol.VarsBucket, protocol.VarKey(tenant, e.cfg.ID, name), value, 0)
}

// SetSessionVariable stores a session variable
// (vars.<extId>.<sessionToken>.<name>). Agents resolve variables KV-first and
// fall back to the lazy resolver, so writing here caches hot values.
func (e *Extension) SetSessionVariable(ctx context.Context, tenant, sessionName, name, value string) error {
	return e.b.KVPut(ctx, protocol.VarsBucket, protocol.SessionVarKey(tenant, e.cfg.ID, sessionName, name), value, 0)
}

// DeleteSessionVariables deletes every session-scoped variable of a session.
// Called automatically on the "deleted" lifecycle event.
func (e *Extension) DeleteSessionVariables(ctx context.Context, tenant, sessionName string) error {
	for name, spec := range e.cfg.Variables {
		if spec.Scope != "session" {
			continue
		}
		if err := e.b.KVDelete(ctx, protocol.VarsBucket, protocol.SessionVarKey(tenant, e.cfg.ID, sessionName, name)); err != nil {
			return err
		}
	}
	return nil
}

// GetSessionVariable reads a session variable by provider (e.g. "agent" for
// vars.agent.locale), falling back to the given default when absent. The
// provider id is explicit so an extension can read another extension's (or the
// agent's) projected KV value for a session.
func (e *Extension) GetSessionVariable(ctx context.Context, tenant, provider, sessionName, name, fallback string) string {
	if sessionName == "" {
		return fallback
	}
	v, err := e.b.KVGet(ctx, protocol.VarsBucket, protocol.SessionVarKey(tenant, provider, sessionName, name))
	if err != nil || v == "" {
		return fallback
	}
	return v
}

// applyConfigKV applies a cfg-bucket watch entry into the local store. Key
// layout: <extId>.<name> (global) or <extId>.<session>.<name> (session).
func (e *Extension) applyConfigKV(ev bus.KvEvent) {
	e.configStore.mu.Lock()
	defer e.configStore.mu.Unlock()
	// Layout: t.<tenant>.<extId>.<name> (global) or
	// t.<tenant>.<extId>.<escapedSession>.<name> (session).
	segs := strings.SplitN(ev.Key, ".", 4)
	if len(segs) < 3 || segs[0] != "t" {
		return
	}
	tenant := segs[1]
	rest := segs[2]
	if len(segs) == 4 {
		rest = segs[2] + "." + segs[3]
	}
	rest = strings.TrimPrefix(rest, e.cfg.ID+".")
	parts := strings.SplitN(rest, ".", 2)
	if len(parts) == 2 && strings.Contains(parts[1], ".") {
		// session-scoped: split the session segment off the remainder
		sessAndName := strings.SplitN(parts[1], ".", 2)
		parts = []string{parts[0], protocol.UnescapeKVSegment(sessAndName[0]), strings.Join(sessAndName[1:], ".")}
	}
	if ev.Deleted {
		switch len(parts) {
		case 1:
			delete(e.configStore.global[tenant], parts[0])
		case 3:
			if m := e.configStore.session[tenant][parts[1]]; m != nil {
				delete(m, parts[2])
			}
		}
		return
	}
	// Envelope {r,v} with a bare-value fallback (pre-0.2 entries).
	var env struct {
		Revision int64 `json:"r"`
		Value    any   `json:"v"`
	}
	v := any(env.Value)
	if json.Unmarshal([]byte(ev.Value), &env) == nil && env.Value != nil {
		v = env.Value
	} else {
		var raw any
		if json.Unmarshal([]byte(ev.Value), &raw) != nil {
			return
		}
		v = raw
	}
	if v == nil {
		return
	}
	if len(parts) == 1 {
		if e.configStore.global[tenant] == nil {
			e.configStore.global[tenant] = map[string]any{}
		}
		e.configStore.global[tenant][parts[0]] = v
	} else if len(parts) == 2 {
		sess := protocol.UnescapeKVSegment(parts[0])
		if e.configStore.session[tenant] == nil {
			e.configStore.session[tenant] = map[string]map[string]any{}
		}
		if e.configStore.session[tenant][sess] == nil {
			e.configStore.session[tenant][sess] = map[string]any{}
		}
		e.configStore.session[tenant][sess][parts[1]] = v
	}
}

// kvWatchStopper adapts a watch cancel func to the Subscription shape so
// Close() tears everything down.
type kvWatchStopper struct{ stop func() }

func (k kvWatchStopper) Next(ctx context.Context) (*easybus.Envelope, bool) {
	return nil, false
}
func (k kvWatchStopper) Close() error {
	if k.stop != nil {
		k.stop()
	}
	return nil
}
