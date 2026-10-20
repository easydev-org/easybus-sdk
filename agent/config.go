package agent

import (
	"context"
	"encoding/json"
	"fmt"

	easybus "github.com/easydev-org/easybus-sdk"
	"github.com/easydev-org/easybus-sdk/bus"
	"github.com/easydev-org/easybus-sdk/extension"
	"github.com/easydev-org/easybus-sdk/protocol"
)

// ConfigError is the typed error set by agent-side config operations.
type ConfigError struct {
	Code    string // invalid_argument | not_found | retryable
	Message string
}

func (e *ConfigError) Error() string { return e.Code + ": " + e.Message }

type configValue struct {
	Revision int64
	Value    any
}

// ConfigAuthority is the agent-side config source of truth: it validates
// writes against manifest declarations, mirrors values into the KV `cfg`
// bucket (caps.kv), serves startup snapshot requests, and delivers changes to
// extensions as 1:1 reqs with an optional ack.
type ConfigAuthority struct {
	bus        bus.Bus
	defaultAck bool

	declarations map[string]map[string][]*easybus.ExtensionConfigItem    // tenant -> extId -> items
	global       map[string]map[string]map[string]configValue            // tenant -> extId -> name -> v
	sessions     map[string]map[string]map[string]map[string]configValue // tenant -> extId -> session -> name -> v
	cancel       context.CancelFunc
	started      bool
}

// ServeConfig turns the agent into the config authority. Call once before
// SetConfig; safe to call again (idempotent).
func (a *Agent) ServeConfig(defaultAck bool) error {
	if a.configAuthority == nil {
		a.configAuthority = &ConfigAuthority{
			bus:          a.b,
			defaultAck:   defaultAck,
			declarations: map[string]map[string][]*easybus.ExtensionConfigItem{},
			global:       map[string]map[string]map[string]configValue{},
			sessions:     map[string]map[string]map[string]map[string]configValue{},
		}
	}
	if a.configAuthority.started {
		return nil
	}
	ctx, cancel := context.WithCancel(context.Background())
	a.configAuthority.cancel = cancel
	a.configAuthority.started = true
	// Snapshot serving moved to the cfg KV bucket: extensions recover state
	// by reading/watching it, so no easybus.config.get subscription remains.
	_ = ctx
	return nil
}

// SetConfig validates against the (cached or provided) manifest, persists via
// the KV mirror, then delivers with an ack. A rejection rolls the value back
// and returns *ConfigError.
func (a *Agent) SetConfig(ctx context.Context, tenant, extID, name string, value any, sessionName string, manifest *easybus.ExtensionManifest, ack *bool) error {
	c := a.configAuthority
	if c == nil || !c.started {
		return &ConfigError{Code: "invalid_argument", Message: "ServeConfig() was not called"}
	}
	m := manifest
	if m == nil {
		cached := a.manifest(extID)
		if cached == nil {
			return &ConfigError{Code: "not_found", Message: fmt.Sprintf("no manifest for %s; Discover() first", extID)}
		}
		m = cached
	}
	c.declare(tenant, m)
	return c.set(ctx, tenant, extID, name, value, sessionName, ack)
}

func (c *ConfigAuthority) declare(tenant string, m *easybus.ExtensionManifest) {
	if c.declarations[tenant] == nil {
		c.declarations[tenant] = map[string][]*easybus.ExtensionConfigItem{}
	}
	if m.Config == nil {
		c.declarations[tenant][m.Id] = nil
		return
	}
	// The manifest already carries the config items directly.
	c.declarations[tenant][m.Id] = m.Config
	c.recover(tenant, m.Id)
}

// configKVEnvelope is the persisted shape: the value plus the per-key
// revision, so both survive an agent restart.
type configKVEnvelope struct {
	Revision int64 `json:"r"`
	Value    any   `json:"v"`
}

func decodeConfigEnvelope(raw string) (configKVEnvelope, bool) {
	var env configKVEnvelope
	if json.Unmarshal([]byte(raw), &env) == nil && env.Value != nil {
		return env, true
	}
	// pre-0.2 entries stored the bare value (revision 0)
	var v any
	if json.Unmarshal([]byte(raw), &v) == nil {
		return configKVEnvelope{Revision: 0, Value: v}, true
	}
	return configKVEnvelope{}, false
}

func (c *ConfigAuthority) recover(tenant, extID string) {
	for _, item := range c.declarations[tenant][extID] {
		raw, _ := c.bus.KVGet(context.Background(), protocol.ConfigKVBucket, configKVKey(tenant, extID, "global", "", item.Name))
		if raw == "" {
			continue
		}
		if env, ok := decodeConfigEnvelope(raw); ok {
			if c.global[tenant] == nil {
				c.global[tenant] = map[string]map[string]configValue{}
			}
			if c.global[tenant][extID] == nil {
				c.global[tenant][extID] = map[string]configValue{}
			}
			if cur, exists := c.global[tenant][extID][item.Name]; !exists || env.Revision > cur.Revision {
				c.global[tenant][extID][item.Name] = configValue{Revision: env.Revision, Value: env.Value}
			}
		}
	}
}

func (c *ConfigAuthority) set(ctx context.Context, tenant, extID, name string, value any, sessionName string, ack *bool) error {
	var item *easybus.ExtensionConfigItem
	items := c.declarations[tenant][extID]
	for _, it := range items {
		if it.Name == name {
			item = it
			break
		}
	}
	if item == nil {
		return &ConfigError{Code: "not_found", Message: fmt.Sprintf("config %s not declared by %s", name, extID)}
	}
	scope := item.Scope
	if scope == "" {
		scope = "global"
	}
	if scope == "session" && sessionName == "" {
		return &ConfigError{Code: "invalid_argument", Message: fmt.Sprintf("config %s requires a session name (scope=session)", name)}
	}
	if msg := validateConfigValue(item, value); msg != "" {
		return &ConfigError{Code: "invalid_argument", Message: msg}
	}

	useAck := c.defaultAck
	if ack != nil {
		useAck = *ack
	}

	// Bump revision per (extId, scope, session, name) key.
	var revision int64
	if c.global[tenant] == nil {
		c.global[tenant] = map[string]map[string]configValue{}
	}
	if c.global[tenant][extID] == nil {
		c.global[tenant][extID] = map[string]configValue{}
	}
	if scope == "global" {
		revision = c.global[tenant][extID][name].Revision + 1
		c.global[tenant][extID][name] = configValue{Revision: revision, Value: value}
	} else {
		if c.sessions[tenant] == nil {
			c.sessions[tenant] = map[string]map[string]map[string]configValue{}
		}
		if c.sessions[tenant][extID] == nil {
			c.sessions[tenant][extID] = map[string]map[string]configValue{}
		}
		if c.sessions[tenant][extID][sessionName] == nil {
			c.sessions[tenant][extID][sessionName] = map[string]configValue{}
		}
		revision = c.sessions[tenant][extID][sessionName][name].Revision + 1
		c.sessions[tenant][extID][sessionName][name] = configValue{Revision: revision, Value: value}
	}

	// Persist first (crash-safe): the cfg KV bucket is the source of truth —
	// extensions recover by reading/watching it (revision rides along so a
	// restarted agent restores its revision counters too).
	raw, err := json.Marshal(configKVEnvelope{Revision: revision, Value: value})
	if err == nil {
		_ = c.bus.KVPut(ctx, protocol.ConfigKVBucket, configKVKey(tenant, extID, scope, sessionName, name), string(raw), 0)
	}

	// Deliver as 1:1 req with optional ack.
	ackVal := useAck
	tenantVal := tenant
	setPayload := easybus.ConfigSet{
		Name:     name,
		Value:    easybus.Value(value),
		Revision: int32(revision),
		Scope:    scope,
		Ack:      &ackVal,
		Tenant:   &tenantVal,
	}
	if sessionName != "" {
		setPayload.SessionName = &sessionName
	}
	// Deliver as 1:1 req. `ack=false` means the extension does not send a
	// HookResponse; to keep the call bounded (and the value applied
	// optimistically), wait a short grace period and ignore a timeout.
	reqOpts := bus.RequestOpts{TimeoutMs: 300, Tenant: tenant}
	if useAck {
		reqOpts.TimeoutMs = 5000
	}
	if sessionName != "" {
		reqOpts.SessionName = sessionName
	}
	reply, err := c.bus.Request(ctx, protocol.ChConfig(tenant, extID), &setPayload, reqOpts)
	if err != nil {
		// Delivery is best-effort in the 0.2 model: the value is already
		// committed to the cfg KV bucket (the source of truth), so an
		// offline extension (no responders) or a lost race is not a failure
		// — the extension recovers via its KV watch. Only an explicit
		// rejection below rolls the value back.
		return nil
	}
	if !useAck {
		return nil
	}
	var hr easybus.HookResponse
	if !protocol.Coerce(reply.Payload, &hr) {
		return &ConfigError{Code: "retryable", Message: "extension ack was not a HookResponse"}
	}
	if !hr.Ok {
		// Roll back memory + KV.
		if scope == "global" {
			if cv, ok := c.global[tenant][extID][name]; ok && cv.Revision == revision {
				delete(c.global[tenant][extID], name)
			}
		} else if cv, ok := c.sessions[tenant][extID][sessionName][name]; ok && cv.Revision == revision {
			delete(c.sessions[tenant][extID][sessionName], name)
		}
		_ = c.bus.KVDelete(ctx, protocol.ConfigKVBucket, configKVKey(tenant, extID, scope, sessionName, name))
		msg := "extension rejected the config change"
		if hr.Error != nil {
			msg = hr.Error.Message
		}
		return &ConfigError{Code: "retryable", Message: msg}
	}
	return nil
}

// DropSessionConfig removes per-session overrides when a session ends.
func (a *Agent) DropSessionConfig(ctx context.Context, tenant, extID, sessionName string) {
	c := a.configAuthority
	if c == nil {
		return
	}
	if vals, ok := c.sessions[tenant][extID][sessionName]; ok {
		for name := range vals {
			_ = c.bus.KVDelete(ctx, protocol.ConfigKVBucket, configKVKey(tenant, extID, "session", sessionName, name))
		}
		delete(c.sessions[tenant][extID], sessionName)
	}
}

func (c *ConfigAuthority) fillSnapshot(tenant, extID string, snap *easybus.ConfigSnapshot) {
	g := map[string]any{}
	for name, cv := range c.global[tenant][extID] {
		g[name] = cv.Value
	}
	sessions := map[string]map[string]any{}
	for sess, vals := range c.sessions[tenant][extID] {
		rec := map[string]any{}
		for name, cv := range vals {
			rec[name] = cv.Value
		}
		sessions[sess] = rec
	}
	for name, v := range g {
		snap.Global[name] = easybus.Value(v)
	}
	snap.Sessions = easybus.Value(sessions)
}

func configKVKey(tenant, extID, scope, sessionName, name string) string {
	if scope == "session" {
		return protocol.TenantKVKey(tenant, extID+"."+protocol.EscapeKVSegment(sessionName)+"."+name)
	}
	return protocol.TenantKVKey(tenant, extID+"."+name)
}

func validateConfigValue(item *easybus.ExtensionConfigItem, value any) string {
	switch item.Type {
	case "string":
		if _, ok := value.(string); !ok {
			return fmt.Sprintf("expected string, got %T", value)
		}
	case "number":
		if _, ok := value.(float64); !ok {
			if _, ok2 := value.(int); !ok2 {
				return fmt.Sprintf("expected number, got %T", value)
			}
		}
	case "boolean":
		if _, ok := value.(bool); !ok {
			return fmt.Sprintf("expected boolean, got %T", value)
		}
	case "list", "object":
		// A structured knob carries a JSON list/object. Accept it in the two forms
		// a caller actually has: an already-decoded slice/map, or the JSON text it
		// was stored as. Rejecting these made every `type: list` knob (sandboxes,
		// servers) IMPOSSIBLE to set through SetConfig, even though the manifest
		// declared it — the failure read as "unknown config type", which points at
		// the manifest instead of at this validator.
		if value == nil {
			return "expected a list or object, got nil"
		}
		if s, ok := value.(string); ok {
			var probe any
			if err := json.Unmarshal([]byte(s), &probe); err != nil {
				return fmt.Sprintf("expected JSON for a %s value, got unparseable string: %v", item.Type, err)
			}
		}
	case "enum":
		s, ok := value.(string)
		if !ok {
			return fmt.Sprintf("expected enum string, got %T", value)
		}
		if item.EnumValues != nil {
			for _, allowed := range item.EnumValues {
				if allowed == s {
					return ""
				}
			}
			return fmt.Sprintf("value %q not in the declared enum values", s)
		}
	case "json":
		return ""
	default:
		return fmt.Sprintf("unknown config type %q", string(item.Type))
	}
	return ""
}

// manifest returns the cached discovery manifest for extID.
func (a *Agent) manifest(extID string) *easybus.ExtensionManifest {
	a.presenceMu.Lock()
	defer a.presenceMu.Unlock()
	return a.manifestCache[extID]
}

// initManifestCache keeps manifests discovered via Discover() for SetConfig.
func (a *Agent) cacheManifest(m *easybus.ExtensionManifest) {
	a.presenceMu.Lock()
	defer a.presenceMu.Unlock()
	if a.manifestCache == nil {
		a.manifestCache = map[string]*easybus.ExtensionManifest{}
	}
	a.manifestCache[m.Id] = m
}

var _ = extension.TypedError{} // keep import surface aligned
