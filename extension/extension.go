package extension

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	easybus "github.com/easydev-org/easybus-sdk"
	"github.com/easydev-org/easybus-sdk/bus"
	"github.com/easydev-org/easybus-sdk/protocol"
	"google.golang.org/protobuf/types/known/structpb"
)

// maxToolContentBytes caps a tool's `content` text returned to the agent. It
// is fed into the model's context, so text above this is REJECTED as a tool
// error; large data must be returned as a FILE (data.files + object store).
// 128 KiB ≈ 32k English tokens / ≈40k+ CJK tokens.
const maxToolContentBytes = 128 * 1024

// ToolResultData is what a tool Execute returns.
type ToolResultData struct {
	Content string
	Data    any
	Object  *easybus.ObjectRef
}

// TypedError carries a standard protocol error code from a tool execution.
// Plain errors map to `internal`; TypedError preserves its code on the wire.
type TypedError struct {
	Code    easybus.ToolResultErrorCode
	Message string
}

func (e *TypedError) Error() string { return e.Message }

// ToolSpec describes a tool and its executor.
//
// InputSchema is an opaque JSON-Schema blob. Convention (protocol-declared):
// any schema node — typically a property — may carry
// `descriptions: map[locale]string` next to `description`, mirroring the
// tool-level Descriptions field. Consumers resolve
// `descriptions[locale] → descriptions[primary-language] → description`
// and strip the `descriptions` key before the schema reaches a model.
type ToolSpec struct {
	Description  string
	Descriptions map[string]string
	InputSchema  map[string]any
	// RequiredConfig declares the config names this tool requires to run (a tool
	// may share a required config with sibling tools). Absent = no config gating.
	RequiredConfig []string
	// Namespace groups this tool with siblings that do the same kind of job
	// (pvc, build, storage, ...). Organisational only: the tool keeps its name.
	Namespace string
	Execute   func(ctx context.Context, args map[string]any, callID, sessionName, tenant string) (ToolResultData, error)
}

// NamespaceSpec declares one grouping of an extension's tools and config.
type NamespaceSpec struct {
	Name         string
	Description  string
	Descriptions map[string]string
}

// VariableSpec describes one template variable and its lazy resolver.
type VariableSpec struct {
	Description  string
	Descriptions map[string]string
	Scope        string // "global" | "session"
	Resolve      func(ctx context.Context, sessionName, tenant string) (string, error)
}

// Config describes an extension.
type Config struct {
	ID      string
	Version string
	// ReplicaID identifies THIS process among the extension's replicas when
	// several run in parallel. Each replica heartbeats its own presence key
	// (`<extId>.<replicaId>`) so replicas never overwrite or evict each other.
	// Resolution order: this field, then EASYBUS_REPLICA_ID, then POD_NAME, then a
	// random id (see resolveReplicaID). Leave empty to accept the defaults.
	ReplicaID string
	Tools     map[string]ToolSpec
	Variables map[string]VariableSpec
	Config    map[string]ConfigSpec
	// Namespaces declares this extension's groupings (pvc, build, storage, ...).
	// Tools and config knobs reference them via their Namespace field; the ORDER
	// here is what a client uses to lay the groups out.
	Namespaces []NamespaceSpec
	CallHooks  []string
	EventHooks []string
	Lifecycle  []string // created / forked / renamed / deleted
	// HookSchemas carries per-hook JSON schemas (call arguments / event
	// payloads); dispatch validates against them (reject / drop on mismatch).
	HookSchemas    HookSchemas
	OnCallHook     OnCallHook
	OnEventHook    OnEventHook
	OnConfigChange OnConfigChangeFunc
	OnLifecycle    OnLifecycleFunc
	// OnInterrupt is called after an interrupt signal cancelled the
	// session's in-flight calls. When nil, interrupts fall back to
	// OnEventHook(ctx, "interrupt", ...).
	OnInterrupt func(ctx context.Context, sessionName, reason, tenant string)
}

// HookSchemas maps hook name -> JSON schema (subset) for call arguments and
// event payloads.
type HookSchemas struct {
	Call  map[string]map[string]any
	Event map[string]map[string]any
}

// validateHookSchema implements the small declarative JSON-schema subset
// hook authors need: type (scalar + object), required, properties[*].type,
// and enum. Returns a message or "".
func validateHookSchema(schema map[string]any, value any, path string) string {
	if schema == nil {
		return ""
	}
	t, _ := schema["type"].(string)
	switch t {
	case "string":
		if _, ok := value.(string); !ok {
			return fmt.Sprintf("%s: expected string, got %T", path, value)
		}
	case "number":
		if _, ok := value.(float64); !ok {
			return fmt.Sprintf("%s: expected number, got %T", path, value)
		}
	case "boolean":
		if _, ok := value.(bool); !ok {
			return fmt.Sprintf("%s: expected boolean, got %T", path, value)
		}
	case "object":
		m, ok := value.(map[string]any)
		if !ok {
			return fmt.Sprintf("%s: expected object, got %T", path, value)
		}
		if req, ok := schema["required"].([]any); ok {
			for _, r := range req {
				if name, ok := r.(string); ok {
					if _, present := m[name]; !present {
						return fmt.Sprintf("%s: missing required field %q", path, name)
					}
				}
			}
		}
		if props, ok := schema["properties"].(map[string]any); ok {
			for name, p := range props {
				if v, present := m[name]; present {
					if sub, ok := p.(map[string]any); ok {
						if msg := validateHookSchema(sub, v, path+"."+name); msg != "" {
							return msg
						}
					}
				}
			}
		}
	}
	if enum, ok := schema["enum"].([]any); ok {
		hit := false
		for _, e := range enum {
			if fmt.Sprintf("%v", e) == fmt.Sprintf("%v", value) {
				hit = true
				break
			}
		}
		if !hit {
			return fmt.Sprintf("%s: value not in the declared enum", path)
		}
	}
	return ""
}

// OnLifecycleFunc receives session lifecycle events. Returning an error is
// logged by the SDK and otherwise ignored (lifecycle is best-effort).
type OnLifecycleFunc func(ctx context.Context, ev *easybus.LifecycleEvent, tenant string) error

// OnCallHook is the sync call-hook handler.
type OnCallHook func(ctx context.Context, hook, sessionName string, args map[string]any, tenant string) (*easybus.HookResponse, error)

// OnEventHook is the async event-hook handler.
type OnEventHook func(ctx context.Context, hook, sessionName string, payload any, tenant string) error

// Extension is the extension-side role.
type Extension struct {
	b        bus.Bus
	cfg      Config
	manifest easybus.ExtensionManifest
	// subsMu guards subs. Serve() installs subscriptions from several goroutines
	// (one per channel family) while Close() may run at any time, so the slice is
	// read and appended concurrently. -race caught exactly that on CI:
	//   Read at ... extension.(*Extension).Close()  vs  append in serveX.
	subsMu      sync.Mutex
	subs        []bus.Subscription
	configStore *ConfigStore
	// replicaID identifies this process among the extension's replicas; it
	// suffixes the presence KV key so N replicas keep N independent
	// heartbeats (see startPresence / Close).
	replicaID string

	// in-flight tool calls per session, so an interrupt signal can cancel
	// the handlers of that session (interrupt has real abort semantics).
	inflightMu sync.Mutex
	inflight   map[string]map[string]context.CancelFunc
}

func orDefault(v, def string) string {
	if v == "" {
		return def
	}
	return v
}

// fieldSchemas maps a structured knob's shape onto the wire type.
//
// Recursion is deliberate but SHALLOW in practice: a field may itself be an
// object (one level covers every current knob), and both the console renderer
// and the validator stop at that depth rather than growing an unbounded tree.
func fieldSchemas(in []FieldSpec) []*easybus.ConfigFieldSchema {
	if len(in) == 0 {
		return nil
	}
	out := make([]*easybus.ConfigFieldSchema, 0, len(in))
	for _, f := range in {
		fld := &easybus.ConfigFieldSchema{
			Name: f.Name, Type: f.Type, Required: f.Required,
			EnumValues: f.EnumValues,
		}
		if f.Kind != "" {
			k := f.Kind
			fld.Kind = &k
		}
		if f.Format != "" {
			fm := f.Format
			fld.Format = &fm
		}
		if f.Default != nil {
			fld.Default = easybus.Value(f.Default)
		}
		if f.Description != "" {
			d := f.Description
			fld.Description = &d
		}
		if f.Descriptions != nil {
			fld.Descriptions = f.Descriptions
		}
		fld.Fields = fieldSchemas(f.Fields)
		out = append(out, fld)
	}
	return out
}

func New(b bus.Bus, cfg Config) *Extension {
	e := &Extension{
		b:           b,
		cfg:         cfg,
		manifest:    easybus.ExtensionManifest{Id: cfg.ID, Version: cfg.Version, Features: append([]string(nil), easybusFeatures...)},
		configStore: newConfigStore(),
		inflight:    map[string]map[string]context.CancelFunc{},
		replicaID:   resolveReplicaID(cfg.ReplicaID),
	}

	if len(cfg.Tools) > 0 {
		e.manifest.Capabilities = []string{easybus.Tools}
		for name, spec := range cfg.Tools {
			t := &easybus.ExtensionTool{
				Name:           name,
				Description:    spec.Description,
				Descriptions:   spec.Descriptions,
				InputSchema:    easybus.Struct(spec.InputSchema),
				RequiredConfig: spec.RequiredConfig,
			}
			if spec.Namespace != "" {
				ns := spec.Namespace
				t.Namespace = &ns
			}
			e.manifest.Tools = append(e.manifest.Tools, t)
		}
	}
	if len(cfg.Variables) > 0 {
		if e.manifest.Capabilities == nil {
			e.manifest.Capabilities = []string{easybus.Prompt}
		} else {
			e.manifest.Capabilities = append(e.manifest.Capabilities, easybus.Prompt)
		}
		for name, spec := range cfg.Variables {
			d := spec.Description
			v := &easybus.ExtensionVariable{Name: name, Scope: orDefault(spec.Scope, "global")}
			if d != "" {
				v.Description = &d
			}
			if spec.Descriptions != nil {
				v.Descriptions = spec.Descriptions
			}
			e.manifest.PromptVariables = append(e.manifest.PromptVariables, v)
		}
	}
	if len(cfg.Config) > 0 {
		for name, spec := range cfg.Config {
			item := &easybus.ExtensionConfigItem{
				Name: name,
				Type: spec.Type,
			}
			k := orDefault(spec.Kind, "value")
			item.Kind = &k
			if spec.Capability != "" {
				cap := spec.Capability
				item.Capability = &cap
			}
			if spec.Description != "" {
				d := spec.Description
				item.Description = &d
			}
			if spec.Descriptions != nil {
				item.Descriptions = spec.Descriptions
			}
			item.EnumValues = spec.EnumValues
			if spec.Default != nil {
				item.Default = easybus.Value(spec.Default)
			}
			item.Scope = orDefault(spec.Scope, "global")
			// A structured knob ships its SHAPE, so the console can render a form
			// (a card per list item with a "+", a picker for an enum field)
			// instead of a JSON textarea. Empty Fields keeps the old behaviour: a
			// json blob, untouched.
			item.Fields = fieldSchemas(spec.Fields)
			if spec.ItemType != "" {
				it := spec.ItemType
				item.ItemType = &it
			}
			if spec.MinItems > 0 {
				n := int32(spec.MinItems)
				item.MinItems = &n
			}
			if spec.MaxItems > 0 {
				n := int32(spec.MaxItems)
				item.MaxItems = &n
			}
			if spec.Namespace != "" {
				ns := spec.Namespace
				item.Namespace = &ns
			}
			e.manifest.Config = append(e.manifest.Config, item)
		}
	}
	if len(cfg.Lifecycle) > 0 {
		e.manifest.Lifecycle = append([]string(nil), cfg.Lifecycle...)
	}
	// Publish the declared groupings. Without this the manifest carries per-tool
	// and per-knob `namespace` tags that reference names the client never sees,
	// so it can only sort them alphabetically instead of using the author's order.
	for _, ns := range cfg.Namespaces {
		out := &easybus.ExtensionNamespace{Name: ns.Name, Descriptions: ns.Descriptions}
		if ns.Description != "" {
			d := ns.Description
			out.Description = &d
		}
		e.manifest.Namespaces = append(e.manifest.Namespaces, out)
	}
	if len(cfg.CallHooks) > 0 || len(cfg.EventHooks) > 0 || len(cfg.HookSchemas.Call) > 0 || len(cfg.HookSchemas.Event) > 0 {
		h := &easybus.HookSpec{
			Call:         cfg.CallHooks,
			Event:        cfg.EventHooks,
			CallSchemas:  map[string]*structpb.Struct{},
			EventSchemas: map[string]*structpb.Struct{},
		}
		for k, v := range cfg.HookSchemas.Call {
			h.CallSchemas[k] = easybus.Struct(v)
		}
		for k, v := range cfg.HookSchemas.Event {
			h.EventSchemas[k] = easybus.Struct(v)
		}
		e.manifest.Hooks = h
	}
	return e
}

// Manifest returns the discovery manifest.
func (e *Extension) Manifest() *easybus.ExtensionManifest { return &e.manifest }

// Serve registers discovery/tool/variable/hook channels and blocks until ctx done.
func (e *Extension) Serve(ctx context.Context) error {
	e.startPresence(ctx)
	if err := e.serveDiscovery(ctx); err != nil {
		return err
	}
	if err := e.serveConfig(ctx); err != nil {
		return err
	}
	if err := e.serveTools(ctx); err != nil {
		return err
	}
	if err := e.serveVariables(ctx); err != nil {
		return err
	}
	if err := e.serveCallHooks(ctx); err != nil {
		return err
	}
	if err := e.serveEventHooks(ctx); err != nil {
		return err
	}
	if err := e.serveLifecycle(ctx); err != nil {
		return err
	}
	if err := e.serveInterrupt(ctx); err != nil {
		return err
	}
	return nil
}

func (e *Extension) Close() error {
	// Take and CLEAR the slice under the lock, then close outside it: closing can
	// block, and the appenders only need the lock to mutate the slice.
	e.subsMu.Lock()
	subs := e.subs
	e.subs = nil
	e.subsMu.Unlock()
	for _, s := range subs {
		_ = s.Close()
	}
	// Leave the presence bucket: delete only THIS replica's key, so sibling
	// replicas of the same extId stay visible to liveness watchers.
	_ = e.b.KVDelete(context.Background(), PresenceBucket, e.presenceKey())
	return e.b.Close()
}

// track records a subscription so Close() can release it.
//
// Every serveX goroutine goes through here; appending directly to e.subs raced
// with Close (see the subsMu field comment).
func (e *Extension) track(sub bus.Subscription) {
	if sub == nil {
		return
	}
	e.subsMu.Lock()
	e.subs = append(e.subs, sub)
	e.subsMu.Unlock()
}

// easybusFeatures is the cooperative feature set this SDK build speaks —
// advertised on the discovery manifest so agents can degrade gracefully.
var easybusFeatures = []string{"dlq", "config-kv", "presence", "kv-escaping", "interrupt-abort", "progress", "multitenant"}

// PresenceBucket carries extension liveness. Each replica owns one key,
// `<extId>.<replicaId>`, whose value is the discovery manifest and whose TTL is
// PresenceTTL (refreshed every PresenceInterval). An extension is ALIVE while
// any `<extId>.` key is unexpired; it is offline once they have all expired.
const PresenceBucket = "easybus-presence"
const PresenceTTL = 15 * 1000
const presenceInterval = 5 * time.Second

// ReplicaIDEnv is the explicit override for a replica's identity (highest
// priority; useful for debugging or pinning an identity).
const ReplicaIDEnv = "EASYBUS_REPLICA_ID"

// PodNameEnv is the k8s downward-API pod name, used as the replica id when
// EASYBUS_REPLICA_ID is unset: it is unique, stable across a pod's restarts (so no
// stale key is left behind), and DNS-safe (KV-key safe).
const PodNameEnv = "POD_NAME"

// resolveReplicaID picks this process's replica id. Priority:
//  1. explicit (Config.ReplicaID) / EASYBUS_REPLICA_ID  — caller/debug override
//  2. POD_NAME                                       — k8s pod name (default)
//  3. a random id                                    — non-k8s / local runs
//
// The result is sanitized to the KV-key alphabet so it can never inject a '.'
// separator (which would split the `<extId>.<replicaId>` key).
func resolveReplicaID(explicit string) string {
	id := explicit
	if id == "" {
		id = os.Getenv(ReplicaIDEnv)
	}
	if id == "" {
		id = os.Getenv(PodNameEnv)
	}
	if id == "" {
		id = protocol.NewID()
	}
	return sanitizeReplicaID(id)
}

// sanitizeReplicaID maps an arbitrary id onto [A-Za-z0-9_-], collapsing every
// other byte (notably '.') to '-'. Empty input becomes a fresh random id.
func sanitizeReplicaID(id string) string {
	var b strings.Builder
	for i := 0; i < len(id); i++ {
		c := id[i]
		switch {
		case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9', c == '-', c == '_':
			b.WriteByte(c)
		default:
			b.WriteByte('-')
		}
	}
	if b.Len() == 0 {
		return protocol.NewID()
	}
	return b.String()
}

// presenceKey is this replica's presence KV key: `<extId>.<replicaId>`.
func (e *Extension) presenceKey() string { return e.cfg.ID + "." + e.replicaID }

// PresencePrefix is the KV-key prefix under which every replica of extID
// heartbeats. Liveness is "any unexpired key with this prefix".
func PresencePrefix(extID string) string { return extID + "." }

// PresenceExtID extracts the extId from a presence key (`<extId>.<replicaId>`).
// A legacy un-suffixed key (`<extId>`) is returned verbatim.
func PresenceExtID(key string) string {
	if i := strings.IndexByte(key, '.'); i >= 0 {
		return key[:i]
	}
	return key
}

func (e *Extension) startPresence(ctx context.Context) {
	put := func() {
		raw, err := json.Marshal(&e.manifest)
		if err == nil {
			_ = e.b.KVPut(ctx, PresenceBucket, e.presenceKey(), string(raw), PresenceTTL)
		}
	}
	put()
	go func() {
		t := time.NewTicker(presenceInterval)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				put()
			}
		}
	}()
}

// ReportProgress reports in-flight progress for a tool call.
func (e *Extension) ReportProgress(ctx context.Context, tenant, callID string, progress *easybus.ToolProgress) error {
	progress.CallId = callID
	return e.b.Publish(ctx, protocol.ChToolProgress(tenant, callID), progress, bus.PublishOpts{Tenant: tenant})
}

func (e *Extension) serveDiscovery(ctx context.Context) error {
	// Queue group = extId: N replicas of the same extension elect a single
	// responder for each `easybus.discover` broadcast, so a requester sees exactly
	// one manifest per extension (never N duplicate replies). Mirrors the
	// tool/variable/hook/interrupt subscriptions.
	sub, err := e.b.Subscribe(ctx, protocol.ChDiscover, bus.SubscribeOpts{Queue: e.cfg.ID})
	if err != nil {
		return err
	}
	e.track(sub)
	go func() {
		for {
			env, ok := sub.Next(ctx)
			if !ok {
				return
			}
			if env.ReplyTo != nil {
				_ = e.b.Publish(ctx, *env.ReplyTo, &e.manifest, bus.PublishOpts{Tenant: protocol.GlobalTenant})
			}
		}
	}()
	return nil
}

func (e *Extension) serveTools(ctx context.Context) error {
	for name, spec := range e.cfg.Tools {
		ch := "easybus.*.tool.call." + e.cfg.ID + "." + name
		sub, err := e.b.Subscribe(ctx, ch, bus.SubscribeOpts{Queue: e.cfg.ID})
		if err != nil {
			return err
		}
		e.track(sub)
		go e.pumpTool(ctx, name, spec, sub)
	}
	return nil
}

func (e *Extension) pumpTool(ctx context.Context, name string, spec ToolSpec, sub bus.Subscription) {
	for {
		env, ok := sub.Next(ctx)
		if !ok {
			return
		}
		if env.ReplyTo == nil {
			continue
		}
		// Each call runs on its own goroutine: a slow handler must never
		// head-of-line block other calls to the same tool.
		go e.handleToolCall(ctx, name, spec, env)
	}
}

// handleToolCall executes one tool call and publishes the result envelope.
func (e *Extension) handleToolCall(ctx context.Context, name string, spec ToolSpec, env *easybus.Envelope) {
	replyTo := ""
	if env.ReplyTo != nil {
		replyTo = *env.ReplyTo
	}
	var call easybus.ToolCallEnvelope
	if !protocol.Coerce(env.Payload, &call) || replyTo == "" {
		return
	}
	sessionName := ""
	if env.SessionName != nil {
		sessionName = *env.SessionName
	}
	tenant := env.Tenant
	if tenant == "" {
		tenant = protocol.SubjectTenant(env.Ch)
	}
	callCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	if sessionName != "" {
		e.trackInflight(tenant, sessionName, call.CallId, cancel)
	}
	res := &easybus.ToolResult{CallId: call.CallId, Tool: name}
	data, err := spec.Execute(callCtx, easybus.StructMap(call.Arguments), call.CallId, sessionName, tenant)
	e.untrackInflight(tenant, sessionName, call.CallId)
	if err != nil {
		code := easybus.ToolResultErrorCodeInternal
		var te *TypedError
		if errors.As(err, &te) {
			code = te.Code
		}
		msg := err.Error()
		res.Error = &easybus.ToolError{Code: code, Message: msg}
	} else {
		// Text feeds the model's context; a huge blob would blow it. Return
		// large data as a FILE (data.files + object store), not as content.
		if data.Content != "" && len([]byte(data.Content)) > maxToolContentBytes {
			msg := fmt.Sprintf(
				"tool '%s' returned %d bytes of text, over the %d-byte limit; return large data as a file (data.files) instead",
				name, len([]byte(data.Content)), maxToolContentBytes,
			)
			res.Error = &easybus.ToolError{Code: easybus.ToolResultErrorCodeInvalidArgument, Message: msg}
		} else if data.Content != "" {
			res.Content = &data.Content
		}
		if res.Error == nil && data.Data != nil {
			res.Data = easybus.Value(data.Data)
		}
		if res.Error == nil && data.Object != nil {
			res.Object = &easybus.ObjectRef{ContentType: data.Object.ContentType, Id: data.Object.Id}
		}
	}
	_ = e.b.Publish(context.Background(), replyTo, res, bus.PublishOpts{Tenant: tenant})
}

func (e *Extension) serveVariables(ctx context.Context) error {
	for name, spec := range e.cfg.Variables {
		ch := "easybus.*.var." + e.cfg.ID + "." + name
		sub, err := e.b.Subscribe(ctx, ch, bus.SubscribeOpts{Queue: e.cfg.ID})
		if err != nil {
			return err
		}
		e.track(sub)
		go func(name string, spec VariableSpec, sub bus.Subscription) {
			for {
				env, ok := sub.Next(ctx)
				if !ok {
					return
				}
				if env.ReplyTo == nil || spec.Resolve == nil {
					continue
				}
				tenant := env.Tenant
				if tenant == "" {
					tenant = protocol.SubjectTenant(env.Ch)
				}
				v, err := spec.Resolve(ctx, deref(env.SessionName), tenant)
				if err != nil {
					continue
				}
				_ = e.b.Publish(ctx, *env.ReplyTo, easybus.ExtensionVariableValue{Name: name, Value: v}, bus.PublishOpts{Tenant: tenant})
			}
		}(name, spec, sub)
	}
	return nil
}

func (e *Extension) serveCallHooks(ctx context.Context) error {
	for _, hook := range e.cfg.CallHooks {
		ch := "easybus.*.hook.call." + e.cfg.ID + "." + hook
		sub, err := e.b.Subscribe(ctx, ch, bus.SubscribeOpts{Queue: e.cfg.ID})
		if err != nil {
			return err
		}
		e.track(sub)
		go func(hook string, sub bus.Subscription) {
			for {
				env, ok := sub.Next(ctx)
				if !ok {
					return
				}
				if env.ReplyTo == nil {
					continue
				}
				var call easybus.HookCall
				_ = protocol.Coerce(env.Payload, &call)
				sessionName := call.SessionName
				if sessionName == "" {
					sessionName = deref(env.SessionName)
				}
				tenant := env.Tenant
				if tenant == "" {
					tenant = protocol.SubjectTenant(env.Ch)
				}
				var res *easybus.HookResponse
				args := easybus.StructMap(call.Arguments)
				if msg := validateHookSchema(e.cfg.HookSchemas.Call[hook], args, "arguments"); msg != "" {
					res = &easybus.HookResponse{Ok: false, Error: &easybus.ToolError{Code: easybus.HookResponseErrorCodeInvalidArgument, Message: msg}}
				} else if e.cfg.OnCallHook == nil {
					res = &easybus.HookResponse{Ok: false, Error: &easybus.ToolError{Code: easybus.HookResponseErrorCodeNotFound, Message: "no handler for hook " + hook}}
				} else {
					r, err := e.cfg.OnCallHook(ctx, hook, sessionName, args, tenant)
					if err != nil {
						res = &easybus.HookResponse{Ok: false, Error: &easybus.ToolError{Code: easybus.HookResponseErrorCodeInternal, Message: err.Error()}}
					} else {
						res = r
					}
				}
				_ = e.b.Publish(ctx, *env.ReplyTo, res, bus.PublishOpts{Tenant: tenant})
			}
		}(hook, sub)
	}
	return nil
}

func (e *Extension) serveEventHooks(ctx context.Context) error {
	for _, hook := range e.cfg.EventHooks {
		ch := "easybus.*.hook.event." + hook
		sub, err := e.b.Subscribe(ctx, ch, bus.SubscribeOpts{Queue: e.cfg.ID})
		if err != nil {
			return err
		}
		e.track(sub)
		go func(hook string, sub bus.Subscription) {
			for {
				env, ok := sub.Next(ctx)
				if !ok {
					return
				}
				var ev easybus.HookEvent
				_ = protocol.Coerce(env.Payload, &ev)
				sessionName := ev.SessionName
				if sessionName == "" {
					sessionName = deref(env.SessionName)
				}
				tenant := env.Tenant
				if tenant == "" {
					tenant = protocol.SubjectTenant(env.Ch)
				}
				evPayload := ev.Payload.AsInterface()
				if msg := validateHookSchema(e.cfg.HookSchemas.Event[hook], evPayload, "payload"); msg != "" {
					continue // invalid event payload: drop (best-effort)
				}
				if e.cfg.OnEventHook != nil {
					_ = e.cfg.OnEventHook(ctx, hook, sessionName, evPayload, tenant)
				}
			}
		}(hook, sub)
	}
	return nil
}

// serveLifecycle subscribes the wildcard lifecycle channel and dispatches
// declared kinds to OnLifecycle. On "deleted" the SDK also cleans up
// session-scoped variables (KV) and the agent side cleans config overrides.
func (e *Extension) serveLifecycle(ctx context.Context) error {
	if len(e.cfg.Lifecycle) == 0 || e.cfg.OnLifecycle == nil {
		return nil
	}
	wanted := map[string]bool{}
	for _, k := range e.cfg.Lifecycle {
		wanted[k] = true
	}
	sub, err := e.b.Subscribe(ctx, protocol.LifecycleWildcard+">", bus.SubscribeOpts{Queue: e.cfg.ID})
	if err != nil {
		return err
	}
	e.track(sub)
	go func() {
		for {
			env, ok := sub.Next(ctx)
			if !ok {
				return
			}
			ev := &easybus.LifecycleEvent{}
			if !protocol.Coerce(env.Payload, ev) {
				continue
			}
			tenant := env.Tenant
			if tenant == "" {
				tenant = protocol.SubjectTenant(env.Ch)
			}
			kind := easybus.LifecycleKindString(ev.Kind)
			if !wanted[kind] {
				continue
			}
			if kind == "deleted" {
				_ = e.DeleteSessionVariables(ctx, tenant, ev.SessionName)
			}
			if err := e.cfg.OnLifecycle(ctx, ev, tenant); err != nil {
				// best-effort: lifecycle handlers log via their own logger
				_ = err
			}
		}
	}()
	return nil
}

func (e *Extension) serveInterrupt(ctx context.Context) error {
	sub, err := e.b.Subscribe(ctx, "easybus.*.ctl.interrupt."+e.cfg.ID, bus.SubscribeOpts{Queue: e.cfg.ID})
	if err != nil {
		return err
	}
	e.track(sub)
	go func() {
		for {
			env, ok := sub.Next(ctx)
			if !ok {
				return
			}
			var sig easybus.InterruptSignal
			_ = protocol.Coerce(env.Payload, &sig)
			sessionName := sig.SessionName
			if sessionName == nil {
				sessionName = env.SessionName
			}
			tenant := env.Tenant
			if tenant == "" {
				tenant = protocol.SubjectTenant(env.Ch)
			}
			// Abort in-flight tool calls first (real cancel semantics):
			// a session-scoped signal cancels that tenant+session only; a
			// signal without a session is a tenant broadcast.
			if s := deref(sessionName); s != "" {
				e.cancelInflight(tenant, s)
			} else if tenant != "" {
				e.cancelTenantInflight(tenant)
			} else {
				e.cancelAllInflight()
			}
			if e.cfg.OnInterrupt != nil {
				e.cfg.OnInterrupt(ctx, deref(sessionName), deref(sig.Reason), tenant)
			} else if e.cfg.OnEventHook != nil {
				_ = e.cfg.OnEventHook(ctx, "interrupt", deref(sessionName), sig.Reason, tenant)
			}
		}
	}()
	return nil
}

func deref(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

// inflightKey namespaces a session by tenant so the same session name under
// two tenants never collides.
func inflightKey(tenant, session string) string { return tenant + "\x00" + session }

// trackInflight registers a per-call cancel under its tenant+session.
func (e *Extension) trackInflight(tenant, session, callID string, cancel context.CancelFunc) {
	e.inflightMu.Lock()
	defer e.inflightMu.Unlock()
	key := inflightKey(tenant, session)
	if e.inflight[key] == nil {
		e.inflight[key] = map[string]context.CancelFunc{}
	}
	e.inflight[key][callID] = cancel
}

// untrackInflight drops the registration (call finished on its own).
func (e *Extension) untrackInflight(tenant, session, callID string) {
	if session == "" {
		return
	}
	e.inflightMu.Lock()
	defer e.inflightMu.Unlock()
	key := inflightKey(tenant, session)
	if m := e.inflight[key]; m != nil {
		delete(m, callID)
		if len(m) == 0 {
			delete(e.inflight, key)
		}
	}
}

// cancelInflight aborts every in-flight call of one tenant's session.
func (e *Extension) cancelInflight(tenant, session string) {
	key := inflightKey(tenant, session)
	e.inflightMu.Lock()
	m := e.inflight[key]
	delete(e.inflight, key)
	e.inflightMu.Unlock()
	for _, cancel := range m {
		cancel()
	}
}

// cancelTenantInflight aborts every in-flight call across one tenant.
func (e *Extension) cancelTenantInflight(tenant string) {
	prefix := tenant + "\x00"
	e.inflightMu.Lock()
	var cancels []context.CancelFunc
	for k, m := range e.inflight {
		if strings.HasPrefix(k, prefix) {
			for _, c := range m {
				cancels = append(cancels, c)
			}
			delete(e.inflight, k)
		}
	}
	e.inflightMu.Unlock()
	for _, c := range cancels {
		c()
	}
}

// cancelAllInflight aborts every in-flight call of the extension.
func (e *Extension) cancelAllInflight() {
	e.inflightMu.Lock()
	all := e.inflight
	e.inflight = map[string]map[string]context.CancelFunc{}
	e.inflightMu.Unlock()
	for _, m := range all {
		for _, cancel := range m {
			cancel()
		}
	}
}
