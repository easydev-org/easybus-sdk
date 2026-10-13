// Package easybus is the SDK's public surface. The wire types live in
// gen/easybus/v1 (generated from proto/easybus/v1/protocol.proto, the single
// source of truth); this package re-exports them and adds the small helpers
// (enum <-> string, payload conversion, channel/arg helpers) that used to live
// in the hand-generated types.gen.go.
package easybus

import (
	"github.com/easydev-org/easybus-sdk/gen/easybus/v1"
	"github.com/easydev-org/easybus-sdk/protocol"
	"google.golang.org/protobuf/types/known/structpb"
)

// Wire message types (aliases to the generated messages).
type (
	Envelope               = easybusv1.Envelope
	ErrorPayload           = easybusv1.ErrorPayload
	ObjectRef              = easybusv1.ObjectRef
	ToolCallEnvelope       = easybusv1.ToolCallEnvelope
	ToolProgress           = easybusv1.ToolProgress
	ToolResult             = easybusv1.ToolResult
	ToolError              = easybusv1.ToolError
	ExtensionVariable      = easybusv1.ExtensionVariable
	ExtensionVariableValue = easybusv1.ExtensionVariableValue
	ExtensionConfigItem    = easybusv1.ExtensionConfigItem
	ConfigSet              = easybusv1.ConfigSet
	ConfigSnapshot         = easybusv1.ConfigSnapshot
	HookCall               = easybusv1.HookCall
	HookEvent              = easybusv1.HookEvent
	HookResponse           = easybusv1.HookResponse
	LifecycleEvent         = easybusv1.LifecycleEvent
	MailboxMessage         = easybusv1.MailboxMessage
	InterruptSignal        = easybusv1.InterruptSignal
	ExtensionTool          = easybusv1.ExtensionTool
	ExtensionManifest      = easybusv1.ExtensionManifest
	HookSpec               = easybusv1.HookSpec
)

// Enum types (aliases to the generated enum types).
type (
	EnvelopeKind        = easybusv1.Envelope_Kind
	LifecycleEventKind  = easybusv1.LifecycleEvent_Kind
	ErrorCode           = easybusv1.Code
	Scope               = easybusv1.Scope
	ExtensionConfigType = easybusv1.ExtensionConfigItem_Type
	ExtensionConfigKind = easybusv1.ExtensionConfigItem_Kind
)

// Envelope kinds.
const (
	Pub   = easybusv1.Envelope_KIND_PUB
	Queue = easybusv1.Envelope_KIND_QUEUE
	Req   = easybusv1.Envelope_KIND_REQ
)

// Lifecycle kinds.
const (
	LifecycleEventKindCreated = easybusv1.LifecycleEvent_KIND_CREATED
	LifecycleEventKindForked  = easybusv1.LifecycleEvent_KIND_FORKED
	LifecycleEventKindRenamed = easybusv1.LifecycleEvent_KIND_RENAMED
	LifecycleEventKindDeleted = easybusv1.LifecycleEvent_KIND_DELETED
)

// Error codes.
const (
	ErrorCodeInvalidArgument  = easybusv1.Code_CODE_INVALID_ARGUMENT
	ErrorCodeNotFound         = easybusv1.Code_CODE_NOT_FOUND
	ErrorCodePermissionDenied = easybusv1.Code_CODE_PERMISSION_DENIED
	ErrorCodeRetryable        = easybusv1.Code_CODE_RETRYABLE
	ErrorCodeBusiness         = easybusv1.Code_CODE_BUSINESS
	ErrorCodeInternal         = easybusv1.Code_CODE_INTERNAL
)

// Scopes.
const (
	ScopeGlobal  = easybusv1.Scope_SCOPE_GLOBAL
	ScopeSession = easybusv1.Scope_SCOPE_SESSION
)

// Config types / kinds.
const (
	ConfigItemString  = easybusv1.ExtensionConfigItem_TYPE_STRING
	ConfigItemNumber  = easybusv1.ExtensionConfigItem_TYPE_NUMBER
	ConfigItemBoolean = easybusv1.ExtensionConfigItem_TYPE_BOOLEAN
	ConfigItemEnum    = easybusv1.ExtensionConfigItem_TYPE_ENUM
	ConfigItemJSON    = easybusv1.ExtensionConfigItem_TYPE_JSON

	ConfigKindValue = easybusv1.ExtensionConfigItem_KIND_VALUE
	ConfigKindModel = easybusv1.ExtensionConfigItem_KIND_MODEL
)

// Manifest capability names are open strings.
const (
	Prompt = "prompt"
	Tools  = "tools"
)

// ── legacy type names (kept so callers need not chase every enum) ───────────
//
// The old OpenAPI types had one enum type per field (ConfigSetScope,
// HookResponseErrorCode, ...). Proto unifies them (Scope, Code, ...), so these
// aliases map the old names onto the unified enums.
type (
	ConfigSetScope                        = easybusv1.Scope
	ExtensionVariableScope                = easybusv1.Scope
	ExtensionConfigItemScope              = easybusv1.Scope
	ExtensionManifestConfigScope          = easybusv1.Scope
	ExtensionManifestPromptVariablesScope = easybusv1.Scope
	ExtensionConfigItemType               = easybusv1.ExtensionConfigItem_Type
	ExtensionManifestConfigType           = easybusv1.ExtensionConfigItem_Type
	ExtensionConfigItemKind               = easybusv1.ExtensionConfigItem_Kind
	ExtensionManifestConfigKind           = easybusv1.ExtensionConfigItem_Kind
	ErrorPayloadCode                      = easybusv1.Code
	HookResponseErrorCode                 = easybusv1.Code
	ToolResultErrorCode                   = easybusv1.Code
	// Manifest capabilities + lifecycle kinds are open strings in the proto
	// (the manifest schema is intentionally extensible), so a plain string.
	ExtensionManifestCapabilities               = string
	ExtensionManifestLifecycle                  = string
	ExtensionManifestConfigCapability           = string
	ExtensionManifestPromptVariablesScopeString = string
)

// Legacy enum constants (lower-case wire names map here).
const (
	ConfigSetScopeGlobal  = easybusv1.Scope_SCOPE_GLOBAL
	ConfigSetScopeSession = easybusv1.Scope_SCOPE_SESSION

	ExtensionConfigItemTypeString  = easybusv1.ExtensionConfigItem_TYPE_STRING
	ExtensionConfigItemTypeNumber  = easybusv1.ExtensionConfigItem_TYPE_NUMBER
	ExtensionConfigItemTypeBoolean = easybusv1.ExtensionConfigItem_TYPE_BOOLEAN
	ExtensionConfigItemTypeEnum    = easybusv1.ExtensionConfigItem_TYPE_ENUM
	ExtensionConfigItemTypeJson    = easybusv1.ExtensionConfigItem_TYPE_JSON

	HookResponseErrorCodeInvalidArgument  = easybusv1.Code_CODE_INVALID_ARGUMENT
	HookResponseErrorCodeNotFound         = easybusv1.Code_CODE_NOT_FOUND
	HookResponseErrorCodePermissionDenied = easybusv1.Code_CODE_PERMISSION_DENIED
	HookResponseErrorCodeRetryable        = easybusv1.Code_CODE_RETRYABLE
	HookResponseErrorCodeBusiness         = easybusv1.Code_CODE_BUSINESS
	HookResponseErrorCodeInternal         = easybusv1.Code_CODE_INTERNAL

	ToolResultErrorCodeInvalidArgument  = easybusv1.Code_CODE_INVALID_ARGUMENT
	ToolResultErrorCodeNotFound         = easybusv1.Code_CODE_NOT_FOUND
	ToolResultErrorCodePermissionDenied = easybusv1.Code_CODE_PERMISSION_DENIED
	ToolResultErrorCodeRetryable        = easybusv1.Code_CODE_RETRYABLE
	ToolResultErrorCodeBusiness         = easybusv1.Code_CODE_BUSINESS
	ToolResultErrorCodeInternal         = easybusv1.Code_CODE_INTERNAL

	ErrorPayloadCodeInvalidArgument  = easybusv1.Code_CODE_INVALID_ARGUMENT
	ErrorPayloadCodeNotFound         = easybusv1.Code_CODE_NOT_FOUND
	ErrorPayloadCodePermissionDenied = easybusv1.Code_CODE_PERMISSION_DENIED
	ErrorPayloadCodeRetryable        = easybusv1.Code_CODE_RETRYABLE
	ErrorPayloadCodeBusiness         = easybusv1.Code_CODE_BUSINESS
	ErrorPayloadCodeInternal         = easybusv1.Code_CODE_INTERNAL
)

// ── enum <-> string (the wire uses the lower-case names) ─────────────────────

var lifecycleNames = map[easybusv1.LifecycleEvent_Kind]string{
	easybusv1.LifecycleEvent_KIND_CREATED: "created",
	easybusv1.LifecycleEvent_KIND_FORKED:  "forked",
	easybusv1.LifecycleEvent_KIND_RENAMED: "renamed",
	easybusv1.LifecycleEvent_KIND_DELETED: "deleted",
}

var lifecycleKinds = map[string]easybusv1.LifecycleEvent_Kind{
	"created": easybusv1.LifecycleEvent_KIND_CREATED,
	"forked":  easybusv1.LifecycleEvent_KIND_FORKED,
	"renamed": easybusv1.LifecycleEvent_KIND_RENAMED,
	"deleted": easybusv1.LifecycleEvent_KIND_DELETED,
}

// LifecycleKindString renders a lifecycle kind as its wire name ("" when unknown).
func LifecycleKindString(k easybusv1.LifecycleEvent_Kind) string { return lifecycleNames[k] }

// LifecycleKindFromString parses a lifecycle kind from its wire name.
func LifecycleKindFromString(s string) easybusv1.LifecycleEvent_Kind { return lifecycleKinds[s] }

// EnvelopeKindString renders an envelope kind as its wire name (req/pub/queue).
func EnvelopeKindString(k easybusv1.Envelope_Kind) string {
	switch k {
	case easybusv1.Envelope_KIND_REQ:
		return "req"
	case easybusv1.Envelope_KIND_PUB:
		return "pub"
	case easybusv1.Envelope_KIND_QUEUE:
		return "queue"
	}
	return ""
}

// EnvelopeKindFromString parses an envelope kind from its wire name.
func EnvelopeKindFromString(s string) easybusv1.Envelope_Kind {
	switch s {
	case "req":
		return easybusv1.Envelope_KIND_REQ
	case "pub":
		return easybusv1.Envelope_KIND_PUB
	case "queue":
		return easybusv1.Envelope_KIND_QUEUE
	}
	return easybusv1.Envelope_KIND_UNSPECIFIED
}

// ConfigTypeFromString maps the manifest's config type string to the enum.
func ConfigTypeFromString(s string) easybusv1.ExtensionConfigItem_Type {
	switch s {
	case "string":
		return easybusv1.ExtensionConfigItem_TYPE_STRING
	case "number":
		return easybusv1.ExtensionConfigItem_TYPE_NUMBER
	case "boolean":
		return easybusv1.ExtensionConfigItem_TYPE_BOOLEAN
	case "enum":
		return easybusv1.ExtensionConfigItem_TYPE_ENUM
	case "json":
		return easybusv1.ExtensionConfigItem_TYPE_JSON
	}
	return easybusv1.ExtensionConfigItem_TYPE_UNSPECIFIED
}

// ConfigTypeString renders a config type as its manifest string.
func ConfigTypeString(t easybusv1.ExtensionConfigItem_Type) string {
	switch t {
	case easybusv1.ExtensionConfigItem_TYPE_STRING:
		return "string"
	case easybusv1.ExtensionConfigItem_TYPE_NUMBER:
		return "number"
	case easybusv1.ExtensionConfigItem_TYPE_BOOLEAN:
		return "boolean"
	case easybusv1.ExtensionConfigItem_TYPE_ENUM:
		return "enum"
	case easybusv1.ExtensionConfigItem_TYPE_JSON:
		return "json"
	}
	return ""
}

// ConfigKindString renders a config kind as its manifest string.
func ConfigKindString(k easybusv1.ExtensionConfigItem_Kind) string {
	if k == easybusv1.ExtensionConfigItem_KIND_MODEL {
		return "model"
	}
	return "value"
}

// ConfigKindFromString maps the manifest's config kind string to the enum.
func ConfigKindFromString(s string) easybusv1.ExtensionConfigItem_Kind {
	switch s {
	case "model":
		return easybusv1.ExtensionConfigItem_KIND_MODEL
	default:
		return easybusv1.ExtensionConfigItem_KIND_VALUE
	}
}

// ScopeFromString maps "global"/"session" to the scope enum.
func ScopeFromString(s string) easybusv1.Scope {
	if s == "session" {
		return easybusv1.Scope_SCOPE_SESSION
	}
	return easybusv1.Scope_SCOPE_GLOBAL
}

// ── payload helpers (opaque JSON bodies are structpb.Value on the wire) ──────

// Value converts an arbitrary Go value into a structpb.Value (the wire shape of
// every opaque payload body). Errors are folded to NullValue.
func Value(v any) *structpb.Value {
	if v == nil {
		return structpb.NewNullValue()
	}
	out, err := structpb.NewValue(v)
	if err != nil {
		return structpb.NewNullValue()
	}
	return out
}

// ValueInterface unwraps a structpb.Value back to an arbitrary Go value.
func ValueInterface(v *structpb.Value) any {
	if v == nil {
		return nil
	}
	return v.AsInterface()
}

// Struct converts a map to a structpb.Struct (tool/hook arguments).
func Struct(m map[string]any) *structpb.Struct {
	if m == nil {
		return nil
	}
	out, err := structpb.NewStruct(m)
	if err != nil {
		return nil
	}
	return out
}

// StructMap unwraps a structpb.Struct to a plain map (nil-safe).
func StructMap(s *structpb.Struct) map[string]any {
	if s == nil {
		return map[string]any{}
	}
	return s.AsMap()
}

// ArgString/ArgInt/... read tool args (kept for callers that hold a map).
func ArgString(args map[string]any, key string) string { return protocol.ArgString(args, key) }
