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
	Envelope         = easybusv1.Envelope
	ErrorPayload     = easybusv1.ErrorPayload
	ObjectRef        = easybusv1.ObjectRef
	ToolCallEnvelope = easybusv1.ToolCallEnvelope
	ToolProgress     = easybusv1.ToolProgress
	ToolResult       = easybusv1.ToolResult
	ToolError        = easybusv1.ToolError
	// Grant is one authorisation handed to an extension at call time: a permission
	// key plus params the extension itself declared. The params are opaque to
	// everything but the extension that defined them.
	Grant = easybusv1.Grant
	// PermissionDecl is one permission an extension can require of a caller.
	PermissionDecl         = easybusv1.PermissionDecl
	ExtensionVariable      = easybusv1.ExtensionVariable
	ExtensionVariableValue = easybusv1.ExtensionVariableValue
	ExtensionConfigItem    = easybusv1.ExtensionConfigItem
	// ConfigFieldSchema types one key of a structured config value (or one key of
	// each list item), so a client can render a form instead of a JSON textarea.
	ConfigFieldSchema = easybusv1.ConfigFieldSchema
	// ExtensionNamespace is one declared grouping of an extension's tools and
	// config (pvc, build, storage, ...).
	ExtensionNamespace = easybusv1.ExtensionNamespace
	ConfigSet          = easybusv1.ConfigSet
	ConfigSnapshot     = easybusv1.ConfigSnapshot
	HookCall           = easybusv1.HookCall
	HookEvent          = easybusv1.HookEvent
	HookResponse       = easybusv1.HookResponse
	LifecycleEvent     = easybusv1.LifecycleEvent
	MailboxMessage     = easybusv1.MailboxMessage
	InterruptSignal    = easybusv1.InterruptSignal
	ExtensionTool      = easybusv1.ExtensionTool
	ExtensionManifest  = easybusv1.ExtensionManifest
	HookSpec           = easybusv1.HookSpec
)

// Enum types (aliases to the generated enum types).
type (
	EnvelopeKind       = easybusv1.Envelope_Kind
	LifecycleEventKind = easybusv1.LifecycleEvent_Kind
	ErrorCode          = easybusv1.Code
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

// Scopes, config types and config kinds are open strings (authored in the
// manifest), so these are plain string constants.
const (
	ScopeGlobal  = "global"
	ScopeSession = "session"

	ConfigItemString  = "string"
	ConfigItemNumber  = "number"
	ConfigItemBoolean = "boolean"
	ConfigItemEnum    = "enum"
	ConfigItemJSON    = "json"

	ConfigKindValue = "value"
	ConfigKindModel = "model"
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
	ErrorPayloadCode      = easybusv1.Code
	HookResponseErrorCode = easybusv1.Code
	ToolResultErrorCode   = easybusv1.Code
)

// Legacy enum constants (lower-case wire names map here).
const (
	ConfigSetScopeGlobal  = "global"
	ConfigSetScopeSession = "session"

	ExtensionConfigItemTypeString  = "string"
	ExtensionConfigItemTypeNumber  = "number"
	ExtensionConfigItemTypeBoolean = "boolean"
	ExtensionConfigItemTypeEnum    = "enum"
	ExtensionConfigItemTypeJson    = "json"

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
