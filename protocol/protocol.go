package protocol

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"regexp"
	"strings"
)

// SessionToken derives the transport-safe token for a session name.
func SessionToken(sessionName string) string {
	sum := sha256.Sum256([]byte(sessionName))
	return base64.RawURLEncoding.EncodeToString(sum[:])[:22]
}

// ---- tenant namespacing (v2) --------------------------------------------
//
// A tenant is an opaque, plaintext isolation key (typically a user id). It is
// the SECOND subject segment for every data-plane channel — easybus.<tenant>.<...>
// — and rides every envelope in its Tenant field. Plaintext keeps subjects and
// KV keys readable for operators; the cost is a strict charset so a tenant can
// never inject a subject separator ('.'), wildcard ('*'/'>'), or whitespace.

var tenantRe = regexp.MustCompile(`^[A-Za-z0-9_-]{1,64}$`)

// GlobalTenant is the reserved tenant for GLOBAL control-plane messages
// (discovery), whose subject carries no tenant segment.
const GlobalTenant = "global"

// IsValidTenant reports whether raw is a legal tenant id.
func IsValidTenant(raw string) bool { return tenantRe.MatchString(raw) }

// ValidateTenant returns raw when it is legal, or "" when it is not. Callers
// on an error path should reject the message.
func ValidateTenant(raw string) (string, bool) {
	if !tenantRe.MatchString(raw) {
		return "", false
	}
	return raw, true
}

// TenantPrefix is the data-plane subject prefix for a tenant: easybus.<tenant>.
func TenantPrefix(tenant string) string { return "easybus." + tenant + "." }

// SubjectTenant extracts the tenant segment from an easybus.<tenant>.<...>
// subject, or "" when the subject is not tenant-namespaced (e.g. the global
// easybus.discover).
func SubjectTenant(ch string) string {
	if !strings.HasPrefix(ch, "easybus.") {
		return ""
	}
	rest := ch[len("easybus."):]
	dot := strings.IndexByte(rest, '.')
	if dot <= 0 {
		return ""
	}
	tenant := rest[:dot]
	if !tenantRe.MatchString(tenant) {
		return ""
	}
	return tenant
}

// TenantKVKey is a tenant-scoped KV key in a shared bucket: t.<tenant>.<rest>.
func TenantKVKey(tenant, rest string) string {
	return "t." + tenant + "." + rest
}

// TenantObjectName is a tenant-scoped object-store name: t.<tenant>.<name>.
func TenantObjectName(tenant, name string) string {
	return "t." + tenant + "." + name
}

const (
	ChDiscover = "easybus.discover"
	// MailboxWildcard is the per-tenant mailbox prefix (append the token).
	// MailboxWildcardAll spans every tenant (cross-tenant consumers).
	MailboxWildcard    = "easybus."
	MailboxWildcardAll = "easybus.*.mailbox."
	// LifecycleWildcard spans every tenant's lifecycle subject.
	LifecycleWildcard = "easybus.*.session.lifecycle."
	// VarsBucket stores extension variables, keyed tenant-first:
	// t.<tenant>.<extId>.<name> (global) and
	// t.<tenant>.<extId>.<sessionToken>.<name> (session).
	VarsBucket = "vars"
)

func ChToolCall(tenant, extID, tool string) string {
	return TenantPrefix(tenant) + "tool.call." + extID + "." + tool
}
func ChToolProgress(tenant, callID string) string {
	return TenantPrefix(tenant) + "tool.progress." + callID
}
func ChVariable(tenant, extID, name string) string {
	return TenantPrefix(tenant) + "var." + extID + "." + name
}
func ChMailbox(tenant, session string) string {
	return TenantPrefix(tenant) + "mailbox." + SessionToken(session)
}
func ChSessionEvents(tenant, session string) string {
	return TenantPrefix(tenant) + "session.events." + SessionToken(session)
}
func ChSessionChanged(tenant string) string {
	return TenantPrefix(tenant) + "session.changed"
}
func ChInterrupt(tenant, extID string) string {
	return TenantPrefix(tenant) + "ctl.interrupt." + extID
}
func ChInterruptAll(tenant string) string {
	return TenantPrefix(tenant) + "ctl.interrupt.>"
}
func ChHookCall(tenant, extID, hook string) string {
	return TenantPrefix(tenant) + "hook.call." + extID + "." + hook
}
func ChHookEvent(tenant, hook string) string {
	return TenantPrefix(tenant) + "hook.event." + hook
}

// ChLifecycle routes one tenant's lifecycle kind (created/forked/renamed/deleted).
func ChLifecycle(tenant, kind string) string {
	return TenantPrefix(tenant) + "session.lifecycle." + kind
}

// VarKey is the global variable KV key (t.<tenant>.<extId>.<name>).
func VarKey(tenant, extID, name string) string {
	return TenantKVKey(tenant, extID+"."+name)
}

// SessionVarKey is the session variable KV key
// (t.<tenant>.<extId>.<token>.<name>).
func SessionVarKey(tenant, extID, sessionName, name string) string {
	return TenantKVKey(tenant, extID+"."+SessionToken(sessionName)+"."+name)
}

// ArgString reads a string tool argument (missing/typed wrong = "").
func ArgString(args map[string]any, key string) string {
	if v, ok := args[key].(string); ok {
		return v
	}
	return ""
}

// ArgInt reads a numeric tool argument with a default (JSON numbers decode
// as float64; integral values only).
func ArgInt(args map[string]any, key string, def int64) int64 {
	if v, ok := args[key].(float64); ok {
		return int64(v)
	}
	return def
}

// ArgFloat reads a float tool argument with a default.
func ArgFloat(args map[string]any, key string, def float64) float64 {
	if v, ok := args[key].(float64); ok {
		return v
	}
	return def
}

// ArgBool reads a boolean tool argument with a default.
func ArgBool(args map[string]any, key string, def bool) bool {
	if v, ok := args[key].(bool); ok {
		return v
	}
	return def
}
func ChConfig(tenant, extID string) string {
	return TenantPrefix(tenant) + "config." + extID
}

// ChConfigGet is DEPRECATED (0.2): config snapshots moved to the cfg KV
// bucket; extensions recover via KV watch. Kept for external callers.
func ChConfigGet(tenant, extID string) string {
	return TenantPrefix(tenant) + "config.get." + extID
}
func ChConfigWildcard(tenant string) string {
	return TenantPrefix(tenant) + "config.get.>"
}

// ConfigExtIDFromGetChannel extracts the extId from easybus.<tenant>.config.get.<extId>.
func ConfigExtIDFromGetChannel(ch string) string {
	const p = ".config.get."
	if i := strings.LastIndex(ch, p); i >= 0 {
		return ch[i+len(p):]
	}
	return ch
}

// ConfigKVBucket mirrors applied config values for persistence (caps.kv).
const ConfigKVBucket = "cfg"

// NewID returns a fresh random UUID-like id.
func NewID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "id"
	}
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return hex.EncodeToString(b[0:4]) + "-" + hex.EncodeToString(b[4:6]) + "-" +
		hex.EncodeToString(b[6:8]) + "-" + hex.EncodeToString(b[8:10]) + "-" +
		hex.EncodeToString(b[10:16])
}

// Coerce round-trips an arbitrary decoded value into a typed shape via JSON.
// Coerce converts a decoded payload into a typed value. When src is raw
// JSON bytes (the transport decode path keeps payloads as json.RawMessage),
// it unmarshals directly — skipping a full Marshal round trip.
func Coerce(src any, dst any) bool {
	switch v := src.(type) {
	case json.RawMessage:
		return json.Unmarshal(v, dst) == nil
	case []byte:
		return json.Unmarshal(v, dst) == nil
	default:
		b, err := json.Marshal(src)
		if err != nil {
			return false
		}
		return json.Unmarshal(b, dst) == nil
	}
}

// Ptr returns a pointer to v. Useful for optional (pointer) fields in
// generated types: Envelope{V: protocol.Ptr(1)}.
func Ptr[T any](v T) *T { return &v }

// EscapeKVSegment encodes a string into a NATS KV-safe key segment using
// only [a-zA-Z0-9-/_=] (NATS KV key alphabet — '%' and ':' are invalid).
// The escape char '=' introduces a two-hex-digit byte ('.' -> "=2E",
// '=' -> "=3D"); every other byte outside the safe set is hex-encoded.
// Round-trips via UnescapeKVSegment and never produces the '.' separator.
func EscapeKVSegment(s string) string {
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9', c == '-' || c == '_' || c == '/':
			b.WriteByte(c)
		default:
			b.WriteByte('=')
			const hex = "0123456789ABCDEF"
			b.WriteByte(hex[c>>4])
			b.WriteByte(hex[c&0xf])
		}
	}
	return b.String()
}

// UnescapeKVSegment reverses EscapeKVSegment.
func UnescapeKVSegment(s string) string {
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		if s[i] == '=' && i+2 < len(s) {
			if hi, ok1 := hexVal(s[i+1]); ok1 {
				if lo, ok2 := hexVal(s[i+2]); ok2 {
					b.WriteByte(hi<<4 | lo)
					i += 2
					continue
				}
			}
		}
		b.WriteByte(s[i])
	}
	return b.String()
}

func hexVal(c byte) (byte, bool) {
	switch {
	case c >= '0' && c <= '9':
		return c - '0', true
	case c >= 'A' && c <= 'F':
		return c - 'A' + 10, true
	case c >= 'a' && c <= 'f':
		return c - 'a' + 10, true
	}
	return 0, false
}
