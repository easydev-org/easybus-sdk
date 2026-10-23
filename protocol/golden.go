package protocol

// EvalSubject evaluates one protocol-subjects case against this implementation.
//
// Lives in the package (not a test file) because two callers need it: the generator that writes
// conformance-cases/protocol-subjects.json, and the test that checks this SDK still reproduces
// it. Keeping one implementation means the table cannot be generated from one code path and
// verified against another.
//
// The function name in each case maps to a protocol function here. A case naming a function this
// file does not know returns "UNKNOWN_FN:<name>" so the test fails loudly rather than passing on
// a zero string.
func EvalSubject(fn string, args map[string]any) any {
	s := func(k string) string { v, _ := args[k].(string); return v }

	switch fn {
	case "chMailbox":
		return ChMailbox(s("tenant"), s("session"))
	case "chToolCall":
		return ChToolCall(s("tenant"), s("ext"), s("tool"))
	case "chToolProgress":
		return ChToolProgress(s("tenant"), s("callId"))
	case "chVariable":
		return ChVariable(s("tenant"), s("ext"), s("name"))
	case "chSessionEvents":
		return ChSessionEvents(s("tenant"), s("session"))
	case "chSessionChanged":
		return ChSessionChanged(s("tenant"))
	case "chInterrupt":
		return ChInterrupt(s("tenant"), s("ext"))
	case "chInterruptAll":
		return ChInterruptAll(s("tenant"))
	case "chLifecycle":
		// Addressed by KIND: each lifecycle kind is its own subject, so a subscriber picks
		// the ones it cares about instead of filtering in code.
		return ChLifecycle(s("tenant"), s("kind"))
	case "chHookCall":
		return ChHookCall(s("tenant"), s("ext"), s("hook"))
	case "chHookEvent":
		// Not per extension: an event hook is addressed by the hook name alone, unlike a call
		// hook which is ext+hook. Asymmetric on purpose and easy to get wrong.
		return ChHookEvent(s("tenant"), s("hook"))
	case "chConfig":
		return ChConfig(s("tenant"), s("ext"))
	case "chConfigGet":
		return ChConfigGet(s("tenant"), s("ext"))
	case "chChannelTenant":
		return ChChannelTenant(s("tenant"))
	case "chChannel":
		got, err := ChChannel(s("tenant"), s("channel"), s("topic"))
		if err != nil {
			return "REJECTED"
		}
		return got
	case "chFileGet":
		return ChFileGet(s("tenant"))
	case "chFileIngest":
		return ChFileIngest(s("tenant"))
	case "subjectTenant":
		return SubjectTenant(s("subject"))
	case "sessionToken":
		return SessionToken(s("session"))
	case "escapeKVSegment":
		return EscapeKVSegment(s("segment"))
	case "unescapeKVSegment":
		return UnescapeKVSegment(s("segment"))
	case "tenantKVKey":
		return TenantKVKey(s("tenant"), s("name"))
	case "sessionVarKey":
		return SessionVarKey(s("tenant"), s("session"), s("ext"), s("name"))
	case "subscriptionKVKey":
		return SubscriptionKVKey(s("tenant"), s("session"), s("id"))
	}
	return "UNKNOWN_FN:" + fn
}
