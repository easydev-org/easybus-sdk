package conformance

import (
	"testing"

	"github.com/easydev-org/easybus-sdk/i18n"
	"github.com/easydev-org/easybus-sdk/protocol"
)

// goldenVectors mirrors tests/golden.ts in easybus-sdk. If either side
// changes a derivation, the other must be updated in lockstep. v2: every
// data-plane channel is easybus.<tenant>.<...>.
const goldenTenant = "alice"

var goldenVectors = map[string]struct{ got, want string }{
	"session_token":     {protocol.SessionToken("sess-1"), "q-Yz86R6J1gXTqvpFg2vNs"},
	"discover":          {protocol.ChDiscover, "easybus.discover"},
	"tool_call":         {protocol.ChToolCall(goldenTenant, "ops", "echo"), "easybus.alice.tool.call.ops.echo"},
	"tool_progress":     {protocol.ChToolProgress(goldenTenant, "c1"), "easybus.alice.tool.progress.c1"},
	"variable":          {protocol.ChVariable(goldenTenant, "ops", "base-url"), "easybus.alice.var.ops.base-url"},
	"interrupt":         {protocol.ChInterrupt(goldenTenant, "ops"), "easybus.alice.ctl.interrupt.ops"},
	"hook_call":         {protocol.ChHookCall(goldenTenant, "ops", "session.before_create"), "easybus.alice.hook.call.ops.session.before_create"},
	"hook_event":        {protocol.ChHookEvent(goldenTenant, "session.created"), "easybus.alice.hook.event.session.created"},
	"mailbox_wildcard":  {protocol.MailboxWildcardAll + ">", "easybus.*.mailbox.>"},
	"mailbox_sess1":     {protocol.ChMailbox(goldenTenant, "sess-1"), "easybus.alice.mailbox.q-Yz86R6J1gXTqvpFg2vNs"},
	"session_events_s1": {protocol.ChSessionEvents(goldenTenant, "sess-1"), "easybus.alice.session.events.q-Yz86R6J1gXTqvpFg2vNs"},
	"config":            {protocol.ChConfig(goldenTenant, "ops"), "easybus.alice.config.ops"},
	"config_get":        {protocol.ChConfigGet(goldenTenant, "ops"), "easybus.alice.config.get.ops"},
	"var_key":           {protocol.VarKey(goldenTenant, "ops", "base-url"), "t.alice.ops.base-url"},
	"session_var_key":   {protocol.SessionVarKey(goldenTenant, "ops", "sess-1", "ws"), "t.alice.ops.q-Yz86R6J1gXTqvpFg2vNs.ws"},
}

func TestGoldenVectors(t *testing.T) {
	for name, v := range goldenVectors {
		if v.got != v.want {
			t.Errorf("%s = %q, want %q (TS/Go derivations drifted)", name, v.got, v.want)
		}
	}
}

// goldenI18n catalog mirrors tests/golden.ts (i18n section) in easybus-sdk
// so the TS and Go i18n cores resolve identically.
var goldenI18n = i18n.Catalog{
	"hello": {"en": "Hello {name}", "zh": "你好 {name}", "ja": "こんにちは {name}"},
	"plain": {"en": "plain", "zh": "简单"},
}

func TestGoldenI18n(t *testing.T) {
	cases := []struct {
		key, locale string
		params      i18n.Params
		want        string
	}{
		{"hello", "zh", i18n.Params{"name": "X"}, "你好 X"},
		{"hello", "zh-CN", i18n.Params{"name": "X"}, "你好 X"},
		{"hello", "zh_Hans", i18n.Params{"name": "X"}, "你好 X"},
		{"hello", "ja-JP", i18n.Params{"name": "X"}, "こんにちは X"},
		{"hello", "de", i18n.Params{"name": "X"}, "Hello X"},
		{"hello", "", i18n.Params{"name": "X"}, "Hello X"},
		{"plain", "zh", nil, "简单"},
		{"plain", "unknown", nil, "plain"},
	}
	for _, c := range cases {
		if got := i18n.Translate(goldenI18n, c.key, c.locale, c.params, ""); got != c.want {
			t.Errorf("Translate(%q,%q) = %q, want %q (TS/Go i18n drifted)", c.key, c.locale, got, c.want)
		}
	}
	if got := i18n.BaseLang("zh-Hans"); got != "zh" {
		t.Errorf("BaseLang(zh-Hans) = %q, want zh", got)
	}
	if got := i18n.Interpolate("Hi {a} {b}", i18n.Params{"a": "1"}); got != "Hi 1 {b}" {
		t.Errorf("Interpolate = %q", got)
	}
}
