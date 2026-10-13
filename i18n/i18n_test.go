package i18n

import "testing"

func TestBaseLang(t *testing.T) {
	cases := map[string]string{
		"zh-CN":   "zh",
		"zh_Hans": "zh",
		"ZH":      "zh",
		"en":      "en",
		"fr":      "fr",
		" zh-TW ": "zh",
	}
	for in, want := range cases {
		if got := BaseLang(in); got != want {
			t.Errorf("BaseLang(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestInterpolate(t *testing.T) {
	if got := Interpolate("Hi {a} {b}", Params{"a": "1"}); got != "Hi 1 {b}" {
		t.Errorf("got %q", got)
	}
	if got := Interpolate("no params", nil); got != "no params" {
		t.Errorf("got %q", got)
	}
	// A brace that is not a valid ident is left untouched.
	if got := Interpolate("{ }", nil); got != "{ }" {
		t.Errorf("got %q", got)
	}
}

var catalog = Catalog{
	"hello": {"en": "Hello {name}", "zh": "你好 {name}", "ja": "こんにちは {name}"},
	"plain": {"en": "plain", "zh": "简单"},
}

func TestTranslate(t *testing.T) {
	if got := Translate(catalog, "hello", "zh", Params{"name": "X"}, ""); got != "你好 X" {
		t.Errorf("zh = %q", got)
	}
	if got := Translate(catalog, "hello", "zh-CN", Params{"name": "X"}, ""); got != "你好 X" {
		t.Errorf("zh-CN = %q", got)
	}
	if got := Translate(catalog, "hello", "ja-JP", Params{"name": "X"}, ""); got != "こんにちは X" {
		t.Errorf("ja-JP = %q", got)
	}
	// Unknown locale falls back to en.
	if got := Translate(catalog, "hello", "de", Params{"name": "X"}, ""); got != "Hello X" {
		t.Errorf("de = %q", got)
	}
	if got := Translate(catalog, "hello", "", Params{"name": "X"}, ""); got != "Hello X" {
		t.Errorf("empty = %q", got)
	}
	// Unknown key returns the key.
	if got := Translate(catalog, "missing", "zh", nil, ""); got != "missing" {
		t.Errorf("missing = %q", got)
	}
}

func TestTranslator(t *testing.T) {
	tr := New(catalog, "")
	if got := tr.T("zh", "plain", nil); got != "简单" {
		t.Errorf("zh plain = %q", got)
	}
	if got := tr.T("en", "plain", nil); got != "plain" {
		t.Errorf("en plain = %q", got)
	}
	if got := tr.T("xx", "plain", nil); got != "plain" {
		t.Errorf("xx plain = %q", got)
	}
}
