// Package i18n is the Go twin of the TypeScript `easybus-sdk` i18n core:
// typed, extensible-locale localisation for extension-authored strings that
// reach the model context.
//
// Tool DESCRIPTIONS are localized through the manifest (`description` +
// `descriptions[locale]`). This package covers the OTHER half: the runtime
// `content` and error text a tool returns, which is fed verbatim to the model.
// A Chinese session should not get English tool results.
//
// Design (mirrors the TS core so the two SDKs behave identically — see the
// cross-SDK conformance vectors):
//
//   - A Catalog is `{ key: { locale: template } }`. The locale set is an OPEN
//     map (any BCP-47-ish tag), so adding a language is pure data.
//   - Keys are declared as named constants (`MsgKey`) per extension, so a
//     mistyped key is a compile error at the call site.
//   - Templates interpolate `{name}` placeholders.
//   - Resolution is total: requested locale -> its base language -> fallback
//     locale (default `en`) -> its base -> the key itself. A message is never
//     empty, even for an untranslated locale.
package i18n

import "strings"

// BaseLocale is the locale every catalog MUST provide (fallback termination).
const BaseLocale = "en"

// Params holds `{placeholder}` interpolation values.
type Params map[string]string

// Catalog maps a message key to its per-locale templates. `locale` tags are
// open-ended (e.g. "en", "zh", "zh-CN", "ja").
type Catalog map[string]map[string]string

// BaseLang lowercases and strips the region/script subtag: `zh-CN`, `zh_Hans`
// and `ZH` all collapse to `zh`.
func BaseLang(locale string) string {
	l := strings.ToLower(strings.TrimSpace(locale))
	l = strings.ReplaceAll(l, "_", "-")
	if i := strings.IndexByte(l, '-'); i >= 0 {
		l = l[:i]
	}
	return l
}

// Interpolate replaces `{name}` placeholders; an unknown name is left literal.
func Interpolate(template string, params Params) string {
	if len(params) == 0 {
		return template
	}
	var b strings.Builder
	b.Grow(len(template))
	for i := 0; i < len(template); {
		c := template[i]
		if c != '{' {
			b.WriteByte(c)
			i++
			continue
		}
		end := strings.IndexByte(template[i:], '}')
		if end < 0 {
			b.WriteString(template[i:])
			break
		}
		name := template[i+1 : i+end]
		if !isIdent(name) {
			b.WriteByte(c)
			i++
			continue
		}
		if v, ok := params[name]; ok {
			b.WriteString(v)
		} else {
			b.WriteString("{")
			b.WriteString(name)
			b.WriteString("}")
		}
		i += end + 1
	}
	return b.String()
}

// isIdent reports whether s is a valid placeholder name ([A-Za-z0-9_]+,
// non-empty), matching the TS `\{([a-zA-Z0-9_]+)\}` rule.
func isIdent(s string) bool {
	if s == "" {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		ok := c == '_' ||
			(c >= 'a' && c <= 'z') ||
			(c >= 'A' && c <= 'Z') ||
			(c >= '0' && c <= '9')
		if !ok {
			return false
		}
	}
	return true
}

// Translate resolves one message. `locale` may be a full tag; matching walks
// exact -> base(locale) -> fallback -> base(fallback) -> first-available ->
// the key itself.
func Translate(c Catalog, key, locale string, params Params, fallback string) string {
	entry := c[key]
	if entry == nil {
		return key
	}
	if fallback == "" {
		fallback = BaseLocale
	}
	for _, cand := range []string{locale, BaseLang(locale), fallback, BaseLang(fallback)} {
		if tpl, ok := entry[cand]; ok {
			return Interpolate(tpl, params)
		}
	}
	// Last resort: any available translation, else the key. Iterate the known
	// common locales first for a deterministic pick, then any other.
	for _, cand := range []string{BaseLocale, "zh"} {
		if tpl, ok := entry[cand]; ok {
			return Interpolate(tpl, params)
		}
	}
	for _, tpl := range entry {
		return Interpolate(tpl, params)
	}
	return key
}

// Translator is a Catalog bound to a fallback locale.
type Translator struct {
	Catalog  Catalog
	Fallback string
}

// New binds a catalog with a fallback locale (defaults to BaseLocale when the
// fallback is empty).
func New(c Catalog, fallback string) *Translator {
	if fallback == "" {
		fallback = BaseLocale
	}
	return &Translator{Catalog: c, Fallback: fallback}
}

// T resolves `key` for `locale`.
func (t *Translator) T(locale, key string, params Params) string {
	return Translate(t.Catalog, key, locale, params, t.Fallback)
}
