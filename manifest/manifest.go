package manifest

import (
	"fmt"
	"os"

	"github.com/easydev-org/easybus-sdk/extension"
	"gopkg.in/yaml.v3"
)

// Manifest is the YAML-declared extension protocol (id/version/tools/
// variables/config/lifecycle/hooks).
type Manifest struct {
	ID        string             `yaml:"id"`
	Version   string             `yaml:"version"`
	Tools     []ManifestTool     `yaml:"tools"`
	Variables []ManifestVariable `yaml:"variables"`
	Config    []ManifestConfig   `yaml:"config"`
	Lifecycle []string           `yaml:"lifecycle"`
	Hooks     *ManifestHooks     `yaml:"hooks"`
	// Namespaces groups this extension's tools and config into named areas
	// (pvc, build, storage, ...).
	//
	// WHY: one extension commonly covers several unrelated jobs, and a flat list
	// of 44 tools with 4 knobs is hard to reason about — and, worse, config was a
	// GLOBAL flat namespace, so two extensions declaring a knob of the same name
	// silently shared one stored value. A namespace is the unit of isolation:
	// config is stored per (extension, namespace), so `ops.build.addr` and
	// `ops.pvc.addr` are different values while `ops`-level knobs stay shared
	// across its namespaces.
	//
	// Declaring a namespace here is OPTIONAL (purely descriptive: a display label
	// and ordering); it becomes meaningful by being referenced from a tool or a
	// config knob via `namespace:`.
	Namespaces []ManifestNamespace `yaml:"namespaces"`
}

// ManifestNamespace describes one grouping of an extension's tools and config.
type ManifestNamespace struct {
	Name         string            `yaml:"name"`
	Description  string            `yaml:"description"`
	Descriptions map[string]string `yaml:"descriptions"`
}

// ManifestTool declares one tool's metadata; handlers are bound by name.
type ManifestTool struct {
	Name         string            `yaml:"name"`
	Description  string            `yaml:"description"`
	Descriptions map[string]string `yaml:"descriptions"`
	// InputSchema is an opaque JSON-Schema blob; any node may carry a
	// `descriptions: map[locale]string` convention next to `description`
	// (see extension.ToolSpec for the resolution contract).
	InputSchema map[string]any `yaml:"input_schema"`
	// RequiredConfig lists the config names this tool requires to run (a tool may
	// share a required config with sibling tools). Absent = no config gating.
	RequiredConfig []string `yaml:"required_config"`
	// Namespace puts this tool in one of the extension's declared namespaces.
	// Purely organisational: the tool's NAME is unchanged (callers keep using
	// `ops-create-pvc`), but a client can group it in the UI.
	Namespace string `yaml:"namespace"`
}

// ManifestVariable declares one prompt template variable.
type ManifestVariable struct {
	Name         string            `yaml:"name"`
	Description  string            `yaml:"description"`
	Descriptions map[string]string `yaml:"descriptions"`
	Scope        string            `yaml:"scope"` // "global" | "session"
}

// ManifestConfig declares one config knob the agent may set at runtime.
type ManifestConfig struct {
	Name string `yaml:"name"`
	// Type selects how the value is entered and validated:
	//
	//	string | number | boolean | enum | json
	//	list   | object        (structured; see Fields / ItemType)
	//
	// `json` remains as the FALLBACK: a json knob with no `fields` is still a raw
	// blob, so existing extensions keep working untouched.
	Type       string `yaml:"type"`
	Kind       string `yaml:"kind"` // "value" (default) | "model"
	Capability string `yaml:"capability"`
	// EnumValues are the allowed choices when Type == "enum".
	EnumValues   []string          `yaml:"enum_values"`
	Default      any               `yaml:"default"`
	Description  string            `yaml:"description"`
	Descriptions map[string]string `yaml:"descriptions"`
	Scope        string            `yaml:"scope"` // "global" | "session"
	// Fields types the SHAPE of a structured knob:
	//
	//	Type == "object"  -> Fields describe that object's keys.
	//	Type == "list"    -> Fields describe each ITEM (ItemType must be "object",
	//	                     or Fields must be empty for a list of scalars).
	//
	// Why this exists: without it a structured knob is one `json` textarea, and
	// every user hand-writes JSON (and a typo is only discovered at run time).
	// With Fields a client renders a real form — one card per list item, a "+" to
	// add, a picker for an enum field — and the SERVER can validate the shape.
	Fields []ManifestField `yaml:"fields"`
	// ItemType is the element type for Type == "list" ("object" | "string" |
	// "number" | "boolean").
	ItemType string `yaml:"item_type"`
	// Required marks a structured knob that must be non-empty.
	Required bool `yaml:"required"`
	// MinItems / MaxItems bound a list (0 = unbounded).
	MinItems int `yaml:"min_items"`
	MaxItems int `yaml:"max_items"`
	// Namespace scopes this knob's STORED VALUE to one of the extension's
	// namespaces. Empty = extension-level, shared by every namespace of that
	// extension (the endpoint/token case).
	Namespace string `yaml:"namespace"`
}

// ManifestField declares one key of a structured config value.
type ManifestField struct {
	Name         string            `yaml:"name"`
	Type         string            `yaml:"type"`   // string | number | boolean | enum | json
	Kind         string            `yaml:"kind"`   // "value" (default) | "secret"
	Format       string            `yaml:"format"` // uri | host | path | email | duration | semver
	EnumValues   []string          `yaml:"enum_values"`
	Default      any               `yaml:"default"`
	Required     bool              `yaml:"required"`
	Description  string            `yaml:"description"`
	Descriptions map[string]string `yaml:"descriptions"`
	// Fields lets a field be an object itself (one level of nesting is enough for
	// every current knob and keeps the renderer honest: recursion is unbounded
	// complexity for no current use).
	Fields []ManifestField `yaml:"fields"`
}

type ManifestHooks struct {
	Call         []string                  `yaml:"call"`
	Event        []string                  `yaml:"event"`
	CallSchemas  map[string]map[string]any `yaml:"call_schemas"`
	EventSchemas map[string]map[string]any `yaml:"event_schemas"`
}

// LoadManifest reads and parses a YAML manifest file.
func LoadManifest(path string) (*Manifest, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	return ParseManifest(data)
}

// ParseManifest parses YAML manifest bytes.
func ParseManifest(data []byte) (*Manifest, error) {
	var m Manifest
	if err := yaml.Unmarshal(data, &m); err != nil {
		return nil, fmt.Errorf("parse manifest: %w", err)
	}
	if m.ID == "" {
		return nil, fmt.Errorf("manifest: id is required")
	}
	if err := m.validateConfig(); err != nil {
		return nil, err
	}
	return &m, nil
}

// validateConfig enforces the config-knob rules that must hold before an
// extension registers.
func (m *Manifest) validateConfig() error {
	for _, c := range m.Config {
		if c.Kind != extension.ConfigKindSecret {
			continue
		}
		// A secret knob is a credential: its value is never returned to a
		// client, and a `default` would ride the (broadcast) manifest — so a
		// secret MUST NOT declare a default.
		//
		// It may be STRUCTURED (a list/object of records that each carry their own
		// `kind: secret` field, e.g. the sandbox list with per-sandbox tokens):
		// withholding the whole knob is then a second, stronger guard on top of
		// the per-field masking.
		switch c.Type {
		case "", "string", "list", "object":
		default:
			return fmt.Errorf("manifest: config %q: kind=secret requires type string, list or object (got %q)", c.Name, c.Type)
		}
		if c.Default != nil && c.Default != "" {
			return fmt.Errorf("manifest: config %q: kind=secret must not declare a default", c.Name)
		}
	}
	return nil
}

// Bindings carries the name-keyed handlers/resolvers used to build a Config.
type Bindings struct {
	// Handlers maps tool name -> executor. Only tools with a bound handler are
	// registered.
	Handlers map[string]extension.ToolSpec
	// Variables maps variable name -> resolver (authoritative fallback).
	Variables   map[string]extension.VariableSpec
	OnCallHook  extension.OnCallHook
	OnEventHook extension.OnEventHook
	// OnConfigChange receives applied config changes; returning an error
	// rejects the change.
	OnConfigChange extension.OnConfigChangeFunc
	// OnLifecycle receives session lifecycle events for the kinds declared
	// in the manifest.
	OnLifecycle extension.OnLifecycleFunc
}

// BuildConfig builds an extension.Config from the manifest + bindings,
// mirroring manifestConfig in easybus-sdk. Only tools bound in Handlers
// are registered; variables use the optional resolver map.
func (m *Manifest) BuildConfig(b Bindings) extension.Config {
	tools := map[string]extension.ToolSpec{}
	for _, t := range m.Tools {
		h, ok := b.Handlers[t.Name]
		if !ok {
			continue
		}
		h.Description = t.Description
		h.Descriptions = t.Descriptions
		h.InputSchema = t.InputSchema
		h.RequiredConfig = t.RequiredConfig
		h.Namespace = t.Namespace
		tools[t.Name] = h
	}

	vars := map[string]extension.VariableSpec{}
	for _, v := range m.Variables {
		spec := extension.VariableSpec{Description: v.Description, Descriptions: v.Descriptions, Scope: v.Scope}
		if spec.Scope == "" {
			spec.Scope = "global"
		}
		if r, ok := b.Variables[v.Name]; ok {
			spec.Resolve = r.Resolve
		}
		vars[v.Name] = spec
	}

	cfg := extension.Config{
		ID:             m.ID,
		Version:        m.Version,
		Tools:          tools,
		Variables:      vars,
		Config:         map[string]extension.ConfigSpec{},
		Namespaces:     toNamespaceSpecs(m.Namespaces),
		Lifecycle:      m.Lifecycle,
		OnCallHook:     b.OnCallHook,
		OnEventHook:    b.OnEventHook,
		OnConfigChange: b.OnConfigChange,
		OnLifecycle:    b.OnLifecycle,
	}
	for _, c := range m.Config {
		cfg.Config[c.Name] = extension.ConfigSpec{
			Description:  c.Description,
			Descriptions: c.Descriptions,
			Type:         c.Type,
			Kind:         c.Kind,
			Capability:   c.Capability,
			EnumValues:   c.EnumValues,
			Default:      c.Default,
			Scope:        c.Scope,
			Fields:       toFieldSpecs(c.Fields),
			ItemType:     c.ItemType,
			MinItems:     c.MinItems,
			MaxItems:     c.MaxItems,
			Namespace:    c.Namespace,
		}
	}
	if m.Hooks != nil {
		cfg.CallHooks = m.Hooks.Call
		cfg.EventHooks = m.Hooks.Event
		if m.Hooks.CallSchemas != nil {
			cfg.HookSchemas.Call = m.Hooks.CallSchemas
		}
		if m.Hooks.EventSchemas != nil {
			cfg.HookSchemas.Event = m.Hooks.EventSchemas
		}
	}
	return cfg
}

// toNamespaceSpecs maps the declared groupings onto the extension config.
func toNamespaceSpecs(in []ManifestNamespace) []extension.NamespaceSpec {
	if len(in) == 0 {
		return nil
	}
	out := make([]extension.NamespaceSpec, 0, len(in))
	for _, n := range in {
		out = append(out, extension.NamespaceSpec{
			Name: n.Name, Description: n.Description, Descriptions: n.Descriptions,
		})
	}
	return out
}

// It recurses one level (a field may itself be an object) because a knob like
// "servers" nests an object per host; deeper nesting is not used anywhere and the
// client renderer stops at this depth on purpose.
func toFieldSpecs(in []ManifestField) []extension.FieldSpec {
	if len(in) == 0 {
		return nil
	}
	out := make([]extension.FieldSpec, 0, len(in))
	for _, f := range in {
		out = append(out, extension.FieldSpec{
			Name:         f.Name,
			Type:         f.Type,
			Kind:         f.Kind,
			Format:       f.Format,
			EnumValues:   f.EnumValues,
			Default:      f.Default,
			Required:     f.Required,
			Description:  f.Description,
			Descriptions: f.Descriptions,
			Fields:       toFieldSpecs(f.Fields),
		})
	}
	return out
}
