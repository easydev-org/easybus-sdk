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
	Name         string            `yaml:"name"`
	Type         string            `yaml:"type"` // string | number | boolean | enum | json
	Kind         string            `yaml:"kind"` // "value" (default) | "model"
	Capability   string            `yaml:"capability"`
	EnumValues   []string          `yaml:"enum_values"`
	Default      any               `yaml:"default"`
	Description  string            `yaml:"description"`
	Descriptions map[string]string `yaml:"descriptions"`
	Scope        string            `yaml:"scope"` // "global" | "session"
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
	return &m, nil
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
