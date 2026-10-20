package manifest

import (
	"testing"

	"github.com/easydev-org/easybus-sdk/extension"
	"gopkg.in/yaml.v3"
)

// TestNamespaceOnToolAndConfig: one extension may cover several unrelated jobs,
// so tools and config knobs can be grouped — and a knob's stored value is scoped
// to (extension, namespace) instead of one GLOBAL flat namespace where two
// extensions declaring `sandboxes` silently shared a single value.
func TestNamespaceOnToolAndConfig(t *testing.T) {
	src := `
id: ops
version: "1"
namespaces:
  - name: pvc
    description: Persistent volume claims
  - name: build
    description: Image builds
config:
  - name: easyops-url
    type: string
  - name: buildkit-addr
    type: string
    namespace: build
tools:
  - name: create-pvc
    description: Create a PVC
    namespace: pvc
  - name: build-image
    description: Build an image
    namespace: build
`
	var m Manifest
	if err := yaml.Unmarshal([]byte(src), &m); err != nil {
		t.Fatal(err)
	}
	if len(m.Namespaces) != 2 {
		t.Fatalf("namespaces = %d, want 2", len(m.Namespaces))
	}
	if m.Namespaces[0].Name != "pvc" || m.Namespaces[1].Name != "build" {
		t.Errorf("namespace order = %v", m.Namespaces)
	}

	// A tool carries its namespace, and its NAME is untouched (callers keep
	// using `ops-create-pvc`; grouping is organisational).
	byName := map[string]ManifestTool{}
	for _, tl := range m.Tools {
		byName[tl.Name] = tl
	}
	if byName["create-pvc"].Namespace != "pvc" {
		t.Errorf("create-pvc namespace = %q, want pvc", byName["create-pvc"].Namespace)
	}
	if byName["create-pvc"].Name != "create-pvc" {
		t.Error("the tool name must not be rewritten by a namespace")
	}

	// A knob's namespace is what scopes its value; empty means extension-level
	// (shared by every namespace of that extension — the endpoint/token case).
	cfgNS := map[string]string{}
	for _, c := range m.Config {
		cfgNS[c.Name] = c.Namespace
	}
	if cfgNS["easyops-url"] != "" {
		t.Errorf("easyops-url namespace = %q, want empty (extension-level)", cfgNS["easyops-url"])
	}
	if cfgNS["buildkit-addr"] != "build" {
		t.Errorf("buildkit-addr namespace = %q, want build", cfgNS["buildkit-addr"])
	}
}

// TestNamespaceReachesSpecs: the namespace must survive BuildConfig, or the
// agent has no way to namespace the stored key.
func TestNamespaceReachesSpecs(t *testing.T) {
	m := Manifest{
		ID: "ops",
		Config: []ManifestConfig{
			{Name: "buildkit-addr", Type: "string", Namespace: "build"},
		},
		Tools: []ManifestTool{
			{Name: "build-image", Namespace: "build"},
		},
	}
	cfg := m.BuildConfig(Bindings{
		Handlers: map[string]extension.ToolSpec{
			"build-image": {Description: "x"},
		},
	})
	if got := cfg.Config["buildkit-addr"].Namespace; got != "build" {
		t.Errorf("config namespace = %q, want build", got)
	}
	if got := cfg.Tools["build-image"].Namespace; got != "build" {
		t.Errorf("tool namespace = %q, want build", got)
	}
}
