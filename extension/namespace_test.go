package extension

import "testing"

// TestNamespacesReachTheManifest: BuildConfig carrying the namespaces is NOT
// enough — they must be published on the wire, together with each tool's and
// each knob's namespace tag. Otherwise a client receives per-tool tags that
// reference names it never got, and can only sort them alphabetically.
//
// This is the bug that shipped in v0.5.0: the field existed, the YAML parsed,
// and the manifest went out with zero namespaces and no per-tool tags.
func TestNamespacesReachTheManifest(t *testing.T) {
	e := New(nil, Config{
		ID: "ops", Version: "1",
		Namespaces: []NamespaceSpec{
			{Name: "services", Description: "Deploy workloads"},
			{Name: "build", Description: "Image builds"},
		},
		Tools: map[string]ToolSpec{
			"create-pvc":  {Description: "Create a PVC", Namespace: "storage"},
			"build-image": {Description: "Build", Namespace: "build"},
		},
		Config: map[string]ConfigSpec{
			"easyops-url":   {Type: "string"},
			"buildkit-addr": {Type: "string", Namespace: "build"},
		},
	})
	m := e.Manifest()

	// 1. The declared groups, in order.
	if len(m.Namespaces) != 2 {
		t.Fatalf("manifest namespaces = %d, want 2", len(m.Namespaces))
	}
	if m.Namespaces[0].GetName() != "services" || m.Namespaces[1].GetName() != "build" {
		t.Errorf("namespace order = %v", m.Namespaces)
	}
	if m.Namespaces[0].GetDescription() != "Deploy workloads" {
		t.Errorf("description lost: %q", m.Namespaces[0].GetDescription())
	}

	// 2. Each tool carries its namespace, and keeps its name.
	byName := map[string]*string{}
	for _, tl := range m.Tools {
		byName[tl.GetName()] = tl.Namespace
	}
	if byName["create-pvc"] == nil || *byName["create-pvc"] != "storage" {
		t.Errorf("create-pvc namespace = %v, want storage", byName["create-pvc"])
	}
	if byName["build-image"] == nil || *byName["build-image"] != "build" {
		t.Errorf("build-image namespace = %v, want build", byName["build-image"])
	}

	// 3. Each knob carries its namespace; extension-level knobs have none.
	knobNS := map[string]*string{}
	for _, c := range m.Config {
		knobNS[c.GetName()] = c.Namespace
	}
	if knobNS["buildkit-addr"] == nil || *knobNS["buildkit-addr"] != "build" {
		t.Errorf("buildkit-addr namespace = %v, want build", knobNS["buildkit-addr"])
	}
	if knobNS["easyops-url"] != nil && *knobNS["easyops-url"] != "" {
		t.Errorf("easyops-url must be extension-level, got %q", *knobNS["easyops-url"])
	}
}
