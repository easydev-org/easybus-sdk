package manifest

import (
	"context"
	"testing"

	"github.com/easydev-org/easybus-sdk/extension"
)

// An extension declares permissions in YAML; a client reads them from the published
// manifest. These tests pin that round trip, because a client cannot render a permission
// it never receives — and the failure would look like "the UI has no permission panel".

const permsYAML = `
id: demo
version: "1"
permissions:
  - key: mr.merge
    label: Merge pull requests
    descriptions:
      zh: 合并 MR
    risk: destructive
    params:
      - name: repo
        type: string
        required: true
        description: owner/name or a wildcard
  - key: exec
    label: Run commands
    risk: write
tools:
  - name: merge-it
    description: merge
    requires: [mr.merge]
`

func TestPermissionsRoundTrip(t *testing.T) {
	m, err := ParseManifest([]byte(permsYAML))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if len(m.Permissions) != 2 {
		t.Fatalf("parsed %d permissions, want 2", len(m.Permissions))
	}

	merge := m.Permissions[0]
	if merge.Key != "mr.merge" || merge.Risk != "destructive" {
		t.Errorf("first permission = %+v", merge)
	}
	if merge.Descriptions["zh"] != "合并 MR" {
		t.Errorf("descriptions lost: %+v", merge.Descriptions)
	}
	// The params shape is what a client renders a form from; losing it means the UI has
	// nothing to show.
	if len(merge.ParamFields) != 1 || merge.ParamFields[0].Name != "repo" {
		t.Fatalf("param fields = %+v", merge.ParamFields)
	}
	if !merge.ParamFields[0].Required {
		t.Error("required flag lost")
	}

	// A permission with no params is a boolean capability, not an error.
	if len(m.Permissions[1].ParamFields) != 0 {
		t.Errorf("boolean permission should carry no params, got %+v", m.Permissions[1].ParamFields)
	}

	// And the tool's requirement survives, which is what makes it gated at all.
	if len(m.Tools) != 1 || len(m.Tools[0].Requires) != 1 || m.Tools[0].Requires[0] != "mr.merge" {
		t.Fatalf("tool requires = %+v", m.Tools)
	}
}

// The manifest is the single source for a tool's requirements: BuildConfig copies them
// onto the runtime spec, so a handler cannot drift from what the manifest documents.
func TestBuildConfigCarriesRequires(t *testing.T) {
	m, err := ParseManifest([]byte(permsYAML))
	if err != nil {
		t.Fatal(err)
	}
	cfg := m.BuildConfig(Bindings{Handlers: map[string]extension.ToolSpec{
		"merge-it": {Execute: func(_ context.Context, _ map[string]any, _, _, _ string) (extension.ToolResultData, error) {
			return extension.ToolResultData{}, nil
		}},
	}})
	spec, ok := cfg.Tools["merge-it"]
	if !ok {
		t.Fatal("tool missing from config")
	}
	if len(spec.Requires) != 1 || spec.Requires[0] != "mr.merge" {
		t.Fatalf("Requires = %v, want [mr.merge]", spec.Requires)
	}
	// And the declared permissions reach the runtime config, so the extension publishes
	// them in its manifest.
	if len(cfg.Permissions) != 2 {
		t.Fatalf("config carries %d permissions, want 2", len(cfg.Permissions))
	}
	if len(cfg.Permissions[0].ParamsFields) != 1 {
		t.Errorf("params shape lost on the way to the runtime config")
	}
}
