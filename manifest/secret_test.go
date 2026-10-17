package manifest

import "testing"

// A secret knob must be a string and must not declare a default (a default
// would ride the broadcast manifest).
func TestSecretConfigValidation(t *testing.T) {
	ok := `
id: x
version: "1"
config:
  - name: gh-token
    type: string
    kind: secret
`
	if _, err := ParseManifest([]byte(ok)); err != nil {
		t.Fatalf("valid secret manifest rejected: %v", err)
	}

	badType := `
id: x
version: "1"
config:
  - name: gh-token
    type: number
    kind: secret
`
	if _, err := ParseManifest([]byte(badType)); err == nil {
		t.Error("secret with non-string type should be rejected")
	}

	badDefault := `
id: x
version: "1"
config:
  - name: gh-token
    type: string
    kind: secret
    default: ghp_leaked
`
	if _, err := ParseManifest([]byte(badDefault)); err == nil {
		t.Error("secret with a default should be rejected")
	}

	// An ordinary knob is unaffected.
	plain := `
id: x
version: "1"
config:
  - name: region
    type: string
    default: us
`
	if _, err := ParseManifest([]byte(plain)); err != nil {
		t.Fatalf("ordinary manifest rejected: %v", err)
	}
}
