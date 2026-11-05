package manifest

import "testing"

/**
 * EVERY TOOL DECLARES ITS IMPACT, AND THE PARSE FAILS WITHOUT IT.
 *
 * The declaration is the only thing that can tell a client "this one only reads" from "this one deletes a
 * repository", and a missing or wrong value is UNDETECTABLE at runtime: nothing fails, the tool simply
 * runs with whatever the caller assumed. So the check has to happen where the declaration is read.
 */
func TestEffectIsRequired(t *testing.T) {
	_, err := ParseManifest([]byte(`
id: probe
tools:
  - name: dangerous
    description: does something
`))
	if err == nil {
		t.Fatal("a tool with no effect parsed; it would be presented as harmless")
	}
}

func TestEffectMustBeOneOfTheFour(t *testing.T) {
	for _, bad := range []string{"catastrophic", "medium2", "NONE"} {
		_, err := ParseManifest([]byte(`
id: probe
tools:
  - name: t
    description: d
    effect: ` + bad + `
`))
		if err == nil {
			t.Errorf("effect %q was accepted", bad)
		}
	}
	for _, good := range []string{"none", "low", "medium", "high"} {
		if _, err := ParseManifest([]byte(`
id: probe
tools:
  - name: t
    description: d
    effect: ` + good + `
`)); err != nil {
			t.Errorf("effect %q rejected: %v", good, err)
		}
	}
}

/**
 * A PERMISSION MAY NOT LOOK SAFER THAN THE TOOL IT UNLOCKS.
 *
 * `risk` and `effect` answer different questions, but they cannot contradict each other: a high-impact
 * tool behind a `read`-labelled permission is a prompt to approve something while being told it is safe.
 */
func TestPermissionRiskCoversToolEffect(t *testing.T) {
	_, err := ParseManifest([]byte(`
id: probe
tools:
  - name: wipe
    description: destroys things
    effect: high
    requires: [wipe.all]
permissions:
  - key: wipe.all
    label: Wipe everything
    risk: read
`))
	if err == nil {
		t.Fatal("a high-effect tool behind risk=read parsed")
	}
}

// And the top risk level covers both of the top effects, since the two scales differ in length.
func TestDestructiveRiskCoversMediumAndHigh(t *testing.T) {
	for _, eff := range []string{"medium", "high"} {
		if _, err := ParseManifest([]byte(`
id: probe
tools:
  - name: act
    description: does a thing
    effect: ` + eff + `
    requires: [act.all]
permissions:
  - key: act.all
    label: Act
    risk: destructive
`)); err != nil {
			t.Errorf("effect %q with risk=destructive rejected: %v", eff, err)
		}
	}
}
