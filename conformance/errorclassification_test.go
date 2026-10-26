package conformance

import (
	"errors"
	"testing"

	"github.com/easydev-org/easybus-sdk/protocol"
)

// runErrorClassificationCase pins how a transport error is CATEGORISED.
//
// This is the shared half of the KV fix. Each SDK used to collapse every failure into a zero value
// (`return 0`, `catch { return 0 }`, `except: return 0`, `Err(_) => 0`), which made a lost race and
// a broken connection indistinguishable. The zero-value behaviour is CORRECT for a lost race and
// catastrophic for anything else, so the category is the contract.
//
// An empty `err` means "no error at all": it must match nothing. A classifier that reports a nil
// error as a fault would send every success path down a fault branch and misread its own result.
func runErrorClassificationCase(t *testing.T, c sharedCase) {
	t.Helper()
	if len(c.Cases) == 0 {
		t.Fatalf("%s: no cases", c.ID)
	}
	for i, mc := range c.Cases {
		msg, _ := mc["err"].(string)
		wantLostRace, _ := mc["lostRace"].(bool)
		wantAbsent, _ := mc["absent"].(bool)
		wantGone, _ := mc["consumerGone"].(bool)

		// An empty message means "no error"; everything else is a real error carrying that text.
		var err error
		if msg != "" {
			err = errors.New(msg)
		}

		if got := protocol.IsLostRace(err); got != wantLostRace {
			t.Errorf("case %d (%v): IsLostRace(%q) = %v, want %v",
				i, mc["why"], msg, got, wantLostRace)
		}
		if got := protocol.IsAbsentKey(err); got != wantAbsent {
			t.Errorf("case %d (%v): IsAbsentKey(%q) = %v, want %v",
				i, mc["why"], msg, got, wantAbsent)
		}
		if got := protocol.IsConsumerGone(err); got != wantGone {
			t.Errorf("case %d (%v): IsConsumerGone(%q) = %v, want %v",
				i, mc["why"], msg, got, wantGone)
		}
	}
}

// The three categories must be MUTUALLY EXCLUSIVE for every case in the shared table.
//
// If one message satisfied two of them, the caller's `switch` would pick whichever branch it
// happens to test first, and the behaviour would depend on clause order rather than on the fault.
func TestErrorClassificationCategoriesAreExclusive(t *testing.T) {
	for _, c := range loadSharedCases(t) {
		if c.Kind != "error-classification" {
			continue
		}
		for i, mc := range c.Cases {
			msg, _ := mc["err"].(string)
			var err error
			if msg != "" {
				err = errors.New(msg)
			}
			n := 0
			for _, m := range []bool{protocol.IsLostRace(err), protocol.IsAbsentKey(err), protocol.IsConsumerGone(err)} {
				if m {
					n++
				}
			}
			if n > 1 {
				t.Errorf("case %d (%q): matches %d categories; a fault must belong to at most one", i, msg, n)
			}
		}
	}
}
