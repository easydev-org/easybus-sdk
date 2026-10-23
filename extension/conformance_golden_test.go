package extension

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

// The shared matching table, from conformance-cases/permission-matching.json.
//
// WHY a shared file rather than a test written once here: the rules are implemented four times,
// and they are the security boundary of every extension. Two SDKs that disagree do not fail
// loudly — they authorise different things for the same grant, silently, depending on which
// language the extension happens to be written in. Every SDK reads this same table.
type matchingCase struct {
	Grant  map[string]string `json:"grant"`
	Target map[string]string `json:"target"`
	Want   bool              `json:"want"`
	Why    string            `json:"why"`
}

type matchingTable struct {
	ID    string         `json:"id"`
	Cases []matchingCase `json:"cases"`
}

func TestSharedMatchingTable(t *testing.T) {
	path := filepath.Join("..", "conformance-cases", "permission-matching.json")
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read shared table: %v", err)
	}
	var tbl matchingTable
	if err := json.Unmarshal(raw, &tbl); err != nil {
		t.Fatalf("parse shared table: %v", err)
	}
	if len(tbl.Cases) == 0 {
		t.Fatal("the shared table is empty")
	}
	for i, c := range tbl.Cases {
		if got := Matches(c.Grant, c.Target); got != c.Want {
			t.Errorf("case %d (%s): grant %v target %v = %v, want %v",
				i, c.Why, c.Grant, c.Target, got, c.Want)
		}
	}
}
