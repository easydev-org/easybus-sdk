package nats

import (
	"errors"
	"testing"
)

// The error-classification helpers decide behaviour under fault, so they are pinned directly.
//
// A test that stops a broker proves the call fails; it CANNOT prove the classification is right,
// because the failure surfaces before the classified branch is reached. These functions are the
// branch, so they are what gets asserted.

// A lost CAS must be recognised as "normal", not as a failure.
//
// If `wrong last sequence` were treated as an error, `Renew` would return (held=true, err) and the
// runtime would keep running a turn another replica now owns. If a network error were treated as a
// loss, `Renew` would return (held=false, nil) and the runtime would abort a turn it still owns.
// The two must never collapse.
func TestErrorClassification(t *testing.T) {
	cases := []struct {
		name         string
		err          error
		wrongSeq     bool
		noMsgFound   bool
		consumerGone bool
	}{
		{
			name:     "server's 10071 phrasing",
			err:      errors.New("nats: wrong last sequence: 42"),
			wrongSeq: true,
		},
		{
			name:     "stream-prefixed phrasing",
			err:      errors.New("nats: stream wrong last sequence"),
			wrongSeq: true,
		},
		{
			name:     "a network failure is NOT a lost race",
			err:      errors.New("nats: connection closed"),
			wrongSeq: false,
		},
		{
			name:     "a timeout is NOT a lost race",
			err:      errors.New("nats: timeout"),
			wrongSeq: false,
		},
		{
			name:       "server's 10037 phrasing",
			err:        errors.New("nats: no message found"),
			noMsgFound: true,
		},
		{
			name:         "consumer deleted",
			err:          errors.New("nats: consumer not found"),
			consumerGone: true,
		},
		{
			name:         "no responders (deleted consumer, pull request)",
			err:          errors.New("nats: no responders available for request"),
			consumerGone: true,
		},
		{
			name:         "a plain timeout is NOT a deleted consumer",
			err:          errors.New("nats: timeout"),
			consumerGone: false,
		},
	}
	for _, c := range cases {
		if got := isWrongLastSequence(c.err); got != c.wrongSeq {
			t.Errorf("%s: isWrongLastSequence = %v, want %v (err=%v)", c.name, got, c.wrongSeq, c.err)
		}
		if got := isNoMessageFound(c.err); got != c.noMsgFound {
			t.Errorf("%s: isNoMessageFound = %v, want %v", c.name, got, c.noMsgFound)
		}
		if got := isConsumerGone(c.err); got != c.consumerGone {
			t.Errorf("%s: isConsumerGone = %v, want %v", c.name, got, c.consumerGone)
		}
	}
}

// nil must classify as none of the above, or every "no error" path would take a fault branch.
func TestErrorClassificationOfNil(t *testing.T) {
	if isWrongLastSequence(nil) || isNoMessageFound(nil) || isConsumerGone(nil) {
		t.Fatal("nil was classified as a specific fault; every success path would take a fault " +
			"branch and misread its own result")
	}
}
