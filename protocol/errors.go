package protocol

import "strings"

// Classifying a transport error is a PROTOCOL concern, not a transport detail.
//
// Four SDKs talk to the same broker, and each one has to answer the same question when a KV call
// fails: is this an expected outcome (a lost race, an absent key) or a real failure? Getting it
// wrong is silent in both directions:
//
//   - a lost CAS reported as a failure → the lease is renewed against a revision it no longer owns,
//     or a healthy turn is aborted because a network blip looked like a takeover;
//   - a network failure reported as "the key is absent" → an extension starts with no configuration
//     and reports itself unconfigured, while the config is plainly set.
//
// Every SDK had the second shape: `catch { return 0 }`, `except: return 0`, `Err(_) => 0`. So the
// classification lives here, once, and the conformance table pins it for all four.

// IsLostRace reports the server's rejection of a CAS that lost a race.
//
// NATS answers 10071 "wrong last sequence: N". The client library does not expose a typed error for
// it, so the message is matched. Two phrasings exist in the wild, with and without the `stream`
// prefix.
func IsLostRace(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "wrong last sequence")
}

// IsAbsentKey reports an absent key: deleted, or expired under a short TTL.
//
// "no message found" (10037) is what a Get returns once the value expired between being listed and
// being read. It is a MISS, and treating it as an error makes a caller take its error path when the
// correct behaviour is the default one.
func IsAbsentKey(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "no message found") || strings.Contains(msg, "key not found")
}

// IsConsumerGone reports that a JetStream consumer no longer exists.
//
// A durable consumer can be deleted underneath a long-lived subscription: operator cleanup, a
// stream reset, or the server reclaiming it. A pull Fetch then fails IMMEDIATELY with "no
// responders", which one deployment read as "nothing to read" — 2,814,600 empty pulls, no
// delivery, no log. The fix is to rebuild, not to keep asking.
func IsConsumerGone(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "consumer not found") ||
		strings.Contains(msg, "consumer deleted") ||
		strings.Contains(msg, "no responders")
}
