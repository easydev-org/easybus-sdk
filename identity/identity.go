// Package identity provides static pre-shared identity signing for the ABC
// protocol. It mirrors easybus-sdk's identity.ts: messages carry the
// sender's `easybus-id` plus an HMAC (`easybus-sig`) over canonical fields, so
// receivers authenticate the sender without a CA.
package identity

import (
	"crypto/hmac"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/json"
)

// Identity is a static pre-shared id + secret.
type Identity struct {
	ID     string
	Secret string
}

// Fields are the covered message fields, in signing order (tenant included,
// so a signature minted for one tenant cannot be replayed under another).
type Fields struct {
	Tenant  string
	Ch      string
	Kind    string
	ID      string
	Payload any
}

// Header is the sender-side auth attachment.
type Header struct {
	ID  string
	Sig string
}

func canonical(id string, f Fields) string {
	payload := ""
	if f.Payload != nil {
		b, err := json.Marshal(f.Payload)
		if err == nil {
			payload = string(b)
		}
	}
	return id + "\n" + f.Tenant + "\n" + f.Ch + "\n" + f.Kind + "\n" + f.ID + "\n" + payload
}

func sign(identity Identity, f Fields) string {
	mac := hmac.New(sha256.New, []byte(identity.Secret))
	mac.Write([]byte(canonical(identity.ID, f)))
	return base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
}

// AuthHeader builds the sender-side auth header.
func AuthHeader(identity Identity, f Fields) Header {
	return Header{ID: identity.ID, Sig: sign(identity, f)}
}

// Verify checks an HMAC against a known secret, constant-time.
func Verify(claimedID, secret string, f Fields, provided string) bool {
	want := sign(Identity{ID: claimedID, Secret: secret}, f)
	a := []byte(want)
	b := []byte(provided)
	return len(a) == len(b) && subtle.ConstantTimeCompare(a, b) == 1
}
