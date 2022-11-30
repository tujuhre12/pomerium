// Package identity contains protobuf types for identity management.
package identity

import (
	"crypto/sha256"
	"sort"

	"golang.org/x/exp/maps"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	"github.com/pomerium/pomerium/pkg/encoding/base58"
)

// Clone clones the Provider.
func (x *Provider) Clone() *Provider {
	return proto.Clone(x).(*Provider)
}

// Hash computes a sha256 hash of the provider's fields. It excludes the Id field.
func (x *Provider) Hash() string {
	tmp := x.Clone()
	tmp.Id = ""
	bs, _ := proto.MarshalOptions{
		AllowPartial:  true,
		Deterministic: true,
	}.Marshal(tmp)
	h := sha256.Sum256(bs)
	return base58.Encode(h[:])
}

// Shrink attempts to remove data from the profile. It removes the largest field, then the id token, then the oauth token.
// It returns true if something was removed.
func (x *Profile) Shrink() bool {
	if x == nil {
		return false
	}

	if x.Claims != nil && len(x.Claims.Fields) > 0 {
		var biggestKey string
		var biggestLength int
		keys := maps.Keys(x.Claims.Fields)
		sort.Strings(keys)
		for _, k := range keys {
			v := x.Claims.Fields[k]
			bs, _ := protojson.Marshal(v)
			if len(bs)+len(k) >= biggestLength {
				biggestKey = k
				biggestLength = len(bs) + len(k)
			}
		}
		delete(x.Claims.Fields, biggestKey)
		return true
	}

	if len(x.IdToken) > 0 {
		x.IdToken = nil
		return true
	}

	if len(x.OauthToken) > 0 {
		x.OauthToken = nil
		return true
	}

	return false
}
