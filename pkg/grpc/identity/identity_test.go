package identity

import (
	"testing"

	"github.com/stretchr/testify/assert"
	structpb "google.golang.org/protobuf/types/known/structpb"
)

func TestProfileShrink(t *testing.T) {
	t.Parallel()

	p := &Profile{
		ProviderId: "provider-1",
		IdToken:    []byte{1, 2, 3, 4},
		OauthToken: []byte{5, 6, 7, 8},
		Claims: &structpb.Struct{
			Fields: map[string]*structpb.Value{
				"a":             structpb.NewStringValue("A"),
				"b":             structpb.NewStringValue("B"),
				"c":             structpb.NewStringValue("reallylongvalue"),
				"reallylongkey": structpb.NewNumberValue(1),
			},
		},
	}

	if assert.True(t, p.Shrink()) {
		assert.Len(t, p.GetClaims().GetFields(), 3)
		assert.Contains(t, p.GetClaims().GetFields(), "reallylongkey")
	}
	if assert.True(t, p.Shrink()) {
		assert.Len(t, p.GetClaims().GetFields(), 2)
		assert.Contains(t, p.GetClaims().GetFields(), "b")
	}
	if assert.True(t, p.Shrink()) {
		assert.Len(t, p.GetClaims().GetFields(), 1)
		assert.Contains(t, p.GetClaims().GetFields(), "a")
	}
	if assert.True(t, p.Shrink()) {
		assert.Len(t, p.GetClaims().GetFields(), 0)
	}
	if assert.True(t, p.Shrink()) {
		assert.Empty(t, p.GetIdToken())
	}
	if assert.True(t, p.Shrink()) {
		assert.Empty(t, p.GetOauthToken())
	}
	if assert.False(t, p.Shrink()) {
		assert.Equal(t, "provider-1", p.GetProviderId())
	}
}
