package authenticate

import (
	"encoding/base64"
	"fmt"
	"net/http"

	"google.golang.org/protobuf/encoding/protojson"

	"github.com/pomerium/pomerium/internal/urlutil"
	"github.com/pomerium/pomerium/pkg/cryptutil"
	"github.com/pomerium/pomerium/pkg/grpc/databroker"
)

func (a *Authenticate) storeRecords(w http.ResponseWriter, records *databroker.Records) {
	state := a.state.Load()

	decrypted, err := protojson.Marshal(records)
	if err != nil {
		// this shouldn't happen
		panic(fmt.Errorf("error marshaling records: %w", err))
	}

	encrypted := cryptutil.Encrypt(state.cookieCipher, decrypted, nil)

	http.SetCookie(w, &http.Cookie{
		Name:  urlutil.QueryRecords,
		Value: base64.RawURLEncoding.EncodeToString(encrypted),
		Path:  "/",
	})
}

func (a *Authenticate) loadRecords(r *http.Request) *databroker.Records {
	state := a.state.Load()

	cookie, err := r.Cookie(urlutil.QueryRecords)
	if err != nil {
		return nil
	}

	encrypted, err := base64.RawURLEncoding.DecodeString(cookie.Value)
	if err != nil {
		return nil
	}

	decrypted, err := cryptutil.Decrypt(state.cookieCipher, encrypted, nil)
	if err != nil {
		return nil
	}

	var records databroker.Records
	err = protojson.Unmarshal(decrypted, &records)
	if err != nil {
		return nil
	}

	return &records
}
