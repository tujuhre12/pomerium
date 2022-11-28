package proxy

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	"github.com/pomerium/pomerium/internal/httputil"
	"github.com/pomerium/pomerium/internal/log"
	"github.com/pomerium/pomerium/internal/sessions"
	"github.com/pomerium/pomerium/internal/urlutil"
	"github.com/pomerium/pomerium/pkg/grpc/databroker"
	"github.com/pomerium/pomerium/pkg/grpc/session"
	"github.com/pomerium/pomerium/pkg/grpc/user"
	"github.com/pomerium/pomerium/pkg/hpke"
)

// callbackViaHPKE implements the callback flow which expects an HPKE encrypted query string
// with the following query string parameters:
//   - pomerium_expiry: when the session expires
//   - pomerium_issued: when the session was issued
//   - pomerium_records: json-encoded array of records to save to the databroker
//   - pomerium_redirect_uri: where to redirect next
//   - pomerium_session: json-encoded sessions.State
func (p *Proxy) callbackViaHPKE(w http.ResponseWriter, r *http.Request) error {
	state := p.state.Load()

	// decrypt the URL values
	senderPublicKey, values, err := hpke.DecryptURLValues(state.hpkePrivateKey, r.Form)
	if err != nil {
		return httputil.NewError(http.StatusBadRequest, fmt.Errorf("invalid encrypted query string: %w", err))
	}

	log.Info(r.Context()).Interface("values", values).Msg("<<<VALuES>>>")

	// confirm this request came from the authenticate service
	err = p.validateSenderPublicKey(r.Context(), senderPublicKey)
	if err != nil {
		return err
	}

	// validate that the request has not expired
	err = urlutil.ValidateTimeParameters(values)
	if err != nil {
		return httputil.NewError(http.StatusBadRequest, err)
	}

	// retrieve the values from the query string
	ss, err := getFromValues[sessions.State](values, urlutil.QuerySessionState)
	if err != nil {
		return err
	}

	s, err := getFromValues[session.Session](values, urlutil.QuerySession)
	if err != nil {
		return err
	}

	u, err := getFromValues[user.User](values, urlutil.QueryUser)
	if err != nil {
		return err
	}

	redirectURI, err := p.getRedirectURIFromValues(values)
	if err != nil {
		return err
	}

	// save the records
	res, err := state.dataBrokerClient.Put(r.Context(), &databroker.PutRequest{
		Records: []*databroker.Record{
			databroker.NewRecord(s),
			databroker.NewRecord(u),
		},
	})
	if err != nil {
		return httputil.NewError(http.StatusInternalServerError, fmt.Errorf("proxy: error saving databroker records: %w", err))
	}
	ss.DatabrokerServerVersion = res.GetServerVersion()
	for _, record := range res.GetRecords() {
		if record.GetVersion() > ss.DatabrokerRecordVersion {
			ss.DatabrokerRecordVersion = record.GetVersion()
		}
	}

	// save the session state
	rawJWT, err := state.encoder.Marshal(ss)
	if err != nil {
		return httputil.NewError(http.StatusInternalServerError, fmt.Errorf("proxy: error marshaling session state: %w", err))
	}
	if err = state.sessionStore.SaveSession(w, r, rawJWT); err != nil {
		return httputil.NewError(http.StatusInternalServerError, fmt.Errorf("proxy: error saving session state: %w", err))
	}

	// if programmatic, encode the session jwt as a query param
	if isProgrammatic := r.FormValue(urlutil.QueryIsProgrammatic); isProgrammatic == "true" {
		q := redirectURI.Query()
		q.Set(urlutil.QueryPomeriumJWT, string(rawJWT))
		redirectURI.RawQuery = q.Encode()
	}

	// redirect
	httputil.Redirect(w, r, redirectURI.String(), http.StatusFound)
	return nil
}

func (p *Proxy) getRedirectURIFromValues(values url.Values) (*url.URL, error) {
	rawRedirectURI := values.Get(urlutil.QueryRedirectURI)
	if rawRedirectURI == "" {
		return nil, httputil.NewError(http.StatusBadRequest, fmt.Errorf("missing %s", urlutil.QueryRedirectURI))
	}
	redirectURI, err := urlutil.ParseAndValidateURL(rawRedirectURI)
	if err != nil {
		return nil, httputil.NewError(http.StatusBadRequest, fmt.Errorf("invalid %s: %w", urlutil.QueryRedirectURI, err))
	}
	return redirectURI, nil
}

func (p *Proxy) validateSenderPublicKey(ctx context.Context, senderPublicKey *hpke.PublicKey) error {
	state := p.state.Load()

	authenticatePublicKey, err := state.authenticateKeyFetcher.FetchPublicKey(ctx)
	if err != nil {
		return httputil.NewError(http.StatusInternalServerError, fmt.Errorf("hpke: error retrieving authenticate service public key: %w", err))
	}

	if !authenticatePublicKey.Equals(senderPublicKey) {
		return httputil.NewError(http.StatusBadRequest, fmt.Errorf("hpke: invalid authenticate service public key"))
	}

	return nil
}

func getFromValues[T any](values url.Values, name string) (*T, error) {
	raw := values.Get(name)
	if raw == "" {
		return nil, httputil.NewError(http.StatusBadRequest, fmt.Errorf("missing %s", name))
	}
	var err error
	var obj T
	if protoObj, ok := (interface{})(&obj).(proto.Message); ok {
		err = protojson.Unmarshal([]byte(raw), protoObj)
	} else {
		err = json.Unmarshal([]byte(raw), &obj)
	}
	if err != nil {
		return nil, httputil.NewError(http.StatusBadRequest, fmt.Errorf("invalid %s: %w", name, err))
	}
	return &obj, nil
}
