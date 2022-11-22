package authenticate

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"time"

	"google.golang.org/protobuf/encoding/protojson"

	"github.com/pomerium/pomerium/internal/httputil"
	"github.com/pomerium/pomerium/internal/sessions"
	"github.com/pomerium/pomerium/internal/urlutil"
	"github.com/pomerium/pomerium/pkg/cryptutil"
	"github.com/pomerium/pomerium/pkg/grpc/databroker"
	"github.com/pomerium/pomerium/pkg/hpke"
)

const defaultExpiry = time.Minute * 5

func (a *Authenticate) redirectViaHPKE(w http.ResponseWriter, r *http.Request,
	receiverPublicKey hpke.PublicKey,
	sessionState *sessions.State,
	records *databroker.Records,
) error {
	state := a.state.Load()

	callbackURL, err := a.getCallbackURL(r)
	if err != nil {
		return err
	}

	values := callbackURL.Query()

	// add the records
	rawRecords, err := protojson.Marshal(records)
	if err != nil {
		return httputil.NewError(http.StatusInternalServerError, fmt.Errorf("error marshaling databroker records: %w", err))
	}
	values.Set(urlutil.QueryRecords, string(rawRecords))

	// add the session
	rawSessionState, err := json.Marshal(sessionState)
	if err != nil {
		return httputil.NewError(http.StatusInternalServerError, fmt.Errorf("error marshaling session state: %w", err))
	}
	values.Set(urlutil.QuerySession, string(rawSessionState))

	// add expiry and issued
	urlutil.BuildTimeParameters(values, defaultExpiry)

	encrypted, err := hpke.EncryptURLValues(state.hpkePrivateKey, receiverPublicKey, values)
	if err != nil {
		return httputil.NewError(http.StatusInternalServerError, fmt.Errorf("error encrypting url values: %w", err))
	}
	callbackURL.RawQuery = encrypted.Encode()

	httputil.Redirect(w, r, callbackURL.String(), http.StatusFound)
	return nil
}

func (a *Authenticate) getCallbackURL(r *http.Request) (*url.URL, error) {
	rawURL := r.FormValue(urlutil.QueryRedirectURI)
	if rawURL == "" {
		return nil, httputil.NewError(http.StatusBadRequest, fmt.Errorf("missing %s", urlutil.QueryRedirectURI))
	}

	redirectURI, err := urlutil.ParseAndValidateURL(rawURL)
	if err != nil {
		return nil, httputil.NewError(http.StatusBadRequest, fmt.Errorf("invalid %s: %w", urlutil.QueryRedirectURI, err))
	}

	var callbackURI *url.URL
	if rawCallbackURL := r.FormValue(urlutil.QueryCallbackURI); rawCallbackURL != "" {
		callbackURI, err = urlutil.ParseAndValidateURL(rawCallbackURL)
		if err != nil {
			return nil, httputil.NewError(http.StatusBadRequest, fmt.Errorf("invalid %s: %w", urlutil.QueryCallbackURI, err))
		}
	} else {
		// otherwise, assume callback is the same host as redirect
		callbackURI, err = urlutil.DeepCopy(redirectURI)
		if err != nil {
			return nil, err
		}
		callbackURI.Path = "/.pomerium/callback/"
		callbackURI.RawQuery = ""
	}

	callbackParams := callbackURI.Query()

	if r.FormValue(urlutil.QueryIsProgrammatic) == "true" {
		callbackParams.Set(urlutil.QueryIsProgrammatic, "true")
	}
	callbackParams.Set(urlutil.QueryRedirectURI, redirectURI.String())
	callbackURI.RawQuery = callbackParams.Encode()
	return callbackURI, nil
}

func (a *Authenticate) storeRecords(w http.ResponseWriter, records *databroker.Records) {
	state := a.state.Load()
	options := a.options.Load()

	decrypted, err := protojson.Marshal(records)
	if err != nil {
		// this shouldn't happen
		panic(fmt.Errorf("error marshaling records: %w", err))
	}

	encrypted := cryptutil.Encrypt(state.cookieCipher, decrypted, nil)

	http.SetCookie(w, &http.Cookie{
		Name:  urlutil.QueryRecords,
		Value: base64.RawURLEncoding.EncodeToString(encrypted),

		Path:     "/",
		Domain:   options.CookieDomain,
		HttpOnly: options.CookieHTTPOnly,
		Secure:   options.CookieSecure,
		Expires:  time.Now().Add(options.CookieExpire),
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
