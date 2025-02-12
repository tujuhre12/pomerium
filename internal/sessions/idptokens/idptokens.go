package idptokens

import (
	"context"
	"fmt"
	"net/http"
	"strings"

	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/pomerium/pomerium/config"
	"github.com/pomerium/pomerium/internal/httputil"
	"github.com/pomerium/pomerium/internal/sessions"
	"github.com/pomerium/pomerium/internal/urlutil"
	"github.com/pomerium/pomerium/pkg/grpc/databroker"
	"github.com/pomerium/pomerium/pkg/grpc/identity"
	"github.com/pomerium/pomerium/pkg/grpc/session"
)

var (
	accessTokenUUIDNamespace   = uuid.MustParse("0194f6f8-e760-76a0-8917-e28ac927a34d")
	identityTokenUUIDNamespace = uuid.MustParse("0194f6f9-aec0-704e-bb4a-51054f17ad17")
)

// A Loader loads sessions from IdP access and identity tokens.
type Loader struct {
	cfg                     *config.Config
	dataBrokerServiceClient databroker.DataBrokerServiceClient
}

// NewLoader creates a new Loader.
func NewLoader(cfg *config.Config, dataBrokerServiceClient databroker.DataBrokerServiceClient) *Loader {
	return &Loader{
		cfg:                     cfg,
		dataBrokerServiceClient: dataBrokerServiceClient,
	}
}

// LoadSession loads sessions from IdP access and identity tokens.
func (l *Loader) LoadSession(r *http.Request) (*session.Session, error) {
	ctx := r.Context()

	idp, err := l.cfg.Options.GetIdentityProviderForRequestURL(urlutil.GetAbsoluteURL(r).String())
	if err != nil {
		return nil, err
	}

	if v := r.Header.Get(httputil.HeaderPomeriumIDPAccessToken); v != "" {
		return l.loadSessionFromAccessToken(ctx, idp, v)
	} else if v := r.Header.Get(httputil.HeaderAuthorization); v != "" {
		prefix := httputil.AuthorizationTypePomeriumIDPAccessToken + " "
		if strings.HasPrefix(strings.ToLower(v), strings.ToLower(prefix)) {
			return l.loadSessionFromAccessToken(ctx, idp, v[len(prefix):])
		}

		prefix = "Bearer " + httputil.AuthorizationTypePomeriumIDPAccessToken + "-"
		if strings.HasPrefix(strings.ToLower(v), strings.ToLower(prefix)) {
			return l.loadSessionFromAccessToken(ctx, idp, v[len(prefix):])
		}
	}

	if v := r.Header.Get(httputil.HeaderPomeriumIDPIdentityToken); v != "" {
		return l.loadSessionFromIdentityToken(ctx, idp, v)
	} else if v := r.Header.Get(httputil.HeaderAuthorization); v != "" {
		prefix := httputil.AuthorizationTypePomeriumIDPIdentityToken + " "
		if strings.HasPrefix(strings.ToLower(v), strings.ToLower(prefix)) {
			return l.loadSessionFromIdentityToken(ctx, idp, v[len(prefix):])
		}

		prefix = "Bearer " + httputil.AuthorizationTypePomeriumIDPIdentityToken + "-"
		if strings.HasPrefix(strings.ToLower(v), strings.ToLower(prefix)) {
			return l.loadSessionFromIdentityToken(ctx, idp, v[len(prefix):])
		}
	}

	return nil, sessions.ErrNoSessionFound
}

func (l *Loader) loadSessionFromAccessToken(ctx context.Context, idp *identity.Provider, rawAccessToken string) (*session.Session, error) {
	sessionID := uuid.NewSHA1(accessTokenUUIDNamespace, []byte(rawAccessToken)).String()
	s, err := session.Get(ctx, l.dataBrokerServiceClient, sessionID)
	if err == nil {
		return s, nil
	} else if status.Code(err) != codes.NotFound {
		return nil, err
	}

	res, err := apiVerifyAccessToken(ctx, l.cfg, &VerifyAccessTokenRequest{
		AccessToken:        rawAccessToken,
		IdentityProviderID: idp.GetId(),
	})
	if err != nil {
		return nil, err
	} else if !res.Valid {
		return nil, fmt.Errorf("invalid access token: %s", res.Error)
	}

	// no session yet, create one
	s = newSession(sessionID, res.Claims)
	s.OauthToken = &session.OAuthToken{
		TokenType:   "Bearer",
		AccessToken: rawAccessToken,
		ExpiresAt:   s.ExpiresAt,
	}
	_, err = session.Put(ctx, l.dataBrokerServiceClient, s)
	if err != nil {
		return nil, err
	}

	return s, nil
}

func (l *Loader) loadSessionFromIdentityToken(ctx context.Context, idp *identity.Provider, rawIdentityToken string) (*session.Session, error) {
	sessionID := uuid.NewSHA1(identityTokenUUIDNamespace, []byte(rawIdentityToken)).String()
	s, err := session.Get(ctx, l.dataBrokerServiceClient, sessionID)
	if err == nil {
		return s, nil
	} else if status.Code(err) != codes.NotFound {
		return nil, err
	}

	res, err := apiVerifyIdentityToken(ctx, l.cfg, &VerifyIdentityTokenRequest{
		IdentityToken:      rawIdentityToken,
		IdentityProviderID: idp.GetId(),
	})
	if err != nil {
		return nil, err
	} else if !res.Valid {
		return nil, fmt.Errorf("invalid access token: %s", res.Error)
	}

	// no session yet, create one
	s = newSession(sessionID, nil)
	s.IdToken = &session.IDToken{
		Subject:   s.UserId,
		ExpiresAt: s.ExpiresAt,
		IssuedAt:  s.IssuedAt,
		Raw:       rawIdentityToken,
	}
	_, err = session.Put(ctx, l.dataBrokerServiceClient, s)
	if err != nil {
		return nil, err
	}

	return s, nil
}

func newSession(sessionID string, claims map[string]any) *session.Session {
	s := &session.Session{
		Id: sessionID,
	}

	if v, ok := claims["oid"].(string); ok {
		s.UserId = v
	} else if v, ok := claims["sub"].(string); ok {
		s.UserId = v
	}

	return s
}
