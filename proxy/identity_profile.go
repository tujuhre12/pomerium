package proxy

import (
	"encoding/json"
	"fmt"
	"time"

	"golang.org/x/oauth2"

	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/pomerium/pomerium/internal/identity"
	"github.com/pomerium/pomerium/internal/identity/manager"
	"github.com/pomerium/pomerium/internal/sessions"
	identitypb "github.com/pomerium/pomerium/pkg/grpc/identity"
	"github.com/pomerium/pomerium/pkg/grpc/session"
	"github.com/pomerium/pomerium/pkg/grpc/user"
)

func newSessionStateFromIdentityProfile(profile *identitypb.Profile) *sessions.State {
	claims := profile.GetClaims().AsMap()

	sessionState := sessions.NewState(profile.GetProviderId())

	// set the subject
	if v, ok := claims["sub"]; ok {
		sessionState.Subject = fmt.Sprint(v)
	} else if v, ok := claims["user"]; ok {
		sessionState.Subject = fmt.Sprint(v)
	}

	// set the oid
	if v, ok := claims["oid"]; ok {
		sessionState.OID = fmt.Sprint(v)
	}

	return sessionState
}

func (p *Proxy) fillSession(s *session.Session, profile *identitypb.Profile, sessionState *sessions.State) {
	options := p.currentOptions.Load()

	claims := profile.GetClaims().AsMap()
	oauthToken := new(oauth2.Token)
	_ = json.Unmarshal(profile.GetOauthToken(), oauthToken)

	s.UserId = sessionState.UserID()
	s.IssuedAt = timestamppb.Now()
	s.AccessedAt = timestamppb.Now()
	s.ExpiresAt = timestamppb.New(time.Now().Add(options.CookieExpire))
	s.IdToken = &session.IDToken{
		Issuer:    sessionState.Issuer,
		Subject:   sessionState.Subject,
		ExpiresAt: timestamppb.New(time.Now().Add(options.CookieExpire)),
		IssuedAt:  timestamppb.Now(),
		Raw:       string(profile.GetIdToken()),
	}
	s.OauthToken = manager.ToOAuthToken(oauthToken)
	if s.Claims == nil {
		s.Claims = make(map[string]*structpb.ListValue)
	}
	for k, vs := range identity.Claims(claims).Flatten().ToPB() {
		s.Claims[k] = vs
	}
}

func (p *Proxy) fillUser(u *user.User, profile *identitypb.Profile, sessionState *sessions.State) {
	claims := profile.GetClaims().AsMap()
	if v, ok := claims["name"]; ok {
		u.Name = fmt.Sprint(v)
	}
	if v, ok := claims["email"]; ok {
		u.Email = fmt.Sprint(v)
	}
	if u.Claims == nil {
		u.Claims = make(map[string]*structpb.ListValue)
	}
	for k, vs := range identity.Claims(claims).Flatten().ToPB() {
		u.Claims[k] = vs
	}
}
