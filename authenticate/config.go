package authenticate

import (
	"github.com/pomerium/pomerium/config"
	"github.com/pomerium/pomerium/internal/authenticateflow"
	"github.com/pomerium/pomerium/internal/identity"
	identitypb "github.com/pomerium/pomerium/pkg/grpc/identity"
)

type (
	// AuthEventKind is the authenticateflow.AuthEventKind.
	AuthEventKind = authenticateflow.AuthEventKind
	// AuthEvent is the authenticateflow.AuthEvent.
	AuthEvent = authenticateflow.AuthEvent
	// AuthEventFn is the authenticateflow.AuthEventFn.
	AuthEventFn = authenticateflow.AuthEventFn
)

// re-export constants
const (
	AuthEventSignInRequest  = authenticateflow.AuthEventSignInRequest
	AuthEventSignInComplete = authenticateflow.AuthEventSignInComplete
)

type authenticateConfig struct {
	getIdentityProvider func(options *config.Options, idpID string) (identity.Authenticator, error)
	profileTrimFn       func(*identitypb.Profile)
	authEventFn         AuthEventFn
}

// An Option customizes the Authenticate config.
type Option func(*authenticateConfig)

func getAuthenticateConfig(options ...Option) *authenticateConfig {
	cfg := new(authenticateConfig)
	WithGetIdentityProvider(defaultGetIdentityProvider)(cfg)
	for _, option := range options {
		option(cfg)
	}
	return cfg
}

// WithGetIdentityProvider sets the getIdentityProvider function in the config.
func WithGetIdentityProvider(getIdentityProvider func(options *config.Options, idpID string) (identity.Authenticator, error)) Option {
	return func(cfg *authenticateConfig) {
		cfg.getIdentityProvider = getIdentityProvider
	}
}

// WithProfileTrimFn sets the profileTrimFn function in the config
func WithProfileTrimFn(profileTrimFn func(*identitypb.Profile)) Option {
	return func(cfg *authenticateConfig) {
		cfg.profileTrimFn = profileTrimFn
	}
}

// WithOnAuthenticationEventHook sets the authEventFn function in the config
func WithOnAuthenticationEventHook(fn authenticateflow.AuthEventFn) Option {
	return func(cfg *authenticateConfig) {
		cfg.authEventFn = fn
	}
}
