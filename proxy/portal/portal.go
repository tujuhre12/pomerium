// Package portal contains the code for the routes portal
package portal

import (
	"fmt"
	"strings"

	"github.com/pomerium/pomerium/config"
	"github.com/pomerium/pomerium/internal/urlutil"
)

// A Route is a portal route.
type Route struct {
	ID             string `json:"id"`
	Name           string `json:"name"`
	Type           string `json:"type"`
	From           string `json:"from"`
	Description    string `json:"description"`
	ConnectCommand string `json:"connect_command,omitempty"`
	LogoURL        string `json:"logo_url"`
}

// RouteFromConfigRoute returns a Route from a config Route.
func RouteFromConfigRoute(route *config.Policy) Route {
	pr := Route{}
	pr.ID = route.ID
	if pr.ID == "" {
		pr.ID = fmt.Sprintf("%x", route.MustRouteID())
	}
	pr.Name = route.Name
	pr.From = route.From
	fromURL, err := urlutil.ParseAndValidateURL(route.From)
	if err == nil {
		if pr.Name == "" {
			pr.Name = fromURL.Host
		}
		if strings.HasPrefix(fromURL.Scheme, "tcp+") {
			pr.Type = "tcp"
			pr.ConnectCommand = "pomerium-cli tcp " + fromURL.Host
		} else if strings.HasPrefix(fromURL.Scheme, "udp+") {
			pr.Type = "udp"
			pr.ConnectCommand = "pomerium-cli udp " + fromURL.Host
		} else {
			pr.Type = "http"
		}
	} else {
		pr.Type = "http"
	}
	pr.Description = route.Description
	pr.LogoURL = route.LogoURL
	return pr
}
