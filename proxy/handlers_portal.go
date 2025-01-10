package proxy

import (
	"encoding/json"
	"net/http"

	"github.com/pomerium/pomerium/internal/httputil"
	"github.com/pomerium/pomerium/proxy/portal"
	"github.com/pomerium/pomerium/ui"
)

func (p *Proxy) routesPortalHTML(w http.ResponseWriter, r *http.Request) error {
	routes := p.getPortalRoutes(r)
	return ui.ServePage(w, r, "Routes", "Routes Portal", map[string]any{
		"routes": routes,
	})
}

func (p *Proxy) routesPortalJSON(w http.ResponseWriter, r *http.Request) error {
	routes := p.getPortalRoutes(r)
	b, err := json.Marshal(map[string]any{
		"routes": routes,
	})
	if err != nil {
		return httputil.NewError(http.StatusInternalServerError, err)
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(b)
	return nil
}

func (p *Proxy) getPortalRoutes(r *http.Request) []portal.Route {
	options := p.currentOptions.Load()
	user := p.getPortalUserInfo(r)
	var routes []portal.Route
	for route := range options.GetAllPolicies() {
		if portal.CheckRouteAccess(user, route) {
			routes = append(routes, portal.RouteFromConfigRoute(route))
		}
	}
	return routes
}
