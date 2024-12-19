package envoyconfig

import (
	"net"
	"strconv"

	envoy_config_listener_v3 "github.com/envoyproxy/go-control-plane/envoy/config/listener/v3"
	envoy_config_route_v3 "github.com/envoyproxy/go-control-plane/envoy/config/route/v3"
	envoy_extensions_filters_network_http_connection_manager "github.com/envoyproxy/go-control-plane/envoy/extensions/filters/network/http_connection_manager/v3"

	"github.com/pomerium/pomerium/config"
)

func (b *Builder) buildRedirectListener(
	cfg *config.Config,
) *envoy_config_listener_v3.Listener {
	li := newTCPListener("http-redirect", buildTCPAddress(cfg.Options.HTTPRedirectAddr, 80))

	// listener filters
	if cfg.Options.UseProxyProtocol {
		li.ListenerFilters = append(li.ListenerFilters, ProxyProtocolFilter())
	}

	li.FilterChains = append(li.FilterChains, &envoy_config_listener_v3.FilterChain{
		Filters: []*envoy_config_listener_v3.Filter{
			b.buildRedirectHTTPConnectionManagerFilter(cfg),
		},
	})

	return li
}

func (b *Builder) buildRedirectHTTPConnectionManagerFilter(
	cfg *config.Config,
) *envoy_config_listener_v3.Filter {
	_, strport, _ := net.SplitHostPort(cfg.Options.Addr)
	port, _ := strconv.ParseUint(strport, 10, 32)
	if port == 0 {
		port = 443
	}

	return HTTPConnectionManagerFilter(&envoy_extensions_filters_network_http_connection_manager.HttpConnectionManager{
		AlwaysSetRequestIdInResponse: true,
		StatPrefix:                   "http-redirect",
		RouteSpecifier: &envoy_extensions_filters_network_http_connection_manager.HttpConnectionManager_RouteConfig{
			RouteConfig: &envoy_config_route_v3.RouteConfiguration{
				Name: "http-redirect",
				VirtualHosts: []*envoy_config_route_v3.VirtualHost{{
					Name:    "http-redirect",
					Domains: []string{"*"},
					Routes: []*envoy_config_route_v3.Route{
						b.buildACMEHTTPRoute(),
						{
							Match: &envoy_config_route_v3.RouteMatch{
								PathSpecifier: &envoy_config_route_v3.RouteMatch_Prefix{
									Prefix: "/",
								},
							},
							Action: &envoy_config_route_v3.Route_Redirect{
								Redirect: &envoy_config_route_v3.RedirectAction{
									SchemeRewriteSpecifier: &envoy_config_route_v3.RedirectAction_HttpsRedirect{
										HttpsRedirect: true,
									},
									PortRedirect: uint32(port),
								},
							},
						},
					},
				}},
			},
		},
		HttpFilters: []*envoy_extensions_filters_network_http_connection_manager.HttpFilter{
			HTTPRouterFilter(),
		},
	})
}

func shouldStartRedirectListener(options *config.Options) bool {
	return !options.InsecureServer && options.HTTPRedirectAddr != ""
}
