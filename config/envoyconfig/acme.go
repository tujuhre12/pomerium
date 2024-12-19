package envoyconfig

import (
	"strconv"

	envoy_config_cluster_v3 "github.com/envoyproxy/go-control-plane/envoy/config/cluster/v3"
	envoy_config_endpoint_v3 "github.com/envoyproxy/go-control-plane/envoy/config/endpoint/v3"
	envoy_config_listener_v3 "github.com/envoyproxy/go-control-plane/envoy/config/listener/v3"
	envoy_config_route_v3 "github.com/envoyproxy/go-control-plane/envoy/config/route/v3"

	"github.com/pomerium/pomerium/config"
)

const (
	acmeHTTPClusterName = "pomerium-acme-http"
	acmeHTTPPrefix      = "/.well-known/acme-challenge/"
)

func (b *Builder) buildACMEHTTPCluster(cfg *config.Config) *envoy_config_cluster_v3.Cluster {
	port, _ := strconv.ParseUint(cfg.ACMEHTTPPort, 10, 32)
	return &envoy_config_cluster_v3.Cluster{
		Name: acmeHTTPClusterName,
		LoadAssignment: &envoy_config_endpoint_v3.ClusterLoadAssignment{
			ClusterName: acmeHTTPClusterName,
			Endpoints: []*envoy_config_endpoint_v3.LocalityLbEndpoints{{
				LbEndpoints: []*envoy_config_endpoint_v3.LbEndpoint{{
					HostIdentifier: &envoy_config_endpoint_v3.LbEndpoint_Endpoint{
						Endpoint: &envoy_config_endpoint_v3.Endpoint{
							Address: buildTCPAddress("127.0.0.1", uint32(port)),
						},
					},
				}},
			}},
		},
	}
}

func (b *Builder) buildACMEHTTPRoute() *envoy_config_route_v3.Route {
	return &envoy_config_route_v3.Route{
		Match: &envoy_config_route_v3.RouteMatch{
			PathSpecifier: &envoy_config_route_v3.RouteMatch_Prefix{
				Prefix: acmeHTTPPrefix,
			},
		},
		Action: &envoy_config_route_v3.Route_Route{
			Route: &envoy_config_route_v3.RouteAction{
				ClusterSpecifier: &envoy_config_route_v3.RouteAction_Cluster{
					Cluster: acmeHTTPClusterName,
				},
			},
		},
	}
}

// Pomerium implements the ACME TLS-ALPN protocol by adding a filter chain to the main HTTPS listener
// that matches the acme-tls/1 application protocol on incoming requests and forwards them to a listener
// started in the `autocert` package. The proxying is done using TCP so that the Go listener can terminate
// the TLS connection using the certmagic package.

const (
	acmeTLSALPNApplicationProtocol = "acme-tls/1"
	acmeTLSALPNClusterName         = "pomerium-acme-tls-alpn"
)

func (b *Builder) buildACMETLSALPNCluster(
	cfg *config.Config,
) *envoy_config_cluster_v3.Cluster {
	port, _ := strconv.ParseUint(cfg.ACMETLSALPNPort, 10, 32)
	return &envoy_config_cluster_v3.Cluster{
		Name: acmeTLSALPNClusterName,
		LoadAssignment: &envoy_config_endpoint_v3.ClusterLoadAssignment{
			ClusterName: acmeTLSALPNClusterName,
			Endpoints: []*envoy_config_endpoint_v3.LocalityLbEndpoints{{
				LbEndpoints: []*envoy_config_endpoint_v3.LbEndpoint{{
					HostIdentifier: &envoy_config_endpoint_v3.LbEndpoint_Endpoint{
						Endpoint: &envoy_config_endpoint_v3.Endpoint{
							Address: buildTCPAddress("127.0.0.1", uint32(port)),
						},
					},
				}},
			}},
		},
	}
}

func (b *Builder) buildACMETLSALPNFilterChain() *envoy_config_listener_v3.FilterChain {
	return &envoy_config_listener_v3.FilterChain{
		FilterChainMatch: &envoy_config_listener_v3.FilterChainMatch{
			ApplicationProtocols: []string{acmeTLSALPNApplicationProtocol},
		},
		Filters: []*envoy_config_listener_v3.Filter{
			TCPProxyFilter(acmeTLSALPNClusterName),
		},
	}
}
