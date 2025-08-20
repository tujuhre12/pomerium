package health

import (
	"fmt"
	"strings"
	"sync"

	"github.com/samber/lo"
)

var (
	defaultCheckMu = &sync.RWMutex{}
	defaultChecks  = map[Check]struct{}{}
)

type Check string

type CheckV2 struct {
	Path     string
	Parent   *CheckV2
	Children map[string]*CheckV2
}

func newCheck(
	name string,
	parent *CheckV2,
) *CheckV2 {
	c := &CheckV2{
		Path:     name,
		Parent:   parent,
		Children: make(map[string]*CheckV2),
	}
	if parent != nil {
		fullPath := parent.Path + "." + name
		c.Path = fullPath
		parent.Children[name] = c
	}
	return c
}

var (
	Databroker            = newCheck("databroker", nil)
	DatabrokerConfig      = newCheck("config", Databroker)
	DatabrokerConfigBuild = newCheck("build", DatabrokerConfig)

	Storage                     = newCheck("storage", nil)
	StorageBackend2             = newCheck("backend", Storage)
	StorageBackendPing          = newCheck("ping", StorageBackend2)
	StorageBackendNotifications = newCheck("notifications", StorageBackend2)
	StorageBackendCleanup2      = newCheck("cleanup", StorageBackend2)
)

func ReportOkV2(check *CheckV2) {
	provider.ReportOK(Check(check.Path))
}

func ReportErrorV2(check *CheckV2, err error) {
	provider.ReportError(
		Check(check.Path),
		err,
	)
}

type Status int

const (
	StatusStarting Status = iota
	StatusRunning
	StatusTerminating
	StatusError
)

func (s Status) String() string {
	v := "unkown"
	switch s {
	case StatusStarting:
		v = "starting"
	case StatusRunning:
		v = "running"
	case StatusTerminating:
		v = "terminating"
	case StatusError:
		v = "error"
	}
	return strings.ToUpper(v)
}

func SetDefaultExpected(
	checks ...Check,
) {
	defaultCheckMu.Lock()
	defer defaultCheckMu.Unlock()
	defaultChecks = lo.Associate(checks, func(check Check) (Check, struct{}) {
		return check, struct{}{}
	})
}

func getDefaultExpected() map[Check]struct{} {
	defaultCheckMu.RLock()
	defer defaultCheckMu.RUnlock()
	return defaultChecks
}

func init() {
	SetDefaultExpected(
		DatabrokerBuildConfig,
		DatabrokerInitialSync,
		CollectAndSendTelemetry,
		StorageBackend,
		XDSCluster,
		XDSListener,
		XDSRouteConfiguration,
		XDSOther,
		RoutesReachable,
	)
}

const (
	AuthenticateService = Check("authenticate.service")

	AuthorizationService = Check("authorize.service")

	ProxyService = Check("proxy.service")

	// BuildDatabrokerConfig checks whether the Databroker config was applied
	DatabrokerBuildConfig = Check("config.databroker.build")
	// DatabrokerInitialSync checks whether the initial sync was applied
	DatabrokerInitialSync = Check("databroker.sync.initial")
	// CollectAndSendTelemetry checks whether telemetry was collected and sent
	CollectAndSendTelemetry = Check("zero.telemetry.collect-and-send")
	// StorageBackend checks whether the storage backend is healthy
	StorageBackend = Check("storage.backend")

	StorageBackendCleanup = Check("storage.backend.cleanup")

	StorageBackendNotification = Check("storage.backend.notifications")

	// XDSCluster checks whether the XDS Cluster resources were applied
	XDSCluster = Check("xds.cluster")
	// XDSListener checks whether the XDS Listener resources were applied
	XDSListener = Check("xds.listener")
	// XDSRouteConfiguration checks whether the XDS RouteConfiguration resources were applied
	XDSRouteConfiguration = Check("xds.route-configuration")
	// XDSOther is a catch-all for other XDS resources
	XDSOther = Check("xds.other")
	// ZeroBootstrapConfigSave checks whether the Zero bootstrap config was saved
	ZeroBootstrapConfigSave = Check("zero.bootstrap-config.save")
	// ZeroConnect checks whether the Zero Connect service is connected
	ZeroConnect = Check("zero.connect")
	// RoutesReachable checks whether all referenced routes can be resolved to this instance
	RoutesReachable = Check("routes.reachable")
)

// ZeroResourceBundle checks whether the Zero resource bundle was applied
func ZeroResourceBundle(bundleID string) Check {
	return Check(fmt.Sprintf("zero.resource-bundle.%s", bundleID))
}
