package v2

import (
	"github.com/elastic/apm-server/beater/request"
	"github.com/elastic/elastic-agent-libs/monitoring"
)

var (
	monitoringKeys = append(request.DefaultResultIDs,
		request.IDResponseErrorsRateLimit,
		request.IDResponseErrorsTimeout,
		request.IDResponseErrorsUnauthorized,
	)
	gRPCMetricsRegistry      = monitoring.Default.NewRegistry("apm-server.intakev2.grpc.metrics")
	gRPCMetricsMonitoringMap = request.MonitoringMapForRegistry(gRPCMetricsRegistry, monitoringKeys)

	// GRPCRegistryMonitoringMaps provides mappings from the fully qualified gRPC
	// method name to its respective monitoring map.
	GRPCRegistryMonitoringMaps = map[string]map[request.ResultID]*monitoring.Int{
		"/" + IntakeService_ServiceDesc.ServiceName + "/" + IntakeService_ServiceDesc.Streams[0].StreamName: gRPCMetricsMonitoringMap,
	}
)
