package databroker

import (
	"context"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/pomerium/pomerium/config"
	"github.com/pomerium/pomerium/pkg/grpc/databroker"
	"github.com/pomerium/pomerium/pkg/grpc/registry"
)

type forwardingServer struct {
	conn *grpc.ClientConn
}

// NewForwardingServer creates a new server that forwards all requests to another server.
func NewForwardingServer(conn *grpc.ClientConn) Server {
	return &forwardingServer{conn: conn}
}

func (srv *forwardingServer) AcquireLease(ctx context.Context, req *databroker.AcquireLeaseRequest) (*databroker.AcquireLeaseResponse, error) {
	return databroker.NewDataBrokerServiceClient(srv.conn).AcquireLease(forwardMetadata(ctx), req)
}

func (srv *forwardingServer) Get(ctx context.Context, req *databroker.GetRequest) (*databroker.GetResponse, error) {
	return databroker.NewDataBrokerServiceClient(srv.conn).Get(forwardMetadata(ctx), req)
}

func (srv *forwardingServer) List(ctx context.Context, req *registry.ListRequest) (*registry.ServiceList, error) {
	return registry.NewRegistryClient(srv.conn).List(forwardMetadata(ctx), req)
}

func (srv *forwardingServer) ListTypes(ctx context.Context, req *emptypb.Empty) (*databroker.ListTypesResponse, error) {
	return databroker.NewDataBrokerServiceClient(srv.conn).ListTypes(forwardMetadata(ctx), req)
}

func (srv *forwardingServer) Patch(ctx context.Context, req *databroker.PatchRequest) (*databroker.PatchResponse, error) {
	return databroker.NewDataBrokerServiceClient(srv.conn).Patch(forwardMetadata(ctx), req)
}

func (srv *forwardingServer) Put(ctx context.Context, req *databroker.PutRequest) (*databroker.PutResponse, error) {
	return databroker.NewDataBrokerServiceClient(srv.conn).Put(forwardMetadata(ctx), req)
}

func (srv *forwardingServer) Query(ctx context.Context, req *databroker.QueryRequest) (*databroker.QueryResponse, error) {
	return databroker.NewDataBrokerServiceClient(srv.conn).Query(forwardMetadata(ctx), req)
}

func (srv *forwardingServer) ReleaseLease(ctx context.Context, req *databroker.ReleaseLeaseRequest) (*emptypb.Empty, error) {
	return databroker.NewDataBrokerServiceClient(srv.conn).ReleaseLease(forwardMetadata(ctx), req)
}

func (srv *forwardingServer) RenewLease(ctx context.Context, req *databroker.RenewLeaseRequest) (*emptypb.Empty, error) {
	return databroker.NewDataBrokerServiceClient(srv.conn).RenewLease(forwardMetadata(ctx), req)
}

func (srv *forwardingServer) Report(ctx context.Context, req *registry.RegisterRequest) (*registry.RegisterResponse, error) {
	return registry.NewRegistryClient(srv.conn).Report(forwardMetadata(ctx), req)
}

func (srv *forwardingServer) ServerInfo(ctx context.Context, req *emptypb.Empty) (*databroker.ServerInfoResponse, error) {
	return databroker.NewDataBrokerServiceClient(srv.conn).ServerInfo(forwardMetadata(ctx), req)
}

func (srv *forwardingServer) SetOptions(ctx context.Context, req *databroker.SetOptionsRequest) (*databroker.SetOptionsResponse, error) {
	return databroker.NewDataBrokerServiceClient(srv.conn).SetOptions(forwardMetadata(ctx), req)
}

func (srv *forwardingServer) Sync(req *databroker.SyncRequest, stream grpc.ServerStreamingServer[databroker.SyncResponse]) error {
	return forwardStream(stream, func(ctx context.Context) (grpc.ServerStreamingClient[databroker.SyncResponse], error) {
		return databroker.NewDataBrokerServiceClient(srv.conn).Sync(ctx, req)
	})
}

func (srv *forwardingServer) SyncLatest(req *databroker.SyncLatestRequest, stream grpc.ServerStreamingServer[databroker.SyncLatestResponse]) error {
	return forwardStream(stream, func(ctx context.Context) (grpc.ServerStreamingClient[databroker.SyncLatestResponse], error) {
		return databroker.NewDataBrokerServiceClient(srv.conn).SyncLatest(ctx, req)
	})
}

func (srv *forwardingServer) Watch(req *registry.ListRequest, stream grpc.ServerStreamingServer[registry.ServiceList]) error {
	return forwardStream(stream, func(ctx context.Context) (grpc.ServerStreamingClient[registry.ServiceList], error) {
		return registry.NewRegistryClient(srv.conn).Watch(ctx, req)
	})
}

func (srv *forwardingServer) Stop() {}

func (srv *forwardingServer) OnConfigChange(_ context.Context, _ *config.Config) {}

func forwardMetadata(ctx context.Context) context.Context {
	inMD, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return ctx
	}

	outMD, ok := metadata.FromOutgoingContext(ctx)
	if !ok {
		outMD = make(metadata.MD)
	}
	for k, vs := range inMD {
		outMD.Append(k, vs...)
	}

	return metadata.NewOutgoingContext(ctx, outMD)
}

func forwardStream[T any](
	serverStream grpc.ServerStreamingServer[T],
	getClientStream func(context.Context) (grpc.ServerStreamingClient[T], error),
) error {
	ctx, cancel := context.WithCancel(serverStream.Context())
	defer cancel()

	clientStream, err := getClientStream(forwardMetadata(ctx))
	if err != nil {
		return err
	}

	for {
		msg, err := clientStream.Recv()
		if err != nil {
			return err
		}

		err = serverStream.Send(msg)
		if err != nil {
			return err
		}
	}
}
