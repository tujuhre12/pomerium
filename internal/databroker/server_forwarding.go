package databroker

import (
	"context"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/pomerium/pomerium/config"
	"github.com/pomerium/pomerium/pkg/grpc/databroker"
	"github.com/pomerium/pomerium/pkg/grpc/registry"
	"github.com/pomerium/pomerium/pkg/grpcutil"
)

const maxForwards = 1

type forwardingServer struct {
	conn *grpc.ClientConn
}

// NewForwardingServer creates a new server that forwards all requests to another server.
func NewForwardingServer(conn *grpc.ClientConn) Server {
	return &forwardingServer{conn: conn}
}

func (srv *forwardingServer) AcquireLease(ctx context.Context, req *databroker.AcquireLeaseRequest) (*databroker.AcquireLeaseResponse, error) {
	return forwardUnary(ctx, func(ctx context.Context) (*databroker.AcquireLeaseResponse, error) {
		return databroker.NewDataBrokerServiceClient(srv.conn).AcquireLease(ctx, req)
	})
}

func (srv *forwardingServer) Get(ctx context.Context, req *databroker.GetRequest) (*databroker.GetResponse, error) {
	return forwardUnary(ctx, func(ctx context.Context) (*databroker.GetResponse, error) {
		return databroker.NewDataBrokerServiceClient(srv.conn).Get(ctx, req)
	})
}

func (srv *forwardingServer) List(ctx context.Context, req *registry.ListRequest) (*registry.ServiceList, error) {
	return forwardUnary(ctx, func(ctx context.Context) (*registry.ServiceList, error) {
		return registry.NewRegistryClient(srv.conn).List(ctx, req)
	})
}

func (srv *forwardingServer) ListTypes(ctx context.Context, req *emptypb.Empty) (*databroker.ListTypesResponse, error) {
	return forwardUnary(ctx, func(ctx context.Context) (*databroker.ListTypesResponse, error) {
		return databroker.NewDataBrokerServiceClient(srv.conn).ListTypes(ctx, req)
	})
}

func (srv *forwardingServer) Patch(ctx context.Context, req *databroker.PatchRequest) (*databroker.PatchResponse, error) {
	return forwardUnary(ctx, func(ctx context.Context) (*databroker.PatchResponse, error) {
		return databroker.NewDataBrokerServiceClient(srv.conn).Patch(ctx, req)
	})
}

func (srv *forwardingServer) Put(ctx context.Context, req *databroker.PutRequest) (*databroker.PutResponse, error) {
	return forwardUnary(ctx, func(ctx context.Context) (*databroker.PutResponse, error) {
		return databroker.NewDataBrokerServiceClient(srv.conn).Put(ctx, req)
	})
}

func (srv *forwardingServer) Query(ctx context.Context, req *databroker.QueryRequest) (*databroker.QueryResponse, error) {
	return forwardUnary(ctx, func(ctx context.Context) (*databroker.QueryResponse, error) {
		return databroker.NewDataBrokerServiceClient(srv.conn).Query(ctx, req)
	})
}

func (srv *forwardingServer) ReleaseLease(ctx context.Context, req *databroker.ReleaseLeaseRequest) (*emptypb.Empty, error) {
	return forwardUnary(ctx, func(ctx context.Context) (*emptypb.Empty, error) {
		return databroker.NewDataBrokerServiceClient(srv.conn).ReleaseLease(ctx, req)
	})
}

func (srv *forwardingServer) RenewLease(ctx context.Context, req *databroker.RenewLeaseRequest) (*emptypb.Empty, error) {
	return forwardUnary(ctx, func(ctx context.Context) (*emptypb.Empty, error) {
		return databroker.NewDataBrokerServiceClient(srv.conn).RenewLease(ctx, req)
	})
}

func (srv *forwardingServer) Report(ctx context.Context, req *registry.RegisterRequest) (*registry.RegisterResponse, error) {
	return forwardUnary(ctx, func(ctx context.Context) (*registry.RegisterResponse, error) {
		return registry.NewRegistryClient(srv.conn).Report(ctx, req)
	})
}

func (srv *forwardingServer) ServerInfo(ctx context.Context, req *emptypb.Empty) (*databroker.ServerInfoResponse, error) {
	return forwardUnary(ctx, func(ctx context.Context) (*databroker.ServerInfoResponse, error) {
		return databroker.NewDataBrokerServiceClient(srv.conn).ServerInfo(ctx, req)
	})
}

func (srv *forwardingServer) SetOptions(ctx context.Context, req *databroker.SetOptionsRequest) (*databroker.SetOptionsResponse, error) {
	return forwardUnary(ctx, func(ctx context.Context) (*databroker.SetOptionsResponse, error) {
		return databroker.NewDataBrokerServiceClient(srv.conn).SetOptions(ctx, req)
	})
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

func checkMaxForwards(ctx context.Context) error {
	forwardedFor := grpcutil.ForwardedForFromIncoming(ctx)
	if len(forwardedFor) >= maxForwards {
		return status.Errorf(codes.FailedPrecondition, "request forwarded for %s which exceeds the maximum number of forwards (%d)",
			strings.Join(forwardedFor, ","), maxForwards)
	}
	return nil
}

func forwardMetadata(ctx context.Context) context.Context {
	if inMD, ok := metadata.FromIncomingContext(ctx); ok {
		outMD, ok := metadata.FromOutgoingContext(ctx)
		if !ok {
			outMD = make(metadata.MD)
		}
		for k, vs := range inMD {
			outMD.Append(k, vs...)
		}
		ctx = metadata.NewOutgoingContext(ctx, outMD)
	}

	forwardedFor := grpcutil.ForwardedForFromIncoming(ctx)
	if p, ok := peer.FromContext(ctx); ok {
		forwardedFor = append(forwardedFor, p.Addr.String())
	} else {
		forwardedFor = append(forwardedFor, "127.0.0.1")
	}
	ctx = grpcutil.WithOutgoingForwardedFor(ctx, forwardedFor)

	return ctx
}

func forwardUnary[T any](ctx context.Context, fn func(ctx context.Context) (T, error)) (T, error) {
	if err := checkMaxForwards(ctx); err != nil {
		var zero T
		return zero, err
	}
	ctx = forwardMetadata(ctx)
	return fn(ctx)
}

func forwardStream[T any](
	serverStream grpc.ServerStreamingServer[T],
	getClientStream func(context.Context) (grpc.ServerStreamingClient[T], error),
) error {
	ctx, cancel := context.WithCancel(serverStream.Context())
	defer cancel()

	if err := checkMaxForwards(ctx); err != nil {
		return err
	}

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
