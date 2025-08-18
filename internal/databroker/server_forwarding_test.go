package databroker_test

import (
	"encoding/base64"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/trace/noop"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/testing/protocmp"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/pomerium/pomerium/config"
	"github.com/pomerium/pomerium/internal/databroker"
	"github.com/pomerium/pomerium/internal/testutil"
	"github.com/pomerium/pomerium/pkg/cryptutil"
	databrokerpb "github.com/pomerium/pomerium/pkg/grpc/databroker"
	"github.com/pomerium/pomerium/pkg/grpcutil"
)

func TestForwardingServer(t *testing.T) {
	t.Parallel()

	sharedKey := cryptutil.NewKey()

	t.Run("forwards requests", func(t *testing.T) {
		t.Parallel()

		srv1 := databroker.NewSecuredServer(databroker.NewBackendServer(noop.NewTracerProvider()))
		t.Cleanup(srv1.Stop)
		srv1.OnConfigChange(t.Context(), &config.Config{Options: &config.Options{
			SharedKey: base64.StdEncoding.EncodeToString(sharedKey),
		}})

		c1 := testutil.NewGRPCServer(t, func(s *grpc.Server) {
			databrokerpb.RegisterDataBrokerServiceServer(s, srv1)
		})

		srv2 := databroker.NewForwardingServer(c1)
		t.Cleanup(srv2.Stop)

		c2 := testutil.NewGRPCServer(t, func(s *grpc.Server) {
			databrokerpb.RegisterDataBrokerServiceServer(s, srv2)
		})

		ctx := t.Context()
		ctx, err := grpcutil.WithSignedJWT(ctx, sharedKey)
		require.NoError(t, err)
		res1, err := databrokerpb.NewDataBrokerServiceClient(c1).ServerInfo(ctx, new(emptypb.Empty))
		assert.NoError(t, err)
		res2, err := databrokerpb.NewDataBrokerServiceClient(c2).ServerInfo(ctx, new(emptypb.Empty))
		assert.NoError(t, err)
		assert.Empty(t, cmp.Diff(res1, res2, protocmp.Transform()))
	})
	t.Run("aborts too many forwards", func(t *testing.T) {
		srv1 := databroker.NewSecuredServer(databroker.NewBackendServer(noop.NewTracerProvider()))
		t.Cleanup(srv1.Stop)
		srv1.OnConfigChange(t.Context(), &config.Config{Options: &config.Options{
			SharedKey: base64.StdEncoding.EncodeToString(sharedKey),
		}})
		c1 := testutil.NewGRPCServer(t, func(s *grpc.Server) {
			databrokerpb.RegisterDataBrokerServiceServer(s, srv1)
		})

		srv2 := databroker.NewForwardingServer(c1)
		t.Cleanup(srv2.Stop)
		c2 := testutil.NewGRPCServer(t, func(s *grpc.Server) {
			databrokerpb.RegisterDataBrokerServiceServer(s, srv2)
		})

		srv3 := databroker.NewForwardingServer(c2)
		t.Cleanup(srv3.Stop)
		c3 := testutil.NewGRPCServer(t, func(s *grpc.Server) {
			databrokerpb.RegisterDataBrokerServiceServer(s, srv3)
		})

		ctx := t.Context()
		ctx, err := grpcutil.WithSignedJWT(ctx, sharedKey)
		require.NoError(t, err)
		res, err := databrokerpb.NewDataBrokerServiceClient(c3).ServerInfo(ctx, new(emptypb.Empty))
		assert.Error(t, err)
		assert.Equal(t, codes.FailedPrecondition, status.Code(err))
		assert.Nil(t, res)
	})
}
