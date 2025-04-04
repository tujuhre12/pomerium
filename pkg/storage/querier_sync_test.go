package storage_test

import (
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/trace/noop"
	grpc "google.golang.org/grpc"
	"google.golang.org/protobuf/testing/protocmp"

	"github.com/pomerium/pomerium/internal/databroker"
	"github.com/pomerium/pomerium/internal/testutil"
	databrokerpb "github.com/pomerium/pomerium/pkg/grpc/databroker"
	"github.com/pomerium/pomerium/pkg/protoutil"
	"github.com/pomerium/pomerium/pkg/storage"
)

func TestSyncQuerier(t *testing.T) {
	t.Parallel()

	ctx := testutil.GetContext(t, 10*time.Minute)
	cc := testutil.NewGRPCServer(t, func(srv *grpc.Server) {
		databrokerpb.RegisterDataBrokerServiceServer(srv, databroker.New(ctx, noop.NewTracerProvider()))
	})
	t.Cleanup(func() { cc.Close() })

	client := databrokerpb.NewDataBrokerServiceClient(cc)

	q1r1 := &databrokerpb.Record{
		Type: "t1",
		Id:   "r1",
		Data: protoutil.ToAny("q1"),
	}
	q1r2 := &databrokerpb.Record{
		Type: "t2",
		Id:   "r2",
		Data: protoutil.ToAny("q2"),
	}
	q1 := storage.NewStaticQuerier(q1r1, q1r2)

	q2r1 := &databrokerpb.Record{
		Type: "t1",
		Id:   "r1",
		Data: protoutil.ToAny("q2"),
	}
	_, err := client.Put(ctx, &databrokerpb.PutRequest{
		Records: []*databrokerpb.Record{q2r1},
	})
	require.NoError(t, err)

	q2r2 := &databrokerpb.Record{
		Type: "t1",
		Id:   "r2",
		Data: protoutil.ToAny("q2"),
	}

	q2 := storage.NewSyncQuerier(client, "t1", q1)
	t.Cleanup(q2.Stop)

	assert.EventuallyWithT(t, func(c *assert.CollectT) {
		res, err := q2.Query(ctx, &databrokerpb.QueryRequest{
			Type: "t1",
			Filter: newStruct(t, map[string]any{
				"id": "r1",
			}),
			Limit: 1,
		})
		if assert.NoError(c, err) && assert.Len(c, res.Records, 1) {
			assert.Empty(c, cmp.Diff(q2r1.Data, res.Records[0].Data, protocmp.Transform()))
		}
	}, time.Second*10, time.Millisecond*50, "should sync records")

	res, err := q2.Query(ctx, &databrokerpb.QueryRequest{
		Type: "t2",
		Filter: newStruct(t, map[string]any{
			"id": "r2",
		}),
		Limit: 1,
	})
	if assert.NoError(t, err) && assert.Len(t, res.Records, 1) {
		assert.Empty(t, cmp.Diff(q1r2.Data, res.Records[0].Data, protocmp.Transform()),
			"should use fallback querier for other record types")
	}

	_, err = client.Put(ctx, &databrokerpb.PutRequest{
		Records: []*databrokerpb.Record{q2r2},
	})
	require.NoError(t, err)

	assert.EventuallyWithT(t, func(c *assert.CollectT) {
		res, err := q2.Query(ctx, &databrokerpb.QueryRequest{
			Type: "t1",
			Filter: newStruct(t, map[string]any{
				"id": "r2",
			}),
			Limit: 1,
		})
		if assert.NoError(c, err) && assert.Len(c, res.Records, 1) {
			assert.Empty(c, cmp.Diff(q2r2.Data, res.Records[0].Data, protocmp.Transform()))
		}
	}, time.Second*10, time.Millisecond*50, "should pick up changes")
}
