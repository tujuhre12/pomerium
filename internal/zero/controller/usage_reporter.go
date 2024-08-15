package controller

import (
	"context"
	"sync"
	"time"

	"golang.org/x/sync/errgroup"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/pomerium/pomerium/internal/log"
	"github.com/pomerium/pomerium/pkg/grpc/databroker"
	"github.com/pomerium/pomerium/pkg/grpc/session"
	"github.com/pomerium/pomerium/pkg/grpc/user"
	"github.com/pomerium/pomerium/pkg/zero/connect"
)

type usageReporterRecord struct {
	userID          string
	userDisplayName string
	userEmail       string
	accessedAt      time.Time
}

type usageReporter struct {
	connectClient connect.ConnectClient

	mu       sync.Mutex
	byUserID map[string]usageReporterRecord
}

func newUsageReporter(connectClient connect.ConnectClient) *usageReporter {
	return &usageReporter{
		connectClient: connectClient,
		byUserID:      make(map[string]usageReporterRecord),
	}
}

func (ur *usageReporter) report(ctx context.Context, records []usageReporterRecord) {
	req := &connect.ReportUsageRequest{}
	for _, record := range records {
		req.Users = append(req.Users, &connect.ReportUsageRequest_User{
			AccessedAt:  timestamppb.New(record.accessedAt),
			DisplayName: record.userDisplayName,
			Email:       record.userEmail,
			Id:          record.userID,
		})
	}
	_, err := ur.connectClient.ReportUsage(ctx, req)
	if err != nil {
		log.Error(ctx).Err(err).Msg("error reporting usage to connect service")
	}
}

func (ur *usageReporter) run(ctx context.Context, client databroker.DataBrokerServiceClient) error {
	eg, ctx := errgroup.WithContext(ctx)
	eg.Go(func() error {
		return databroker.NewSyncer("zero-usage-reporter-sessions", usageReporterSessionSyncerHandler{
			usageReporter: ur,
			client:        client,
		}).Run(ctx)
	})
	eg.Go(func() error {
		return databroker.NewSyncer("zero-usage-reporter-users", usageReporterUserSyncerHandler{
			usageReporter: ur,
			client:        client,
		}).Run(ctx)
	})
	return eg.Wait()
}

type usageReporterSessionSyncerHandler struct {
	*usageReporter
	client databroker.DataBrokerServiceClient
}

func (h usageReporterSessionSyncerHandler) GetDataBrokerServiceClient() databroker.DataBrokerServiceClient {
	return h.client
}

func (h usageReporterSessionSyncerHandler) ClearRecords(_ context.Context) {
	// do nothing
}

func (h usageReporterSessionSyncerHandler) UpdateRecords(ctx context.Context, _ uint64, records []*databroker.Record) {
	var updates []usageReporterRecord

	h.mu.Lock()
	for _, record := range records {
		// ignore deleted records
		if record.GetDeletedAt() != nil {
			continue
		}

		// get the session data
		data, err := record.GetData().UnmarshalNew()
		if err != nil {
			log.Error(ctx).Err(err).Msg("error unmarshaling session data")
			continue
		}

		s, ok := data.(*session.Session)
		if !ok {
			log.Error(ctx).Msg("unexpected non-session data stored in session record")
			continue
		}

		if s.GetUserId() == "" {
			// ignore sessions not associated with a user
			continue
		}

		// create or update usage records in the collection
		r := h.byUserID[s.GetUserId()]
		r.accessedAt = s.GetAccessedAt().AsTime()
		r.userID = s.GetUserId()
		h.byUserID[r.userID] = r
		updates = append(updates, r)
	}
	h.mu.Unlock()

	h.report(ctx, updates)
}

type usageReporterUserSyncerHandler struct {
	*usageReporter
	client databroker.DataBrokerServiceClient
}

func (h usageReporterUserSyncerHandler) GetDataBrokerServiceClient() databroker.DataBrokerServiceClient {
	return h.client
}

func (h usageReporterUserSyncerHandler) ClearRecords(_ context.Context) {
	h.mu.Lock()
	clear(h.byUserID)
	h.mu.Unlock()
}

func (h usageReporterUserSyncerHandler) UpdateRecords(ctx context.Context, _ uint64, records []*databroker.Record) {
	var updates []usageReporterRecord

	h.mu.Lock()
	for _, record := range records {
		// delete the associated user if the record is deleted
		if record.GetDeletedAt() != nil {
			delete(h.byUserID, record.GetId())
			continue
		}

		// get the user data
		data, err := record.GetData().UnmarshalNew()
		if err != nil {
			log.Error(ctx).Err(err).Msg("error unmarshaling user data")
			continue
		}

		u, ok := data.(*user.User)
		if !ok {
			log.Error(ctx).Msg("unexpected non-user data stored in user record")
			continue
		}

		// create or update usage records in the collection
		r := h.byUserID[u.GetId()]
		if r.accessedAt.Before(record.GetModifiedAt().AsTime()) {
			r.accessedAt = record.GetModifiedAt().AsTime()
		}
		r.userDisplayName = u.GetName()
		r.userEmail = u.GetEmail()
		r.userID = u.GetId()
		h.byUserID[r.userID] = r
		updates = append(updates, r)
	}
	h.mu.Unlock()

	h.report(ctx, updates)
}
