package controller

import (
	"context"
	"sync"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/pomerium/pomerium/internal/log"
	sdk "github.com/pomerium/pomerium/internal/zero/api"
	"github.com/pomerium/pomerium/pkg/grpc/databroker"
	"github.com/pomerium/pomerium/pkg/grpc/session"
	"github.com/pomerium/pomerium/pkg/grpc/user"
	"github.com/pomerium/pomerium/pkg/protoutil"
	"github.com/pomerium/pomerium/pkg/zero/cluster"
)

type usageReporterRecord struct {
	userID          string
	userDisplayName string
	userEmail       string
	accessedAt      time.Time
}

type usageReporter struct {
	api *sdk.API

	mu       sync.Mutex
	byUserID map[string]usageReporterRecord
}

func newUsageReporter(api *sdk.API) *usageReporter {
	return &usageReporter{
		api:      api,
		byUserID: make(map[string]usageReporterRecord),
	}
}

func (ur *usageReporter) report(ctx context.Context, records []usageReporterRecord) {
	// if there were no updates there's nothing to do
	if len(records) == 0 {
		return
	}

	req := cluster.ReportUsageRequest{}
	for _, record := range records {
		req.Users = append(req.Users, cluster.ReportUsageUser{
			AccessedAt:  record.accessedAt,
			DisplayName: record.userDisplayName,
			Email:       record.userEmail,
			Id:          record.userID,
		})
	}
	err := ur.api.ReportUsage(ctx, req)
	if err != nil {
		log.Error(ctx).Err(err).Msg("error reporting usage to api")
	}
}

func (ur *usageReporter) run(ctx context.Context, client databroker.DataBrokerServiceClient) error {
	eg, ctx := errgroup.WithContext(ctx)
	eg.Go(func() error {
		return databroker.NewSyncer("zero-usage-reporter-sessions", usageReporterSessionSyncerHandler{
			usageReporter: ur,
			client:        client,
		}, databroker.WithTypeURL(protoutil.GetTypeURL(new(session.Session)))).Run(ctx)
	})
	eg.Go(func() error {
		return databroker.NewSyncer("zero-usage-reporter-users", usageReporterUserSyncerHandler{
			usageReporter: ur,
			client:        client,
		}, databroker.WithTypeURL(protoutil.GetTypeURL(new(user.User)))).Run(ctx)
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
