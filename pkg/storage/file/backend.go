package file

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"sync"
	"time"

	"github.com/cockroachdb/pebble/v2"
	"github.com/hashicorp/go-set/v3"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/pomerium/pomerium/internal/signal"
	"github.com/pomerium/pomerium/pkg/grpc/databroker"
	databrokerpb "github.com/pomerium/pomerium/pkg/grpc/databroker"
	"github.com/pomerium/pomerium/pkg/pebbleutil"
	"github.com/pomerium/pomerium/pkg/storage"
)

type (
	reader interface {
		pebble.Reader
	}
	writer interface {
		pebble.Writer
	}
	readerWriter interface {
		reader
		writer
	}
)

type Backend struct {
	dsn            string
	onRecordChange *signal.Signal

	mu sync.RWMutex
	db *pebble.DB
}

func New(dsn string) *Backend {
	return &Backend{
		dsn:            dsn,
		onRecordChange: signal.New(),
	}
}

// Close closes the backend.
func (backend *Backend) Close() error {
	db, err := backend.init()
	if err != nil {
		return fmt.Errorf("pebble: error initializing: %w", err)
	}

	backend.mu.Lock()
	defer backend.mu.Unlock()

	return db.Close()
}

// Get is used to retrieve a record.
func (backend *Backend) Get(
	_ context.Context,
	recordType, recordID string,
) (*databrokerpb.Record, error) {
	db, err := backend.init()
	if err != nil {
		return nil, fmt.Errorf("pebble: error initializing: %w", err)
	}

	backend.mu.RLock()
	defer backend.mu.RUnlock()

	record, err := recordKeySpace.get(db, recordType, recordID)
	if isNotFound(err) {
		err = storage.ErrNotFound
	} else if err != nil {
		err = fmt.Errorf("pebble: error getting record: %w", err)
	}
	if err != nil {
		return nil, err
	}

	return record, err
}

// GetOptions gets the options for a type.
func (backend *Backend) GetOptions(
	_ context.Context,
	recordType string,
) (options *databrokerpb.Options, err error) {
	db, err := backend.init()
	if err != nil {
		return nil, fmt.Errorf("pebble: error initializing: %w", err)
	}

	backend.mu.RLock()
	defer backend.mu.RUnlock()

	options, err = optionsKeySpace.get(db, recordType)
	if isNotFound(err) {
		options = new(databrokerpb.Options)
	} else {
		return nil, fmt.Errorf("pebble: error getting options: %w", err)
	}

	return options, err
}

// Lease acquires a lease, or renews an existing one. If the lease is acquired true is returned.
func (backend *Backend) Lease(
	_ context.Context,
	leaseName, leaseID string,
	ttl time.Duration,
) (acquired bool, err error) {
	db, err := backend.init()
	if err != nil {
		return false, fmt.Errorf("pebble: error initializing: %w", err)
	}

	backend.mu.Lock()
	defer backend.mu.Unlock()

	// get the current lease
	currentLeaseID, expiresAt, err := leaseKeySpace.get(db, leaseName)
	if isNotFound(err) {
		// lease doesn't exist yet, so acquire the lease
	} else if err != nil {
		return false, fmt.Errorf("pebble: error getting lease: %w", err)
	} else if currentLeaseID == leaseID || expiresAt.Before(time.Now()) {
		// leaes is either for this id, or has expired, so acquire the lease
	} else {
		// don't acquire the lease because someone else has it
		return false, nil
	}
	err = leaseKeySpace.set(db, leaseName, leaseID, time.Now().Add(ttl))
	if err != nil {
		return false, fmt.Errorf("pebble: error setting lease: %w", err)
	}

	return true, err
}

// ListTypes lists all the known record types.
func (backend *Backend) ListTypes(
	_ context.Context,
) (recordTypes []string, err error) {
	db, err := backend.init()
	if err != nil {
		return nil, fmt.Errorf("pebble: error initializing: %w", err)
	}

	backend.mu.RLock()
	defer backend.mu.RUnlock()

	for recordType, err := range recordKeySpace.iterateTypes(db) {
		if err != nil {
			return nil, fmt.Errorf("error iterating record types from pebble: %w", err)
		}
		recordTypes = append(recordTypes, recordType)
	}

	return recordTypes, err
}

// Put is used to insert or update records.
func (backend *Backend) Put(
	_ context.Context,
	records []*databroker.Record,
) (serverVersion uint64, err error) {
	db, err := backend.init()
	if err != nil {
		return 0, fmt.Errorf("pebble: error initializing: %w", err)
	}

	backend.mu.Lock()
	defer backend.mu.Unlock()

	serverVersion, err = metadataKeySpace.getServerVersion(db)
	if err != nil {
		return 0, fmt.Errorf("pebble: error getting server version: %w", err)
	}

	batch := db.NewIndexedBatch()

	// update records
	recordTypes := set.New[string](len(records))
	for i := range records {
		recordTypes.Insert(records[i].GetType())
		records[i] = proto.CloneOf(records[i])
		err = backend.updateRecordLocked(batch, records[i])
		if err != nil {
			return 0, fmt.Errorf("pebble: error updating record (type=%s id=%s): %w",
				records[i].GetType(), records[i].GetId(), err)
		}
	}

	// enforce options
	for recordType := range recordTypes.Items() {
		err = backend.enforceOptionsLocked(batch, recordType)
		if err != nil {
			return 0, fmt.Errorf("pebble: error enforcing options (type=%s): %w",
				recordType, err)
		}
	}

	err = batch.Commit(nil)
	if err != nil {
		return 0, fmt.Errorf("pebble: error committing batch for update: %w", err)
	}

	return serverVersion, err
}

// Patch is used to update specific fields of existing records.
func (backend *Backend) Patch(
	_ context.Context,
	records []*databroker.Record,
	fields *fieldmaskpb.FieldMask,
) (serverVersion uint64, patchedRecords []*databroker.Record, err error) {
	db, err := backend.init()
	if err != nil {
		return 0, nil, fmt.Errorf("pebble: error initializing: %w", err)
	}

	backend.mu.Lock()
	defer backend.mu.Unlock()

	serverVersion, err = metadataKeySpace.getServerVersion(db)
	if err != nil {
		return 0, nil, fmt.Errorf("pebble: error getting server version: %w", err)
	}

	batch := db.NewIndexedBatch()

	// update records
	recordTypes := set.New[string](len(records))
	patchedRecords = make([]*databroker.Record, len(records))
	for i := range records {
		recordTypes.Insert(records[i].GetType())
		patchedRecords[i] = proto.CloneOf(records[i])
		err = backend.patchRecordLocked(batch, patchedRecords[i], fields)
		if err != nil {
			return 0, nil, fmt.Errorf("pebble: error patching record (type=%s id=%s): %w",
				records[i].GetType(), records[i].GetId(), err)
		}
	}

	// enforce options
	for recordType := range recordTypes.Items() {
		err = backend.enforceOptionsLocked(batch, recordType)
		if err != nil {
			return 0, nil, fmt.Errorf("pebble: error enforcing options (type=%s): %w",
				recordType, err)
		}
	}

	err = batch.Commit(nil)
	if err != nil {
		return 0, nil, fmt.Errorf("pebble: error committing batch for patch: %w", err)
	}

	return serverVersion, patchedRecords, err
}

// SetOptions sets the options for a type.
func (backend *Backend) SetOptions(
	_ context.Context,
	recordType string,
	options *databroker.Options,
) error {
	db, err := backend.init()
	if err != nil {
		return fmt.Errorf("pebble: error initializing: %w", err)
	}

	backend.mu.Lock()
	defer backend.mu.Unlock()

	// if the options are empty, just delete them since we will return empty options on not found
	if proto.Equal(options, new(databrokerpb.Options)) {
		err = optionsKeySpace.delete(db, recordType)
	} else {
		err = optionsKeySpace.set(db, recordType, options)
	}
	if err != nil {
		return fmt.Errorf("pebble: error updating options: %w", err)
	}

	return nil
}

// Sync syncs record changes after the specified version.
func (backend *Backend) Sync(
	ctx context.Context,
	recordType string,
	serverVersion, recordVersion uint64,
	wait bool,
) storage.RecordIterator {
	return func(yield func(*databrokerpb.Record, error) bool) {
		db, err := backend.init()
		if err != nil {
			yield(nil, fmt.Errorf("pebble: error initializing: %w", err))
			return
		}

		backend.mu.RLock()
		currentServerVersion, err := metadataKeySpace.getServerVersion(db)
		backend.mu.RUnlock()
		if err != nil {
			yield(nil, fmt.Errorf("pebble: error getting server version: %w", err))
			return
		} else if serverVersion != currentServerVersion {
			yield(nil, storage.ErrInvalidServerVersion)
			return
		}

		changed := backend.onRecordChange.Bind()
		defer backend.onRecordChange.Unbind(changed)

		for {
			backend.mu.RLock()
			records, err := backend.listChangedRecordsAfterLocked(db, recordType, recordVersion)
			backend.mu.RUnlock()
			if err != nil {
				yield(nil, fmt.Errorf("pebble: error listing changed records: %w", err))
				return
			}

			if len(records) > 0 {
				for _, record := range records {
					if !yield(record, nil) {
						return
					}
				}
				continue
			}

			if !wait {
				break
			}

			select {
			case <-ctx.Done():
				yield(nil, context.Cause(ctx))
				return
			case <-changed:
			}
		}
	}
}

// SyncLatest syncs all the records.
func (backend *Backend) SyncLatest(
	ctx context.Context,
	recordType string,
	filter storage.FilterExpression,
) (serverVersion, recordVersion uint64, seq storage.RecordIterator, err error) {
	panic("not implemented")
}

func (backend *Backend) init() (*pebble.DB, error) {
	backend.mu.RLock()
	db := backend.db
	backend.mu.RUnlock()

	if db != nil {
		return db, nil
	}

	backend.mu.Lock()
	defer backend.mu.Unlock()

	db = backend.db
	if db != nil {
		return db, nil
	}

	u, err := url.Parse(backend.dsn)
	if err != nil {
		return nil, fmt.Errorf("invalid dsn, expected url: %w", err)
	}

	switch u.Scheme {
	case "memory":
		db = pebbleutil.MustOpenMemory(nil)
	case "file":
		db, err = pebbleutil.Open(u.Path, nil)
		if err != nil {
			return nil, fmt.Errorf("error opening pebble database at %s: %w", u.Path, err)
		}
	}

	err = migrate(db)
	if err != nil {
		return nil, fmt.Errorf("error migrating pebble database: %w", err)
	}

	backend.db = db
	return db, nil
}

func (backend *Backend) listChangedRecordsAfterLocked(
	r reader,
	recordType string,
	recordVersion uint64,
) ([]*databrokerpb.Record, error) {
	return nil, nil
}

func (backend *Backend) patchRecordLocked(
	rw readerWriter,
	record *databrokerpb.Record,
	fields *fieldmaskpb.FieldMask,
) error {
	existing, err := recordKeySpace.get(rw, record.GetType(), record.GetId())
	if isNotFound(err) {
		// skip records that don't exist
		return nil
	} else if err != nil {
		return fmt.Errorf("pebble: error getting existing record: %w", err)
	}

	err = storage.PatchRecord(existing, record, fields)
	if err != nil {
		return fmt.Errorf("pebble: error patching record: %w", err)
	}

	return backend.updateRecordLocked(rw, record)
}

func (backend *Backend) updateRecordLocked(
	rw readerWriter,
	record *databrokerpb.Record,
) error {
	if record.GetDeletedAt() != nil {
		return backend.deleteRecordLocked(rw, record.GetType(), record.GetId())
	}

	existing, err := recordKeySpace.get(rw, record.GetType(), record.GetId())
	if isNotFound(err) {
		// nothing to do
	} else if err != nil {
		return fmt.Errorf("pebble: error getting existing record: %w", err)
	} else {
		err = recordIndexByModifiedAtKeySpace.delete(rw, existing.GetType(), existing.GetModifiedAt().AsTime(), existing.GetId())
		if err != nil {
			return fmt.Errorf("pebble: error deleting record index by modified at for existing record: %w", err)
		}
	}

	latestRecordVersion, err := metadataKeySpace.getLatestRecordVersion(rw)
	if err != nil {
		return fmt.Errorf("pebble: error getting latest record version: %w", err)
	}
	latestRecordVersion++

	record.ModifiedAt = timestamppb.Now()
	record.Version = latestRecordVersion

	err = recordChangeKeySpace.set(rw, record)
	if err != nil {
		return fmt.Errorf("pebble: error setting record change: %w", err)
	}

	err = recordKeySpace.set(rw, record)
	if err != nil {
		return fmt.Errorf("pebble: error setting record: %w", err)
	}

	err = recordIndexByModifiedAtKeySpace.set(rw, record.GetType(), record.GetModifiedAt().AsTime(), record.GetId())
	if err != nil {
		return fmt.Errorf("pebble: error setting record index by modified at: %w", err)
	}

	err = metadataKeySpace.setLatestRecordVersion(rw, latestRecordVersion)
	if err != nil {
		return fmt.Errorf("pebble: error setting latest record version: %w", err)
	}

	return nil
}

func (backend *Backend) deleteRecordLocked(
	rw readerWriter,
	recordType, recordID string,
) error {
	record, err := recordKeySpace.get(rw, recordType, recordID)
	if isNotFound(err) {
		// doesn't exist, so ignore
		return nil
	}

	err = recordKeySpace.delete(rw, recordType, recordID)
	if err != nil {
		return fmt.Errorf("pebble: error deleting record: %w", err)
	}

	err = recordIndexByModifiedAtKeySpace.delete(rw, record.GetType(), record.GetModifiedAt().AsTime(), record.GetId())
	if err != nil {
		return fmt.Errorf("pebble: error deleting record index by modified at: %w", err)
	}

	return nil
}

func (backend *Backend) enforceOptionsLocked(
	rw readerWriter,
	recordType string,
) error {
	options, err := optionsKeySpace.get(rw, recordType)
	if isNotFound(err) {
		// no options defined, nothing to do
		return nil
	} else if err != nil {
		return fmt.Errorf("pebble: error getting options: %w", err)
	}

	// if capacity isn't set, there's nothing to do
	if options.Capacity == nil {
		return nil
	}

	var cnt uint64
	for key, err := range recordIndexByModifiedAtKeySpace.iterateForRecordTypeReversed(rw, recordType) {
		if err != nil {
			return fmt.Errorf("pebble: error iterating record modified at index: %w", err)
		}
		cnt++

		if cnt > options.GetCapacity() {
			err = backend.deleteRecordLocked(rw, key.recordType, key.recordID)
			if err != nil {
				return fmt.Errorf("pebble: error deleting record to enforce capacity: %w", err)
			}
		}
	}

	return nil
}

func isNotFound(err error) bool {
	return errors.Is(err, pebble.ErrNotFound)
}
