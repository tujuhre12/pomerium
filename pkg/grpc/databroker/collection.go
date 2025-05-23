package databroker

import (
	"cmp"
	"context"
	"iter"
	"strings"

	"github.com/hashicorp/go-set/v3"
	"google.golang.org/protobuf/proto"
)

// CompareRecordsByTypeAndID orders records by type and id.
func CompareRecordsByTypeAndID(x, y *Record) int {
	return cmp.Or(
		strings.Compare(x.GetType(), y.GetType()),
		strings.Compare(x.GetId(), y.GetId()),
	)
}

// SortedRecordCollection stores records ordered by (type, id).
type SortedRecordCollection interface {
	// All returns an interator of all the records.
	All(ctx context.Context) iter.Seq2[*Record, error]
	// Update updates records. If a record has DeletedAt set, it is removed.
	Update(ctx context.Context, records []*Record) error
}

type inMemorySortedRecordCollection struct {
	records *set.TreeSet[*Record]
}

// NewInMemorySortedRecordCollection creates a new SortedRecordCollection that stores records in memory.
func NewInMemorySortedRecordCollection() SortedRecordCollection {
	return &inMemorySortedRecordCollection{
		records: set.NewTreeSet(CompareRecordsByTypeAndID),
	}
}

func (s *inMemorySortedRecordCollection) All(_ context.Context) iter.Seq2[*Record, error] {
	return func(yield func(*Record, error) bool) {
		for record := range s.records.Items() {
			if !yield(record, nil) {
				return
			}
		}
	}
}

func (s *inMemorySortedRecordCollection) Update(_ context.Context, records []*Record) error {
	for _, record := range records {
		// always remove the record since we've only ordered by (type, id) and the contents may have changed
		s.records.Remove(record)
		if record.DeletedAt == nil {
			s.records.Insert(record)
		}
	}
	return nil
}

// A RecordChange indicates a record has changed.
type RecordChange struct {
	Original, Desired *Record
}

// ReconcileSortedRecordCollections compares all the records in original with those in desired.
// If anything has changed a RecordChange will be yielded. For new records, original will be nil.
// For deleted records, desired will be nil.
//
// The SortedRecordCollections should not be updated while this function is running.
func ReconcileSortedRecordCollections(ctx context.Context, original, desired SortedRecordCollection) iter.Seq2[*RecordChange, error] {
	return func(yield func(*RecordChange, error) bool) {
		originalNext, originalStop := iter.Pull2(original.All(ctx))
		defer originalStop()

		desiredNext, desiredStop := iter.Pull2(desired.All(ctx))
		defer desiredStop()

		originalRecord, originalErr, originalValid := originalNext()
		desiredRecord, desiredErr, desiredValid := desiredNext()

		for {
			if originalErr != nil {
				yield(nil, originalErr)
				return
			}
			if desiredErr != nil {
				yield(nil, desiredErr)
				return
			}

			// if both are invalid we are done
			if !originalValid && !desiredValid {
				return
			}

			// if only the original is valid, these are records we need to remove
			if !desiredValid {
				if !yield(&RecordChange{originalRecord, nil}, nil) {
					return
				}
				originalRecord, originalErr, originalValid = originalNext()
				continue
			}

			// if only the desired is valid, these are new records
			if !originalValid {
				if !yield(&RecordChange{nil, desiredRecord}, nil) {
					return
				}
				desiredRecord, desiredErr, desiredValid = desiredNext()
				continue
			}

			// compare the records
			c := CompareRecordsByTypeAndID(originalRecord, desiredRecord)

			// if original is less than desired, this is a record that has been removed
			if c < 0 {
				if !yield(&RecordChange{originalRecord, nil}, nil) {
					return
				}
				originalRecord, originalErr, originalValid = originalNext()
				continue
			}

			// if desired is less than original, this is a new record
			if c > 0 {
				if !yield(&RecordChange{nil, desiredRecord}, nil) {
					return
				}
				desiredRecord, desiredErr, desiredValid = desiredNext()
				continue
			}

			// there is an original and desired record for the same (type, id), compare their data and move forward
			if !proto.Equal(originalRecord.Data, desiredRecord.Data) {
				if !yield(&RecordChange{originalRecord, desiredRecord}, nil) {
					return
				}
			}
			originalRecord, originalErr, originalValid = originalNext()
			desiredRecord, desiredErr, desiredValid = desiredNext()
		}
	}
}
