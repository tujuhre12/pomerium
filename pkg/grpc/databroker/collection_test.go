package databroker_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/pomerium/pomerium/pkg/grpc/databroker"
	"github.com/pomerium/pomerium/pkg/protoutil"
)

func TestReconcileSortedRecordCollections(t *testing.T) {
	t.Parallel()

	r1 := &databroker.Record{Type: "record", Id: "1"}
	r2a := &databroker.Record{Type: "record", Id: "2", Data: protoutil.NewAnyString("a")}
	r2b := &databroker.Record{Type: "record", Id: "2", Data: protoutil.NewAnyString("b")}
	r3 := &databroker.Record{Type: "record", Id: "3"}
	r4 := &databroker.Record{Type: "record", Id: "4"}

	for _, tc := range []struct {
		original, desired []*databroker.Record
		expected          []*databroker.RecordChange
	}{
		{nil, nil, nil},
		{[]*databroker.Record{r1}, []*databroker.Record{r1}, nil},
		{[]*databroker.Record{r1, r4}, []*databroker.Record{r2a, r3, r4}, []*databroker.RecordChange{{r1, nil}, {nil, r2a}, {nil, r3}}},
		{[]*databroker.Record{r1, r2a}, nil, []*databroker.RecordChange{{r1, nil}, {r2a, nil}}},
		{nil, []*databroker.Record{r1, r2a}, []*databroker.RecordChange{{nil, r1}, {nil, r2a}}},
		{[]*databroker.Record{r1, r2a}, []*databroker.Record{r1, r2b}, []*databroker.RecordChange{{r2a, r2b}}},
	} {
		c1 := databroker.NewInMemorySortedRecordCollection()
		assert.NoError(t, c1.Update(context.Background(), tc.original))
		c2 := databroker.NewInMemorySortedRecordCollection()
		assert.NoError(t, c2.Update(context.Background(), tc.desired))

		var changes []*databroker.RecordChange
		for change, err := range databroker.ReconcileSortedRecordCollections(context.Background(), c1, c2) {
			assert.NoError(t, err)
			changes = append(changes, change)
		}

		assert.Equal(t, tc.expected, changes)
	}
}
