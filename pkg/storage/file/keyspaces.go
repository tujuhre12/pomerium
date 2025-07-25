package file

import (
	"bytes"
	"fmt"
	"iter"
	"time"

	"github.com/cockroachdb/pebble/v2"

	databrokerpb "github.com/pomerium/pomerium/pkg/grpc/databroker"
	"github.com/pomerium/pomerium/pkg/pebbleutil"
)

// pebble is an ordered key-value database
// we break up keys into various keyspaces

const (
	prefixUnusedKeySpace = iota
	prefixMetadataKeySpace
	prefixRecordKeySpace
	prefixRecordIndexByModifiedAtKeySpace
	prefixRecordChangeKeySpace
	prefixRecordChangeIndexByRecordTypeKeySpace
	prefixLeaseKeySpace
	prefixOptionsKeySpace
)

// metadata:
//   latestRecordVersion:
//     key: prefix-metadata | 0x01
//     value: {latestRecordVersion as uint64}
//   serverVersion:
//     key: prefix-metadata | 0x02
//     value: {serverVersion as uint64}
//   migration:
//     key: prefix-metadata | 0x03
//     value: {migration as uint64}

type metadataKeySpaceType struct{}

var metadataKeySpace metadataKeySpaceType

func (ks metadataKeySpaceType) encodeLatestRecordVersionKey() []byte {
	prefix := []byte{prefixMetadataKeySpace}
	field := []byte{0x01}

	data := make([]byte, len(prefix)+len(field))
	offset := 0

	copy(data[offset:], prefix)
	offset += len(prefix)

	copy(data[offset:], field)
	offset += len(field)

	return data
}

func (ks metadataKeySpaceType) encodeServerVersionKey() []byte {
	prefix := []byte{prefixMetadataKeySpace}
	field := []byte{0x02}

	data := make([]byte, len(prefix)+len(field))
	offset := 0

	copy(data[offset:], prefix)
	offset += len(prefix)

	copy(data[offset:], field)
	offset += len(field)

	return data
}

func (ks metadataKeySpaceType) encodeMigrationKey() []byte {
	prefix := []byte{prefixMetadataKeySpace}
	field := []byte{0x03}

	data := make([]byte, len(prefix)+len(field))
	offset := 0

	copy(data[offset:], prefix)
	offset += len(prefix)

	copy(data[offset:], field)
	offset += len(field)

	return data
}

func (ks metadataKeySpaceType) getLatestRecordVersion(r reader) (uint64, error) {
	return get(r, ks.encodeLatestRecordVersionKey(), decodeUint64)
}

func (ks metadataKeySpaceType) getServerVersion(r reader) (uint64, error) {
	return get(r, ks.encodeServerVersionKey(), decodeUint64)
}

func (ks metadataKeySpaceType) getMigration(r reader) (uint64, error) {
	return get(r, ks.encodeMigrationKey(), decodeUint64)
}

func (ks metadataKeySpaceType) setLatestRecordVersion(w writer, latestRecordVersion uint64) error {
	return w.Set(ks.encodeLatestRecordVersionKey(), encodeUint64(latestRecordVersion), nil)
}

func (ks metadataKeySpaceType) setServerVersion(w writer, serverVersion uint64) error {
	return w.Set(ks.encodeServerVersionKey(), encodeUint64(serverVersion), nil)
}

func (ks metadataKeySpaceType) setMigration(w writer, migration uint64) error {
	return w.Set(ks.encodeMigrationKey(), encodeUint64(migration), nil)
}

// record:
//   keys: prefix-record | {recordType as bytes} | 0x00 | {recordID as bytes}
//   values: {record as proto}

type recordKeySpaceType struct{}

var recordKeySpace recordKeySpaceType

func (ks recordKeySpaceType) bounds() (lowerBound, upperBound []byte) {
	prefix := []byte{prefixRecordKeySpace}

	data := make([]byte, len(prefix))
	offset := 0

	copy(data[offset:], prefix)
	offset += len(prefix)

	return data, pebbleutil.PrefixToUpperBound(upperBound)
}

func (ks recordKeySpaceType) boundsForRecordType(recordType string) (lowerBound, upperBound []byte) {
	prefix := []byte{prefixRecordKeySpace}

	data := make([]byte, len(prefix)+len(recordType)+1)
	offset := 0

	copy(data[offset:], prefix)
	offset += len(prefix)

	copy(data[offset:], recordType)
	offset += len(recordType)

	data[offset] = 0x00
	offset++

	return data, pebbleutil.PrefixToUpperBound(upperBound)
}

func (ks recordKeySpaceType) decodeKey(data []byte) (recordType, recordID string, err error) {
	prefix := []byte{prefixRecordKeySpace}
	if !bytes.HasPrefix(data, prefix) {
		return "", "", fmt.Errorf("invalid key, unexpected prefix")
	}
	data = data[len(prefix):]

	idx := bytes.IndexByte(data, 0x00)
	if idx < 0 {
		return "", "", fmt.Errorf("invalid key, missing record id")
	}

	recordType = string(data[:idx])
	recordID = string(data[idx+1:])

	return recordType, recordID, nil
}

func (ks recordKeySpaceType) delete(w writer, recordType, recordID string) error {
	return w.Delete(ks.encodeKey(recordType, recordID), nil)
}

func (ks recordKeySpaceType) encodeKey(recordType, recordID string) []byte {
	prefix := []byte{prefixRecordKeySpace}

	data := make([]byte, len(prefix)+len(recordType)+1+len(recordID))
	offset := 0

	copy(data[offset:], prefix)
	offset += len(prefix)

	copy(data[offset:], recordType)
	offset += len(recordType)

	data[offset] = 0x00
	offset++

	copy(data[offset:], recordID)
	offset += len(recordID)

	return data
}

func (ks recordKeySpaceType) encodeValue(record *databrokerpb.Record) []byte {
	return encodeProto(record)
}

func (ks recordKeySpaceType) get(r reader, recordType, recordID string) (*databrokerpb.Record, error) {
	return get(r, ks.encodeKey(recordType, recordID), decodeProto[databrokerpb.Record])
}

func (ks recordKeySpaceType) iterateTypes(r reader) iter.Seq2[string, error] {
	return func(yield func(string, error) bool) {
		opts := new(pebble.IterOptions)
		opts.LowerBound, opts.UpperBound = ks.bounds()

		var previousRecordType string
		for key, err := range pebbleutil.IterateKeys(r, opts) {
			if err != nil {
				yield("", err)
				return
			}

			recordType, _, err := ks.decodeKey(key)
			if err != nil {
				// skip invalid keys
				continue
			}

			if previousRecordType != "" && recordType == previousRecordType {
				// skip until the record type changes
				continue
			}
			previousRecordType = recordType

			if !yield(recordType, nil) {
				return
			}
		}
	}
}

func (ks recordKeySpaceType) set(w writer, record *databrokerpb.Record) error {
	return w.Set(ks.encodeKey(record.GetType(), record.GetId()), ks.encodeValue(record), nil)
}

// record-index-by-modified-at:
//   keys: prefix-record-index-by-modified-at | {recordType as bytes} | 0x00 | {modifiedAt as timestamp} | 0x00 | {recordID as bytes}
//   values: nil

type (
	recordIndexByModifiedAtKeySpaceType struct{}
	recordIndexByModifiedAtKeySpaceKey  struct {
		recordType string
		modifiedAt time.Time
		recordID   string
	}
)

var recordIndexByModifiedAtKeySpace recordIndexByModifiedAtKeySpaceType

func (ks recordIndexByModifiedAtKeySpaceType) boundsForRecordType(recordType string) (lowerBound, upperBound []byte) {
	prefix := []byte{prefixRecordIndexByModifiedAtKeySpace}

	data := make([]byte, len(prefix)+len(recordType)+1)
	offset := 0

	copy(data[offset:], prefix)
	offset += len(prefix)

	copy(data[offset:], recordType)
	offset += len(recordType)

	data[offset] = 0x00
	offset++

	return data, pebbleutil.PrefixToUpperBound(upperBound)
}

func (ks recordIndexByModifiedAtKeySpaceType) decodeKey(data []byte) (recordType string, modifiedAt time.Time, recordID string, err error) {
	prefix := []byte{prefixRecordIndexByModifiedAtKeySpace}
	if !bytes.HasPrefix(data, prefix) {
		return recordType, modifiedAt, recordID, fmt.Errorf("invalid key, invalid prefix")
	}
	data = data[len(prefix):]

	idx := bytes.IndexByte(data, 0x00)
	if idx < 0 {
		return recordType, modifiedAt, recordID, fmt.Errorf("invalid key, invalid record type")
	}
	recordType = string(data[:idx])
	data = data[idx+1:]

	modifiedAt, err = decodeTimestamp(data)
	if err != nil {
		return recordType, modifiedAt, recordID, fmt.Errorf("invalid key, %w", err)
	}
	data = data[8:]

	recordID = string(data)

	return recordType, modifiedAt, recordID, nil
}

func (ks recordIndexByModifiedAtKeySpaceType) delete(w writer, recordType string, modifiedAt time.Time, recordID string) error {
	return w.Delete(ks.encodeKey(recordType, modifiedAt, recordID), nil)
}

func (ks recordIndexByModifiedAtKeySpaceType) encodeKey(recordType string, modifiedAt time.Time, recordID string) []byte {
	prefix := []byte{prefixRecordIndexByModifiedAtKeySpace}
	modifiedAtData := encodeTimestamp(modifiedAt)

	data := make([]byte, len(prefix)+len(recordType)+1+len(modifiedAtData)+1+len(recordID))
	offset := 0

	copy(data[offset:], prefix)
	offset += len(prefix)

	copy(data[offset:], recordType)
	offset += len(recordType)

	data[offset] = 0x00
	offset++

	copy(data[offset:], modifiedAtData)
	offset += len(modifiedAtData)

	data[offset] = 0x00
	offset++

	copy(data[offset:], recordID)
	offset += len(recordID)

	return data
}

func (ks recordIndexByModifiedAtKeySpaceType) encodeValue() []byte {
	return nil
}

func (ks recordIndexByModifiedAtKeySpaceType) iterateForRecordTypeReversed(r reader, recordType string) iter.Seq2[recordIndexByModifiedAtKeySpaceKey, error] {
	return func(yield func(recordIndexByModifiedAtKeySpaceKey, error) bool) {
		var key recordIndexByModifiedAtKeySpaceKey

		opts := new(pebble.IterOptions)
		opts.LowerBound, opts.UpperBound = ks.boundsForRecordType(recordType)
		it, err := r.NewIter(opts)
		if err != nil {
			yield(key, err)
			return
		}

		for it.Last(); it.Valid(); it.Prev() {
			_, modifiedAt, recordID, err := ks.decodeKey(it.Key())
			if err != nil {
				// ignore invalid keys
				continue
			}

			if !yield(recordIndexByModifiedAtKeySpaceKey{recordType, modifiedAt, recordID}, nil) {
				return
			}
		}
		err = it.Error()
		if err != nil {
			yield(key, err)
			return
		}
	}
}

func (ks recordIndexByModifiedAtKeySpaceType) set(w writer, recordType string, modifiedAt time.Time, recordID string) error {
	return w.Set(ks.encodeKey(recordType, modifiedAt, recordID), ks.encodeValue(), nil)
}

// record-change:
//	keys: prefix-record-change | {version as uint64}
//	values: {record as proto}

type recordChangeKeySpaceType struct{}

var recordChangeKeySpace recordChangeKeySpaceType

func (ks recordChangeKeySpaceType) encodeKey(version uint64) []byte {
	return encodeUint64(version)
}

func (ks recordChangeKeySpaceType) encodeValue(record *databrokerpb.Record) []byte {
	return encodeProto(record)
}

func (ks recordChangeKeySpaceType) set(w writer, record *databrokerpb.Record) error {
	return w.Set(ks.encodeKey(record.GetVersion()), ks.encodeValue(record), nil)
}

// record-change-index-by-record-type
//   keys: prefix-record-change-index-by-record-type | {recordType as string} | {version as uint64}
//   values: nil

type recordChangeIndexByRecordTypeKeySpaceType struct{}

var recordChangeIndexByRecordTypeKeySpace recordChangeIndexByRecordTypeKeySpaceType

func (ks recordChangeIndexByRecordTypeKeySpaceType) delete(w writer, recordType string, version uint64) error {
	return w.Delete(ks.encodeKey(recordType, version), nil)
}

func (ks recordChangeIndexByRecordTypeKeySpaceType) encodeKey(recordType string, version uint64) []byte {
	prefix := []byte{prefixRecordChangeIndexByRecordTypeKeySpace}
	versionData := encodeUint64(version)

	data := make([]byte, len(prefix)+len(recordType)+1+len(versionData))
	offset := 0

	copy(data[offset:], prefix)
	offset += len(prefix)

	copy(data[offset:], recordType)
	offset += len(recordType)

	data[offset] = 0x00
	offset++

	copy(data[offset:], versionData)
	offset += len(versionData)

	return data
}

func (ks recordChangeIndexByRecordTypeKeySpaceType) encodeValue() []byte {
	return nil
}

func (ks recordChangeIndexByRecordTypeKeySpaceType) iterateForRecordTypeAfter(r reader, recordType string, afterVersion uint64) iter.Seq2[uint64, error] {
	return func(yield func(uint64, error) bool) {
		opts := new(pebble.IterOptions)
		opts.LowerBound = ks.encodeKey(recordType, afterVersion+1)
	}
}

func (ks recordChangeIndexByRecordTypeKeySpaceType) set(w writer, recordType string, version uint64) error {
	return w.Set(ks.encodeKey(recordType, version), ks.encodeValue(), nil)
}

// lease:
//   keys: prefix-lease | {leaseName as bytes}
//   values: {leaseID as bytes} | 0x00 | {expiresAt as timestamp}

type leaseKeySpaceType struct{}

var leaseKeySpace leaseKeySpaceType

func (ks leaseKeySpaceType) get(r reader, leaseName string) (leaseID string, expiresAt time.Time, err error) {
	panic("not implemented")
}

func (ks leaseKeySpaceType) set(w writer, leaseName, leaseID string, expiresAt time.Time) error {
	panic("not implemented")
}

// options:
//   keys: prefix-options | {recordType as bytes}
//   values: {options as proto}

type optionsKeySpaceType struct{}

var optionsKeySpace optionsKeySpaceType

func (ks optionsKeySpaceType) delete(w writer, recordType string) error {
	return w.Delete(ks.encodeKey(recordType), nil)
}

func (ks optionsKeySpaceType) encodeKey(recordType string) []byte {
	prefix := []byte{prefixOptionsKeySpace}

	data := make([]byte, len(prefix)+len(recordType))
	offset := 0

	copy(data[offset:], prefix)
	offset += len(prefix)

	copy(data[offset:], recordType)
	offset += len(recordType)

	return data
}

func (ks optionsKeySpaceType) encodeValue(options *databrokerpb.Options) []byte {
	return encodeProto(options)
}

func (ks optionsKeySpaceType) get(r reader, recordType string) (*databrokerpb.Options, error) {
	return get(r, ks.encodeKey(recordType), decodeProto[databrokerpb.Options])
}

func (ks optionsKeySpaceType) set(w writer, recordType string, options *databrokerpb.Options) error {
	return w.Set(ks.encodeKey(recordType), ks.encodeValue(options), nil)
}

func get[T any](r reader, key []byte, fn func(value []byte) (T, error)) (T, error) {
	var value T

	raw, closer, err := r.Get(key)
	if err != nil {
		return value, err
	}
	value, err = fn(raw)
	_ = closer.Close()

	return value, err
}
