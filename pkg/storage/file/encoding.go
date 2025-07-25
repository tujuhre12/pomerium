package file

import (
	"encoding/binary"
	"fmt"
	"time"

	"google.golang.org/protobuf/proto"
)

var (
	marshalOptions = proto.MarshalOptions{
		AllowPartial:  true,
		Deterministic: true,
	}
	unmarshalOptions = proto.UnmarshalOptions{
		AllowPartial:   true,
		DiscardUnknown: true,
	}
)

func decodeProto[T any, TPtr interface {
	*T
	proto.Message
}](data []byte) (TPtr, error) {
	var msg T
	err := unmarshalOptions.Unmarshal(data, TPtr(&msg))
	if err != nil {
		return nil, err
	}
	return TPtr(&msg), nil
}

func decodeTimestamp(data []byte) (time.Time, error) {
	ts, err := decodeUint64(data)
	if err != nil {
		return time.Time{}, fmt.Errorf("invalid timestamp, %w", err)
	}
	return time.UnixMicro(int64(ts)), nil
}

func decodeUint64(data []byte) (uint64, error) {
	if len(data) < 8 {
		return 0, fmt.Errorf("invalid uint64, expected 8 bytes, got %d", len(data))
	}
	return binary.BigEndian.Uint64(data), nil
}

func encodeProto(msg proto.Message) []byte {
	data, err := marshalOptions.Marshal(msg)
	if err != nil {
		panic(err)
	}
	return data
}

func encodeTimestamp(value time.Time) []byte {
	return encodeUint64(uint64(value.UnixMicro()))
}

func encodeUint64(value uint64) []byte {
	data := make([]byte, 8)
	binary.BigEndian.PutUint64(data, value)
	return data
}

// decoders

// func decodeMigrationValue(value []byte) (uint64, error) {
// 	return decodeUInt64Value(value)
// }

// func decodeRecordKey(key []byte) (recordType, recordID string, err error) {
// 	if !bytes.HasPrefix(key, []byte{pebblePrefixRecord}) {
// 		return "", "", fmt.Errorf("invalid record key, missing record prefix")
// 	}
// 	key = key[1:]

// 	idx := bytes.IndexByte(key, 0x00)
// 	if idx < 0 {
// 		return "", "", fmt.Errorf("invalid record key, missing record id")
// 	}

// 	return string(key[:idx]), string(key[idx+1:]), nil
// }

// func decodeRecordVersionValue(value []byte) (uint64, error) {
// 	return decodeUInt64Value(value)
// }

// func decodeServerVersionValue(value []byte) (uint64, error) {
// 	return decodeUInt64Value(value)
// }

// func decodeUInt64Value(value []byte) (uint64, error) {
// 	var w wrapperspb.UInt64Value
// 	err := unmarshalOptions.Unmarshal(value, &w)
// 	if err != nil {
// 		return 0, err
// 	}
// 	return w.Value, nil
// }

// // encoders

// func encodeMigrationKey() []byte {
// 	key := make([]byte, 1)
// 	offset := 0

// 	key[offset] = pebblePrefixMigration
// 	offset++

// 	return key
// }

// func encodeRecordByModifiedAtIndexKey(recordType string, modifiedAt time.Time, recordID string) []byte {
// }

// func encodeRecordKey(recordType, recordID string) []byte {
// 	key := make([]byte, 1+len(recordType)+1+len(recordID))
// 	offset := 0

// 	key[offset] = pebblePrefixRecord
// 	offset++

// 	copy(key[offset:], recordType)
// 	offset += len(recordType)

// 	key[offset] = 0x00
// 	offset++

// 	copy(key[offset:], recordID)
// 	offset += len(recordID)

// 	return key
// }

// func encodeRecordVersionKey() []byte {
// 	return []byte{pebblePrefixRecordVersion}
// }

// func encodeRecordVersionValue(recordVersion uint64) []byte {
// 	value, err := marshalOptions.Marshal(wrapperspb.UInt64(recordVersion))
// 	if err != nil {
// 		panic(err)
// 	}
// 	return value
// }

// func encodeServerVersionKey() []byte {
// 	return []byte{pebblePrefixServerVersion}
// }

// func encodeServerVersionValue(serverVersion uint64) []byte {
// 	value, err := marshalOptions.Marshal(wrapperspb.UInt64(serverVersion))
// 	if err != nil {
// 		panic(err)
// 	}
// 	return value
// }
