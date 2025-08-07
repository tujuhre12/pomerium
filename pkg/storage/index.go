package storage

import (
	"net/netip"

	"github.com/gaissmai/bart"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/structpb"

	databrokerpb "github.com/pomerium/pomerium/pkg/grpc/databroker"
)

const (
	indexField = "$index"
	cidrField  = "cidr"
)

// GetRecordIndex gets a record's index. If there is no index, nil is returned.
func GetRecordIndex(msg proto.Message) *structpb.Struct {
	for {
		data, ok := msg.(*anypb.Any)
		if !ok {
			break
		}
		msg, _ = data.UnmarshalNew()
	}

	var s *structpb.Struct
	if sv, ok := msg.(*structpb.Value); ok {
		s = sv.GetStructValue()
	} else {
		s, _ = msg.(*structpb.Struct)
	}
	if s == nil {
		return nil
	}

	f, ok := s.Fields[indexField]
	if !ok {
		return nil
	}
	return f.GetStructValue()
}

// GetRecordIndexCIDR returns the $index.cidr for a record's data. If none is available nil is returned.
func GetRecordIndexCIDR(msg proto.Message) *netip.Prefix {
	obj := GetRecordIndex(msg)
	if obj == nil {
		return nil
	}

	cf, ok := obj.Fields[cidrField]
	if !ok {
		return nil
	}

	c := cf.GetStringValue()
	if c == "" {
		return nil
	}

	prefix, err := netip.ParsePrefix(c)
	if err != nil {
		return nil
	}
	return &prefix
}

func RecordMatchesIPPrefix(record *databrokerpb.Record, prefix netip.Prefix) bool {
	cidr := GetRecordIndexCIDR(record.GetData())
	if cidr == nil {
		return false
	}

	var tbl bart.Table[struct{}]
	tbl.Insert(*cidr, struct{}{})
	_, ok := tbl.LookupPrefix(prefix)
	return ok
}

func RecordMatchesIPAddr(record *databrokerpb.Record, addr netip.Addr) bool {
	cidr := GetRecordIndexCIDR(record.GetData())
	if cidr == nil {
		return false
	}
	var tbl bart.Table[struct{}]
	tbl.Insert(*cidr, struct{}{})
	_, ok := tbl.Lookup(addr)
	return ok
}
