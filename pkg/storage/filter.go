package storage

import (
	"cmp"
	"fmt"
	"net/netip"
	"slices"
	"sort"
	"strings"

	"google.golang.org/protobuf/types/known/structpb"

	databrokerpb "github.com/pomerium/pomerium/pkg/grpc/databroker"
	"github.com/pomerium/pomerium/pkg/iterutil"
)

// A FilterExpression describes an AST for record stream filters.
type FilterExpression interface {
	isFilterExpression()
}

// FilterExpressionFromStruct creates a FilterExpression from a protobuf struct.
func FilterExpressionFromStruct(s *structpb.Struct) (FilterExpression, error) {
	if s == nil {
		return nil, nil
	}
	return filterExpressionFromStruct(nil, s)
}

func filterExpressionFromValue(path []string, v *structpb.Value) (FilterExpression, error) {
	switch vv := v.GetKind().(type) {
	case *structpb.Value_ListValue:
		var or OrFilterExpression
		for _, vvv := range vv.ListValue.Values {
			e, err := filterExpressionFromValue(path, vvv)
			if err != nil {
				return nil, err
			}
			or = append(or, e)
		}
		return or, nil
	case *structpb.Value_StructValue:
		return filterExpressionFromStruct(path, vv.StructValue)
	}
	return filterExpressionFromEq(path, v)
}

func filterExpressionFromStruct(path []string, s *structpb.Struct) (FilterExpression, error) {
	var and AndFilterExpression
	var fs []string
	for f := range s.GetFields() {
		fs = append(fs, f)
	}
	sort.Strings(fs)

	for _, f := range fs {
		v := s.GetFields()[f]
		switch f {
		case "$and":
			expr, err := filterExpressionFromValue(path, v)
			if err != nil {
				return nil, err
			}
			or, ok := expr.(OrFilterExpression)
			if !ok {
				return nil, fmt.Errorf("$and must be an array")
			}
			if len(or) == 1 {
				and = append(and, or[0])
			} else {
				and = append(and, AndFilterExpression(or))
			}
		case "$or":
			expr, err := filterExpressionFromValue(path, v)
			if err != nil {
				return nil, err
			}
			or, ok := expr.(OrFilterExpression)
			if !ok {
				return nil, fmt.Errorf("$or must be an array")
			}
			if len(or) == 1 {
				and = append(and, or[0])
			} else {
				and = append(and, or)
			}
		case "$eq":
			expr, err := filterExpressionFromEq(path, v)
			if err != nil {
				return nil, err
			}
			and = append(and, expr)
		default:
			expr, err := filterExpressionFromValue(append(path, f), v)
			if err != nil {
				return nil, err
			}
			and = append(and, expr)
		}
	}

	if len(and) == 1 {
		return and[0], nil
	}
	return and, nil
}

func filterExpressionFromEq(path []string, v *structpb.Value) (FilterExpression, error) {
	switch vv := v.GetKind().(type) {
	case *structpb.Value_BoolValue:
		return EqualsFilterExpression{
			Fields: path,
			Value:  fmt.Sprintf("%v", vv.BoolValue),
		}, nil
	case *structpb.Value_NullValue:
		return EqualsFilterExpression{
			Fields: path,
			Value:  fmt.Sprintf("%v", vv.NullValue),
		}, nil
	case *structpb.Value_NumberValue:
		return EqualsFilterExpression{
			Fields: path,
			Value:  fmt.Sprintf("%v", vv.NumberValue),
		}, nil
	case *structpb.Value_StringValue:
		return EqualsFilterExpression{
			Fields: path,
			Value:  vv.StringValue,
		}, nil
	}
	return nil, fmt.Errorf("unsupported struct value type for eq: %T", v.GetKind())
}

// An OrFilterExpression represents a logical-or comparison operator.
type OrFilterExpression []FilterExpression

func (OrFilterExpression) isFilterExpression() {}

// An AndFilterExpression represents a logical-and comparison operator.
type AndFilterExpression []FilterExpression

func (AndFilterExpression) isFilterExpression() {}

// An EqualsFilterExpression represents a field comparison operator.
type EqualsFilterExpression struct {
	Fields []string
	Value  string
}

func (f EqualsFilterExpression) MatchesRecord(record *databrokerpb.Record) (bool, error) {
	switch {
	case slices.Equal(f.Fields, []string{"type"}):
		return record.GetType() == f.Value, nil
	case slices.Equal(f.Fields, []string{"id"}):
		return record.GetId() == f.Value, nil
	case slices.Equal(f.Fields, []string{"$index"}):
		if prefix, err := netip.ParsePrefix(f.Value); err == nil {
			return RecordMatchesIPPrefix(record, prefix), nil
		} else if addr, err := netip.ParseAddr(f.Value); err == nil {
			return RecordMatchesIPAddr(record, addr), nil
		}
		return false, nil
	default:
		return false, fmt.Errorf("unsupported equals expression: %s", strings.Join(f.Fields, "."))
	}
}

func (f EqualsFilterExpression) String() string {
	return fmt.Sprintf("%s=%s", strings.Join(f.Fields, "."), f.Value)
}

func (EqualsFilterExpression) isFilterExpression() {}

type DNF [][]EqualsFilterExpression

func (f DNF) String() string {
	var sb strings.Builder
	for i, x := range f {
		if i > 0 {
			sb.WriteByte('|')
		}
		sb.WriteByte('(')
		for j, y := range x {
			if j > 0 {
				sb.WriteByte('&')
			}
			sb.WriteString(y.String())
		}
		sb.WriteByte(')')
	}
	return sb.String()
}

// FilterToDNF converts a filter into disjunctive normal form.
// (e.g. a list of AND expressions OR'd together)
func FilterToDNF(filter FilterExpression) DNF {
	cmpEqualsFilterExpression := func(a, b EqualsFilterExpression) int {
		return cmp.Or(slices.Compare(a.Fields, b.Fields), cmp.Compare(a.Value, b.Value))
	}
	sortAndCompact := func(dnf DNF) DNF {
		for i := range dnf {
			slices.SortFunc(dnf[i], cmpEqualsFilterExpression)
			dnf[i] = slices.CompactFunc(dnf[i], func(a, b EqualsFilterExpression) bool {
				return cmpEqualsFilterExpression(a, b) == 0
			})
		}
		slices.SortFunc(dnf, func(a, b []EqualsFilterExpression) int {
			return slices.CompareFunc(a, b, cmpEqualsFilterExpression)
		})
		return slices.CompactFunc(dnf, func(a, b []EqualsFilterExpression) bool {
			return slices.CompareFunc(a, b, cmpEqualsFilterExpression) == 0
		})
	}

	var toDNF func(filter FilterExpression) DNF
	toDNF = func(filter FilterExpression) DNF {
		if filter == nil {
			return DNF{nil}
		}

		switch filter := filter.(type) {
		case EqualsFilterExpression:
			return DNF{{filter}}
		case AndFilterExpression:
			children := make([][][]EqualsFilterExpression, len(filter))
			for i := range filter {
				children[i] = toDNF(filter[i])
			}
			var dnf DNF
			for _, f := range filter {
				var next DNF
				for xs := range iterutil.Product(slices.Values(dnf), slices.Values(toDNF(f))) {
					next = append(next, slices.Concat(xs...))
				}
				dnf = next
			}
			return dnf
		case OrFilterExpression:
			var dnf DNF
			for _, f := range filter {
				dnf = append(dnf, toDNF(f)...)
			}
			return dnf
		default:
			panic("unsupported filter expression")
		}
	}
	return sortAndCompact(toDNF(filter))
}
