package storage

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/structpb"
)

func TestFilterExpressionFromStruct(t *testing.T) {
	type M = map[string]any
	type A = []any

	s, err := structpb.NewStruct(M{
		"$and": A{
			M{"a": M{"b": "1"}},
		},
		"c": M{
			"d": M{
				"e": M{
					"$eq": "2",
				},
			},
		},
		"f": A{
			"3", "4", "5",
		},
		"$or": A{
			M{"g": "6"},
			M{"h": "7"},
		},
	})
	require.NoError(t, err)
	expr, err := FilterExpressionFromStruct(s)
	assert.NoError(t, err)
	assert.Equal(t,
		AndFilterExpression{
			EqualsFilterExpression{
				Fields: []string{"a", "b"},
				Value:  "1",
			},
			OrFilterExpression{
				EqualsFilterExpression{
					Fields: []string{"g"},
					Value:  "6",
				},
				EqualsFilterExpression{
					Fields: []string{"h"},
					Value:  "7",
				},
			},
			EqualsFilterExpression{
				Fields: []string{"c", "d", "e"},
				Value:  "2",
			},
			OrFilterExpression{
				EqualsFilterExpression{
					Fields: []string{"f"},
					Value:  "3",
				},
				EqualsFilterExpression{
					Fields: []string{"f"},
					Value:  "4",
				},
				EqualsFilterExpression{
					Fields: []string{"f"},
					Value:  "5",
				},
			},
		},
		expr)
}

func TestFilterToDNF(t *testing.T) {
	t.Parallel()

	and := func(fs ...FilterExpression) AndFilterExpression {
		return AndFilterExpression(fs)
	}
	or := func(fs ...FilterExpression) OrFilterExpression {
		return OrFilterExpression(fs)
	}
	eq := func(key, value string) EqualsFilterExpression {
		return EqualsFilterExpression{Fields: []string{key}, Value: value}
	}

	for _, tc := range []struct {
		expect DNF
		filter FilterExpression
	}{
		{
			expect: DNF{
				{eq("A", "1")},
			},
			filter: eq("A", "1"),
		},
		{
			expect: DNF{
				{eq("A", "1"), eq("B", "2")},
			},
			filter: and(
				eq("A", "1"),
				eq("B", "2"),
			),
		},
		{
			expect: DNF{
				{eq("A", "1")},
				{eq("B", "2")},
			},
			filter: or(
				eq("A", "1"),
				eq("B", "2"),
			),
		},
		{
			expect: DNF{
				{eq("A", "1"), eq("C", "3")},
				{eq("A", "1"), eq("D", "4")},
				{eq("B", "2"), eq("C", "3")},
				{eq("B", "2"), eq("D", "4")},
			},
			filter: and(
				or(eq("A", "1"), eq("B", "2")),
				or(eq("C", "3"), eq("D", "4")),
			),
		},
	} {
		actual := FilterToDNF(tc.filter)
		assert.Equal(t, tc.expect.String(), actual.String())
	}
}
