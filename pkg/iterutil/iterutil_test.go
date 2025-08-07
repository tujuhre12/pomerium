package iterutil_test

import (
	"cmp"
	"iter"
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/pomerium/pomerium/pkg/iterutil"
)

func TestProduct(t *testing.T) {
	t.Parallel()

	assert.Empty(t, slices.Collect(iterutil.Product[int]()))
	assert.Equal(t,
		[][]int{{1}, {2}},
		slices.Collect(iterutil.Product(
			slices.Values([]int{1, 2}),
		)))
	assert.Equal(t,
		[][]int{{3}, {4}},
		slices.Collect(iterutil.Product(
			slices.Values([]int(nil)),
			slices.Values([]int{3, 4}),
		)))
	assert.Equal(t,
		[][]int{
			{1, 3, 5},
			{1, 3, 6},
			{1, 4, 5},
			{1, 4, 6},
			{2, 3, 5},
			{2, 3, 6},
			{2, 4, 5},
			{2, 4, 6},
		},
		slices.Collect(iterutil.Product(
			slices.Values([]int{1, 2}),
			slices.Values([]int{3, 4}),
			slices.Values([]int{5, 6}),
		)))
}

func TestSortedIntersection(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		input  [][]int
		expect []int
	}{
		{
			input:  [][]int{},
			expect: nil,
		},
		{
			input:  [][]int{{1}, {1}},
			expect: []int{1},
		},
		{
			input:  [][]int{{1, 5, 11, 23, 99}, {1, 25, 99, 104}},
			expect: []int{1, 99},
		},
		{
			input:  [][]int{{1, 2, 3, 4, 5}, {1, 3, 5}, {2, 4, 5}, {5}},
			expect: []int{5},
		},
	} {
		seqs := make([]iter.Seq[int], len(tc.input))
		for i, input := range tc.input {
			seqs[i] = slices.Values(input)
		}
		actual := slices.Collect(iterutil.SortedIntersection(cmp.Compare[int], seqs...))
		assert.Equal(t, tc.expect, actual)
	}
}

func TestSortedUnion(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		input  [][]int
		expect []int
	}{
		{
			input:  [][]int{},
			expect: nil,
		},
		{
			input:  [][]int{{1}, {1}},
			expect: []int{1},
		},
		{
			input:  [][]int{{1, 5, 11, 23, 99}, {1, 25, 99, 104}},
			expect: []int{1, 5, 11, 23, 25, 99, 104},
		},
		{
			input:  [][]int{{1, 2, 3, 4, 5}, {1, 3, 5}, {2, 4, 5}, {5}},
			expect: []int{1, 2, 3, 4, 5},
		},
	} {
		seqs := make([]iter.Seq[int], len(tc.input))
		for i, input := range tc.input {
			seqs[i] = slices.Values(input)
		}
		actual := slices.Collect(iterutil.SortedUnion(cmp.Compare[int], seqs...))
		assert.Equal(t, tc.expect, actual)
	}
}
