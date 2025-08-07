// Package iterutil contains functions for working with iterators.
package iterutil

import "iter"

type Pair[K any, V any] struct {
	Key   K
	Value V
}

func PairsFromSeq2[K any, V any](seq iter.Seq2[K, V]) iter.Seq[Pair[K, V]] {
	return func(yield func(Pair[K, V]) bool) {
		for first, second := range seq {
			if !yield(Pair[K, V]{first, second}) {
				return
			}
		}
	}
}

func PairsToSeq2[K any, V any](seq iter.Seq[Pair[K, V]]) iter.Seq2[K, V] {
	return func(yield func(K, V) bool) {
		for pair := range seq {
			if !yield(pair.Key, pair.Value) {
				return
			}
		}
	}
}

// SortedIntersection computes the set-intersection of zero or more sorted iterators.
// For an element to be returned, it must be found in all of the sequences. Values are
// assumed to be sorted. If they are not sorted the intersection will not be valid.
func SortedIntersection[E any](compare func(a, b E) int, seqs ...iter.Seq[E]) iter.Seq[E] {
	switch len(seqs) {
	case 0:
		return func(_ func(E) bool) {}
	case 1:
		return seqs[0]
	case 2:
		return func(yield func(E) bool) {
			next1, stop1 := iter.Pull(seqs[0])
			defer stop1()
			next2, stop2 := iter.Pull(seqs[1])
			defer stop2()

			value1, ok1 := next1()
			value2, ok2 := next2()
			for ok1 && ok2 {
				switch compare(value1, value2) {
				case -1:
					value1, ok1 = next1()
				case 0:
					if !yield(value1) {
						return
					}
					value1, ok1 = next1()
					value2, ok2 = next2()
				case 1:
					value2, ok2 = next2()
				}
			}
		}
	default:
		return SortedIntersection(compare,
			SortedIntersection(compare, seqs[:len(seqs)/2]...),
			SortedIntersection(compare, seqs[len(seqs)/2:]...))
	}
}

// SortedUnion computes the set-union of zero or more sorted iterators.
// For an element to be returned, it must be found in at least one of the sequences.
// Values are assumed to be sorted and only duplicates are removed.
func SortedUnion[E any](compare func(a, b E) int, seqs ...iter.Seq[E]) iter.Seq[E] {
	switch len(seqs) {
	case 0:
		return func(_ func(E) bool) {}
	case 1:
		return seqs[0]
	case 2:
		return func(yield func(E) bool) {
			next1, stop1 := iter.Pull(seqs[0])
			defer stop1()
			next2, stop2 := iter.Pull(seqs[1])
			defer stop2()

			value1, ok1 := next1()
			value2, ok2 := next2()
			for ok1 || ok2 {
				switch {
				case !ok1:
					if !yield(value2) {
						return
					}
					value2, ok2 = next2()
				case !ok2:
					if !yield(value1) {
						return
					}
					value1, ok1 = next1()
				default:
					switch compare(value1, value2) {
					case -1:
						if !yield(value1) {
							return
						}
						value1, ok1 = next1()
					case 0:
						if !yield(value1) {
							return
						}
						value1, ok1 = next1()
						value2, ok2 = next2()
					case 1:
						if !yield(value2) {
							return
						}
						value2, ok2 = next2()
					}
				}
			}
		}
	default:
		return SortedUnion(compare,
			SortedUnion(compare, seqs[:len(seqs)/2]...),
			SortedUnion(compare, seqs[len(seqs)/2:]...))
	}
}

// Product returns the cartesian product of multiple iterators.
func Product[E any](seqs ...iter.Seq[E]) iter.Seq[[]E] {
	switch len(seqs) {
	case 0:
		return func(_ func([]E) bool) {}
	case 1:
		return func(yield func([]E) bool) {
			for x := range seqs[0] {
				if !yield([]E{x}) {
					return
				}
			}
		}
	default:
		return func(yield func([]E) bool) {
			first := seqs[0]
			rest := Product(seqs[1:]...)
			hasFirst := false
			for x := range first {
				hasFirst = true
				hasRest := false
				for ys := range rest {
					hasRest = true

					xs := append([]E{x}, ys...)
					if !yield(xs) {
						return
					}
				}

				// handle the case when there were no elements in the other sequences
				if !hasRest {
					if !yield([]E{x}) {
						return
					}
				}
			}

			// handle the case when there were no elements in the first sequence
			if !hasFirst {
				for xs := range rest {
					if !yield(xs) {
						return
					}
				}
			}
		}
	}
}
