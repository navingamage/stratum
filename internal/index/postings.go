// Package index implements the inverted index that maps label matchers to
// the set of series they select.
//
// Everything is expressed as a Postings iterator over ascending series refs.
// Keeping the iterators lazy is what makes a selective query cheap: an
// intersection can leapfrog past long runs of a large postings list without
// ever materialising it, so the cost tracks the size of the *result* rather
// than the size of the inputs.
package index

import (
	"container/heap"
	"sort"

	"github.com/navingamage/stratum/internal/model"
)

// Postings iterates a sorted, deduplicated sequence of series refs.
type Postings interface {
	// Next advances to the next ref.
	Next() bool

	// Seek advances to the first ref at or after v. It never moves
	// backwards: seeking to a value already passed leaves the position
	// unchanged and reports true.
	Seek(v model.SeriesRef) bool

	// At returns the current ref, valid after Next or Seek returned true.
	At() model.SeriesRef

	// Err returns any error encountered while iterating.
	Err() error
}

// sizer is implemented by postings whose length is known up front. The
// planner uses it to order intersections smallest-first, which is worth a
// large constant factor: intersecting a 3-element list into a 3-million
// element one costs 3 seeks, the other way round costs 3 million.
type sizer interface {
	Len() int
}

// EstimateLen returns the number of refs in p if that is known cheaply, and
// -1 otherwise.
func EstimateLen(p Postings) int {
	if s, ok := p.(sizer); ok {
		return s.Len()
	}
	return -1
}

// ExpandPostings materialises a postings iterator into a slice. It exists for
// tests and for the block writer, which genuinely needs every ref; query
// paths should stay lazy.
func ExpandPostings(p Postings) ([]model.SeriesRef, error) {
	var out []model.SeriesRef
	for p.Next() {
		out = append(out, p.At())
	}
	return out, p.Err()
}

// errPostings is an already-failed iterator, so that constructors can report
// errors without changing their signature.
type errPostings struct{ err error }

func (e errPostings) Next() bool                { return false }
func (e errPostings) Seek(model.SeriesRef) bool { return false }
func (e errPostings) At() model.SeriesRef       { return 0 }
func (e errPostings) Err() error                { return e.err }
func (e errPostings) Len() int                  { return 0 }

// ErrPostings returns a postings iterator that yields nothing and reports err.
func ErrPostings(err error) Postings { return errPostings{err} }

// EmptyPostings returns an iterator over no refs.
func EmptyPostings() Postings { return errPostings{} }

// IsEmpty reports whether p is statically known to be empty, letting callers
// short-circuit before building an operator tree around it.
func IsEmpty(p Postings) bool {
	e, ok := p.(errPostings)
	return ok && e.err == nil
}

// listPostings iterates a sorted slice of refs.
type listPostings struct {
	list []model.SeriesRef
	cur  model.SeriesRef
	pos  int // index just past cur
}

// NewListPostings returns postings over refs, which must already be sorted
// and deduplicated.
func NewListPostings(refs []model.SeriesRef) Postings {
	if len(refs) == 0 {
		return EmptyPostings()
	}
	return &listPostings{list: refs}
}

func (p *listPostings) At() model.SeriesRef { return p.cur }
func (p *listPostings) Err() error          { return nil }
func (p *listPostings) Len() int            { return len(p.list) }

func (p *listPostings) Next() bool {
	if p.pos >= len(p.list) {
		return false
	}
	p.cur = p.list[p.pos]
	p.pos++
	return true
}

func (p *listPostings) Seek(v model.SeriesRef) bool {
	if p.pos > 0 && p.cur >= v {
		return true
	}
	// Binary search the remainder. Linear scanning would be faster for a
	// near-by target, but intersections routinely seek across most of a large
	// list and the worst case dominates.
	rest := p.list[p.pos:]
	i := sort.Search(len(rest), func(i int) bool { return rest[i] >= v })
	p.pos += i
	if p.pos >= len(p.list) {
		p.pos = len(p.list)
		return false
	}
	p.cur = p.list[p.pos]
	p.pos++
	return true
}

// Intersect returns the postings present in every input.
func Intersect(its ...Postings) Postings {
	if len(its) == 0 {
		return EmptyPostings()
	}
	if len(its) == 1 {
		return its[0]
	}
	for _, p := range its {
		if IsEmpty(p) {
			return EmptyPostings()
		}
	}

	// Order by known length so the shortest list drives the iteration. An
	// unknown length sorts last: it is usually a lazily-computed union whose
	// cost we would rather pay only for refs that survived everything else.
	arr := append([]Postings(nil), its...)
	sort.SliceStable(arr, func(i, j int) bool {
		li, lj := EstimateLen(arr[i]), EstimateLen(arr[j])
		switch {
		case li < 0:
			return false
		case lj < 0:
			return true
		}
		return li < lj
	})
	return &intersectPostings{arr: arr}
}

type intersectPostings struct {
	arr         []Postings
	cur         model.SeriesRef
	initialised bool
	err         error
}

func (it *intersectPostings) At() model.SeriesRef { return it.cur }
func (it *intersectPostings) Err() error          { return it.err }

// align advances every input to it.cur, raising cur whenever one overshoots,
// until they all agree. This is the leapfrog join: a ref missing from any
// input is skipped without visiting the refs in between.
func (it *intersectPostings) align() bool {
Loop:
	for {
		for _, p := range it.arr {
			if !p.Seek(it.cur) {
				it.err = p.Err()
				return false
			}
			if v := p.At(); v > it.cur {
				it.cur = v
				continue Loop
			}
		}
		return true
	}
}

func (it *intersectPostings) Next() bool {
	if it.err != nil {
		return false
	}
	if !it.arr[0].Next() {
		it.err = it.arr[0].Err()
		return false
	}
	it.initialised = true
	it.cur = it.arr[0].At()
	return it.align()
}

func (it *intersectPostings) Seek(v model.SeriesRef) bool {
	if it.err != nil {
		return false
	}
	// Guarded on initialised rather than on cur, because ref 0 is a legitimate
	// value and would otherwise be indistinguishable from "not started".
	if it.initialised && it.cur >= v {
		return true
	}
	it.initialised = true
	it.cur = v
	return it.align()
}

// Merge returns the union of the inputs.
func Merge(its ...Postings) Postings {
	switch len(its) {
	case 0:
		return EmptyPostings()
	case 1:
		return its[0]
	}

	h := make(postingsHeap, 0, len(its))
	total := 0
	for _, p := range its {
		if !p.Next() {
			if err := p.Err(); err != nil {
				return ErrPostings(err)
			}
			continue
		}
		if n := EstimateLen(p); n >= 0 && total >= 0 {
			total += n
		} else {
			total = -1
		}
		h = append(h, p)
	}
	if len(h) == 0 {
		return EmptyPostings()
	}
	return &mergedPostings{h: h, upperBound: total}
}

type postingsHeap []Postings

func (h postingsHeap) Len() int           { return len(h) }
func (h postingsHeap) Less(i, j int) bool { return h[i].At() < h[j].At() }
func (h postingsHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *postingsHeap) Push(x any)        { *h = append(*h, x.(Postings)) }
func (h *postingsHeap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[:n-1]
	return x
}

type mergedPostings struct {
	h           postingsHeap
	initialised bool
	cur         model.SeriesRef
	err         error

	// upperBound is the sum of input lengths, or -1 if unknown. It over-counts
	// refs that appear in several inputs, so it is only a hint for
	// intersection ordering, never a real length.
	upperBound int
}

func (it *mergedPostings) At() model.SeriesRef { return it.cur }
func (it *mergedPostings) Err() error          { return it.err }
func (it *mergedPostings) Len() int            { return it.upperBound }

func (it *mergedPostings) Next() bool {
	if it.err != nil || len(it.h) == 0 {
		return false
	}
	if !it.initialised {
		// Every input was already advanced once by Merge.
		heap.Init(&it.h)
		it.initialised = true
		it.cur = it.h[0].At()
		return true
	}

	// Drain every input sitting on the value just returned, which is what
	// deduplicates the union.
	for len(it.h) > 0 && it.h[0].At() == it.cur {
		if it.h[0].Next() {
			heap.Fix(&it.h, 0)
			continue
		}
		if err := it.h[0].Err(); err != nil {
			it.err = err
			return false
		}
		heap.Pop(&it.h)
	}
	if len(it.h) == 0 {
		return false
	}
	it.cur = it.h[0].At()
	return true
}

func (it *mergedPostings) Seek(v model.SeriesRef) bool {
	if it.err != nil {
		return false
	}
	if !it.initialised {
		if !it.Next() {
			return false
		}
	}
	if it.cur >= v {
		return true
	}
	for len(it.h) > 0 && it.h[0].At() < v {
		if it.h[0].Seek(v) {
			heap.Fix(&it.h, 0)
			continue
		}
		if err := it.h[0].Err(); err != nil {
			it.err = err
			return false
		}
		heap.Pop(&it.h)
	}
	if len(it.h) == 0 {
		return false
	}
	it.cur = it.h[0].At()
	return true
}

// Without returns the refs in full that are not in remove.
func Without(full, remove Postings) Postings {
	if IsEmpty(full) {
		return EmptyPostings()
	}
	if IsEmpty(remove) {
		return full
	}
	return &removedPostings{full: full, remove: remove}
}

type removedPostings struct {
	full, remove Postings

	initialised bool
	emitted     bool
	fullOK      bool
	removeOK    bool
	cur         model.SeriesRef
	err         error
}

func (rp *removedPostings) At() model.SeriesRef { return rp.cur }

func (rp *removedPostings) Err() error {
	if rp.err != nil {
		return rp.err
	}
	if err := rp.full.Err(); err != nil {
		return err
	}
	return rp.remove.Err()
}

func (rp *removedPostings) init() {
	if rp.initialised {
		return
	}
	rp.fullOK = rp.full.Next()
	rp.removeOK = rp.remove.Next()
	rp.initialised = true
}

// emit publishes the ref `full` currently sits on and steps `full` forward, so
// that after emit the iterator is positioned on the next candidate.
func (rp *removedPostings) emit() bool {
	rp.cur = rp.full.At()
	rp.emitted = true
	rp.fullOK = rp.full.Next()
	return true
}

// advance walks to the next ref of full that does not appear in remove.
func (rp *removedPostings) advance() bool {
	for {
		if !rp.fullOK {
			if err := rp.full.Err(); err != nil {
				rp.err = err
			}
			return false
		}
		// Exclusion list exhausted: everything left survives.
		if !rp.removeOK {
			if err := rp.remove.Err(); err != nil {
				rp.err = err
				return false
			}
			return rp.emit()
		}

		fcur, rcur := rp.full.At(), rp.remove.At()
		switch {
		case fcur < rcur:
			return rp.emit()
		case rcur < fcur:
			// Jump the exclusion list forward rather than stepping it, so a
			// long run of excluded refs costs one seek instead of many.
			rp.removeOK = rp.remove.Seek(fcur)
		default:
			// Present in both: drop it and move on.
			rp.fullOK = rp.full.Next()
			if rp.fullOK {
				rp.removeOK = rp.remove.Seek(rp.full.At())
			}
		}
	}
}

func (rp *removedPostings) Next() bool {
	if rp.err != nil {
		return false
	}
	rp.init()
	return rp.advance()
}

func (rp *removedPostings) Seek(v model.SeriesRef) bool {
	if rp.err != nil {
		return false
	}
	if rp.emitted && rp.cur >= v {
		return true
	}
	rp.init()
	if rp.fullOK && rp.full.At() < v {
		rp.fullOK = rp.full.Seek(v)
	}
	if rp.removeOK && rp.remove.At() < v {
		rp.removeOK = rp.remove.Seek(v)
	}
	return rp.advance()
}
