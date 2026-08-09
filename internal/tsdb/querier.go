package tsdb

import (
	"sort"

	"github.com/navingamage/stratum/internal/block"
	"github.com/navingamage/stratum/internal/chunk"
	"github.com/navingamage/stratum/internal/index"
	"github.com/navingamage/stratum/internal/memtable"
	"github.com/navingamage/stratum/internal/model"
)

// Series is one series' labels together with a way to walk its samples.
type Series interface {
	// Labels returns the series' label set.
	Labels() model.Labels

	// Iterator returns an iterator over the series' samples within the
	// querier's time range. A non-nil argument may be reused.
	Iterator(chunk.Iterator) chunk.Iterator
}

// SeriesSet is an iterator over series, in ascending label order.
type SeriesSet interface {
	Next() bool
	At() Series
	Err() error
}

// Querier reads a fixed time range of the database.
//
// A querier pins the blocks it needs open for its lifetime, so the storage
// underneath it cannot be compacted away mid-query. That is why Close matters
// and why queriers are meant to be short-lived: an abandoned one blocks disk
// reclamation.
type Querier interface {
	// Select returns the series matching every matcher.
	Select(matchers ...*model.Matcher) SeriesSet

	// LabelValues returns the values of a label across matching series.
	LabelValues(name string, matchers ...*model.Matcher) ([]string, error)

	// LabelNames returns every label name in range.
	LabelNames() ([]string, error)

	// Close releases the querier's hold on the underlying blocks.
	Close() error
}

// errSeriesSet is an already-failed set, so constructors can report errors
// without changing their signature.
type errSeriesSet struct{ err error }

func (e errSeriesSet) Next() bool { return false }
func (e errSeriesSet) At() Series { return nil }
func (e errSeriesSet) Err() error { return e.err }

// EmptySeriesSet returns a set with no series.
func EmptySeriesSet() SeriesSet { return errSeriesSet{} }

// ErrSeriesSet returns a set that reports err.
func ErrSeriesSet(err error) SeriesSet { return errSeriesSet{err} }

// chunkSeries is a series backed by a list of chunks and a time range.
type chunkSeries struct {
	labels     model.Labels
	chunks     []chunk.Chunk
	mint, maxt int64
}

func (s *chunkSeries) Labels() model.Labels { return s.labels }

func (s *chunkSeries) Iterator(it chunk.Iterator) chunk.Iterator {
	if ci, ok := it.(*chainIterator); ok && ci != nil {
		ci.reset(s.chunks, s.mint, s.maxt)
		return ci
	}
	ci := &chainIterator{}
	ci.reset(s.chunks, s.mint, s.maxt)
	return ci
}

// chainIterator walks a series' chunks in order, clipped to a time range.
//
// The clipping is done here rather than by the caller because chunks are
// selected whole: a chunk overlapping the range at all is loaded entirely,
// and the samples outside it must not reach the query engine or a rate()
// would be computed over a wider window than the user asked for.
type chainIterator struct {
	chunks     []chunk.Chunk
	mint, maxt int64

	i   int
	cur chunk.Iterator

	t   int64
	v   float64
	err error
}

func (it *chainIterator) reset(chunks []chunk.Chunk, mint, maxt int64) {
	it.chunks = chunks
	it.mint, it.maxt = mint, maxt
	it.i = -1
	it.cur = nil
	it.err = nil
}

func (it *chainIterator) At() (int64, float64) { return it.t, it.v }
func (it *chainIterator) Err() error           { return it.err }

func (it *chainIterator) Next() bool {
	if it.err != nil {
		return false
	}
	for {
		if it.cur == nil {
			it.i++
			if it.i >= len(it.chunks) {
				return false
			}
			it.cur = it.chunks[it.i].Iterator(it.cur)
			// Skip straight to the first sample that can be in range.
			if !it.cur.SeekTo(it.mint) {
				if err := it.cur.Err(); err != nil {
					it.err = err
					return false
				}
				it.cur = nil
				continue
			}
			t, v := it.cur.At()
			if t > it.maxt {
				// Chunks are time-ordered, so everything after this is later
				// still and the whole series is done.
				return false
			}
			it.t, it.v = t, v
			return true
		}

		if !it.cur.Next() {
			if err := it.cur.Err(); err != nil {
				it.err = err
				return false
			}
			it.cur = nil
			continue
		}
		t, v := it.cur.At()
		if t > it.maxt {
			return false
		}
		if t < it.mint {
			continue
		}
		it.t, it.v = t, v
		return true
	}
}

func (it *chainIterator) SeekTo(t int64) bool {
	if it.err != nil {
		return false
	}
	if it.i >= 0 && it.cur != nil && it.t >= t {
		return true
	}
	for it.Next() {
		if it.t >= t {
			return true
		}
	}
	return false
}

// blockSeriesSet iterates the series of one block matching a postings list.
type blockSeriesSet struct {
	b          *block.Block
	p          index.Postings
	mint, maxt int64

	cur *chunkSeries
	err error
}

func (s *blockSeriesSet) Next() bool {
	if s.err != nil {
		return false
	}
	for s.p.Next() {
		ls, chunks, err := s.b.SeriesChunks(s.p.At(), s.mint, s.maxt)
		if err != nil {
			s.err = err
			return false
		}
		if len(chunks) == 0 {
			continue
		}
		s.cur = &chunkSeries{labels: ls, chunks: chunks, mint: s.mint, maxt: s.maxt}
		return true
	}
	s.err = s.p.Err()
	return false
}

func (s *blockSeriesSet) At() Series { return s.cur }
func (s *blockSeriesSet) Err() error { return s.err }

// headSeriesSet iterates the head's series matching a postings list.
//
// Unlike a block, the head is mutable, so the set is materialised at
// construction: label sets and chunk lists for every matching series are
// captured up front.
//
// That is not laziness lost for nothing. A flush truncates the head as soon
// as its data is safely in a block, and a query that read chunk lists lazily
// would find series emptied out underneath it half-way through and silently
// return short results. Capturing the chunk references pins them - chunks are
// immutable once handed out, and the open chunk is already a copy - so the
// query sees one consistent instant of the head no matter what maintenance
// does next.
//
// The head is also a hash map rather than a sorted structure, so the matching
// series have to be sorted here anyway before they can merge with block
// results. The sort is over matched series only, not the whole head.
type headSeriesSet struct {
	series []*chunkSeries

	pos int
	err error
}

func newHeadSeriesSet(h *memtable.Head, p index.Postings, mint, maxt int64) SeriesSet {
	refs, err := index.ExpandPostings(p)
	if err != nil {
		return ErrSeriesSet(err)
	}

	out := make([]*chunkSeries, 0, len(refs))
	for _, ref := range refs {
		ls, ok := h.SeriesLabels(ref)
		if !ok {
			continue // truncated between the postings read and now
		}
		chunks := h.SeriesChunks(ref, mint, maxt)
		if len(chunks) == 0 {
			continue
		}
		out = append(out, &chunkSeries{labels: ls, chunks: chunks, mint: mint, maxt: maxt})
	}
	sort.Slice(out, func(i, j int) bool {
		return model.Compare(out[i].labels, out[j].labels) < 0
	})

	return &headSeriesSet{series: out, pos: -1}
}

func (s *headSeriesSet) Next() bool {
	s.pos++
	return s.pos < len(s.series)
}

func (s *headSeriesSet) At() Series { return s.series[s.pos] }
func (s *headSeriesSet) Err() error { return s.err }

// mergeSeriesSet merges several sets in label order, combining the samples of
// series that appear in more than one.
//
// Sets are given oldest-first, so where two carry the same timestamp for the
// same series the newer one wins - which is what makes a query correct while
// a compaction has published its output but not yet deleted its inputs.
type mergeSeriesSet struct {
	sets []SeriesSet

	// live holds the sets currently positioned on a series.
	live []SeriesSet
	// heads mirrors live: the series each is sitting on.
	heads []Series

	cur Series
	err error

	started bool
}

// NewMergeSeriesSet merges sets, which must be supplied oldest-first.
func NewMergeSeriesSet(sets ...SeriesSet) SeriesSet {
	switch len(sets) {
	case 0:
		return EmptySeriesSet()
	case 1:
		return sets[0]
	}
	return &mergeSeriesSet{sets: sets}
}

func (m *mergeSeriesSet) Next() bool {
	if m.err != nil {
		return false
	}
	if !m.started {
		m.started = true
		for _, s := range m.sets {
			if s.Next() {
				m.live = append(m.live, s)
				m.heads = append(m.heads, s.At())
				continue
			}
			if err := s.Err(); err != nil {
				m.err = err
				return false
			}
		}
	}
	if len(m.live) == 0 {
		return false
	}

	// Smallest label set across the positioned sets.
	best := 0
	for i := 1; i < len(m.heads); i++ {
		if model.Compare(m.heads[i].Labels(), m.heads[best].Labels()) < 0 {
			best = i
		}
	}
	want := m.heads[best].Labels()

	// Every set sitting on it contributes, in input order.
	var group []Series
	for i := 0; i < len(m.heads); i++ {
		if m.heads[i].Labels().Equal(want) {
			group = append(group, m.heads[i])
		}
	}

	if len(group) == 1 {
		m.cur = group[0]
	} else {
		m.cur = &mergedSeries{labels: want, parts: group}
	}

	// Advance the sets that contributed, dropping any that are exhausted.
	keptSets := m.live[:0]
	keptHeads := m.heads[:0]
	for i, s := range m.live {
		if !m.heads[i].Labels().Equal(want) {
			keptSets = append(keptSets, s)
			keptHeads = append(keptHeads, m.heads[i])
			continue
		}
		if s.Next() {
			keptSets = append(keptSets, s)
			keptHeads = append(keptHeads, s.At())
			continue
		}
		if err := s.Err(); err != nil {
			m.err = err
			return false
		}
	}
	m.live, m.heads = keptSets, keptHeads
	return true
}

func (m *mergeSeriesSet) At() Series { return m.cur }
func (m *mergeSeriesSet) Err() error { return m.err }

// mergedSeries is one series assembled from several sources.
type mergedSeries struct {
	labels model.Labels
	parts  []Series
}

func (s *mergedSeries) Labels() model.Labels { return s.labels }

func (s *mergedSeries) Iterator(chunk.Iterator) chunk.Iterator {
	its := make([]chunk.Iterator, 0, len(s.parts))
	for _, p := range s.parts {
		its = append(its, p.Iterator(nil))
	}
	return &sampleMergeIterator{its: its, ok: make([]bool, len(its))}
}

// sampleMergeIterator merges sample streams by timestamp, resolving ties in
// favour of the last input.
type sampleMergeIterator struct {
	its []chunk.Iterator
	ok  []bool

	t   int64
	v   float64
	err error

	started bool
}

func (m *sampleMergeIterator) At() (int64, float64) { return m.t, m.v }
func (m *sampleMergeIterator) Err() error           { return m.err }

func (m *sampleMergeIterator) Next() bool {
	if m.err != nil {
		return false
	}
	if !m.started {
		m.started = true
		for i, it := range m.its {
			m.ok[i] = it.Next()
			if err := it.Err(); err != nil {
				m.err = err
				return false
			}
		}
	}

	best := -1
	var bestT int64
	for i, ok := range m.ok {
		if !ok {
			continue
		}
		t, _ := m.its[i].At()
		if best < 0 || t < bestT {
			best, bestT = i, t
		}
	}
	if best < 0 {
		return false
	}

	m.t = bestT
	for i, ok := range m.ok {
		if !ok {
			continue
		}
		t, v := m.its[i].At()
		if t != bestT {
			continue
		}
		// Later inputs overwrite earlier ones at the same timestamp, and all
		// of them advance, which is what deduplicates.
		m.v = v
		m.ok[i] = m.its[i].Next()
		if err := m.its[i].Err(); err != nil {
			m.err = err
			return false
		}
	}
	return true
}

func (m *sampleMergeIterator) SeekTo(t int64) bool {
	if m.err != nil {
		return false
	}
	if m.started && m.t >= t {
		return true
	}
	for m.Next() {
		if m.t >= t {
			return true
		}
	}
	return false
}
