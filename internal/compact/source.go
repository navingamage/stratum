// Package compact merges blocks into larger ones and flushes the head to
// disk.
//
// Both operations are the same operation: read series from one or more
// sources in label order, merge their samples, and write a new block. They go
// through one writer, so a block produced by a flush and a block produced by
// a compaction are structurally identical and there is only one format to
// test.
package compact

import (
	"container/heap"
	"fmt"
	"sort"

	"github.com/navingamage/stratum/internal/block"
	"github.com/navingamage/stratum/internal/chunk"
	"github.com/navingamage/stratum/internal/index"
	"github.com/navingamage/stratum/internal/memtable"
	"github.com/navingamage/stratum/internal/model"
)

// blockSource walks a block's series in label order.
//
// Series are stored sorted, so the dense series index is already label order
// and the walk needs no sorting at all.
type blockSource struct {
	b   *block.Block
	n   int
	pos int

	ls     model.Labels
	chunks []chunk.Chunk
	err    error
}

// NewBlockSource returns a source over an open block.
func NewBlockSource(b *block.Block) block.SeriesSource {
	return &blockSource{b: b, n: b.Index().NumSeries(), pos: -1}
}

func (s *blockSource) Symbols() []string { return s.b.Index().Symbols() }

func (s *blockSource) Next() bool {
	if s.err != nil {
		return false
	}
	s.pos++
	if s.pos >= s.n {
		return false
	}
	ls, chunks, err := s.b.SeriesChunks(model.SeriesRef(s.pos), model.MinTime, model.MaxTime)
	if err != nil {
		s.err = err
		return false
	}
	s.ls, s.chunks = ls, chunks
	return true
}

func (s *blockSource) At() (model.Labels, []chunk.Chunk) { return s.ls, s.chunks }
func (s *blockSource) Err() error                        { return s.err }

// headSource walks a head block's series in label order.
//
// The head is a hash map, so unlike a block it has no inherent order and the
// label sets have to be sorted up front. That costs one sort of the series
// set per flush, which is negligible against writing every sample out.
type headSource struct {
	h      *memtable.Head
	refs   []model.SeriesRef
	labels []model.Labels

	mint, maxt int64

	pos    int
	chunks []chunk.Chunk
	err    error
}

// NewHeadSource returns a source over the samples of h within [mint, maxt].
func NewHeadSource(h *memtable.Head, mint, maxt int64) (block.SeriesSource, error) {
	refs, err := index.ExpandPostings(h.Index().All())
	if err != nil {
		return nil, fmt.Errorf("compact: enumerating head series: %w", err)
	}

	s := &headSource{h: h, mint: mint, maxt: maxt, pos: -1}
	s.refs = make([]model.SeriesRef, 0, len(refs))
	s.labels = make([]model.Labels, 0, len(refs))

	type entry struct {
		ref model.SeriesRef
		ls  model.Labels
	}
	entries := make([]entry, 0, len(refs))
	for _, ref := range refs {
		ls, ok := h.SeriesLabels(ref)
		if !ok {
			// Truncated between listing the postings and reading the labels.
			continue
		}
		entries = append(entries, entry{ref, ls})
	}
	sort.Slice(entries, func(i, j int) bool {
		return model.Compare(entries[i].ls, entries[j].ls) < 0
	})
	for _, e := range entries {
		s.refs = append(s.refs, e.ref)
		s.labels = append(s.labels, e.ls)
	}
	return s, nil
}

func (s *headSource) Symbols() []string {
	set := make(map[string]struct{})
	for _, ls := range s.labels {
		for _, l := range ls {
			set[l.Name] = struct{}{}
			set[l.Value] = struct{}{}
		}
	}
	out := make([]string, 0, len(set))
	for k := range set {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func (s *headSource) Next() bool {
	for {
		s.pos++
		if s.pos >= len(s.refs) {
			return false
		}
		s.chunks = s.h.SeriesChunks(s.refs[s.pos], s.mint, s.maxt)
		if len(s.chunks) > 0 {
			return true
		}
		// A series with nothing in range contributes no chunks, and a series
		// with no chunks would be written as an empty index entry.
	}
}

func (s *headSource) At() (model.Labels, []chunk.Chunk) { return s.labels[s.pos], s.chunks }
func (s *headSource) Err() error                        { return s.err }

// mergeSource merges several sources into one stream in label order.
//
// Sources must be supplied oldest-first. Where two of them hold the same
// series at the same timestamp, the later source wins - a sample rewritten by
// a subsequent flush is the corrected one.
type mergeSource struct {
	sources []block.SeriesSource

	// h holds the sources currently positioned on a series, ordered by label.
	h sourceHeap

	ls     model.Labels
	chunks []chunk.Chunk
	err    error

	samplesPerChunk int
	started         bool
}

// NewMergeSource merges sources, re-chunking merged samples at
// samplesPerChunk. Sources must be given oldest-first.
func NewMergeSource(samplesPerChunk int, sources ...block.SeriesSource) block.SeriesSource {
	if samplesPerChunk <= 0 {
		samplesPerChunk = memtable.DefaultSamplesPerChunk
	}
	if len(sources) == 1 {
		return sources[0]
	}
	return &mergeSource{sources: sources, samplesPerChunk: samplesPerChunk}
}

func (m *mergeSource) Symbols() []string {
	set := make(map[string]struct{})
	for _, s := range m.sources {
		for _, sym := range s.Symbols() {
			set[sym] = struct{}{}
		}
	}
	out := make([]string, 0, len(set))
	for k := range set {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// sourceHeap orders positioned sources by their current label set, with ties
// broken by input order so that the oldest source is popped first and the
// newest therefore overwrites it during the sample merge.
type sourceHeap []*heapEntry

type heapEntry struct {
	src   block.SeriesSource
	order int
	ls    model.Labels
}

func (h sourceHeap) Len() int      { return len(h) }
func (h sourceHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h sourceHeap) Less(i, j int) bool {
	if c := model.Compare(h[i].ls, h[j].ls); c != 0 {
		return c < 0
	}
	return h[i].order < h[j].order
}
func (h *sourceHeap) Push(x any) { *h = append(*h, x.(*heapEntry)) }
func (h *sourceHeap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[:n-1]
	return x
}

func (m *mergeSource) Next() bool {
	if m.err != nil {
		return false
	}

	if !m.started {
		m.started = true
		for i, s := range m.sources {
			if s.Next() {
				ls, _ := s.At()
				m.h = append(m.h, &heapEntry{src: s, order: i, ls: ls})
				continue
			}
			if err := s.Err(); err != nil {
				m.err = err
				return false
			}
		}
		heap.Init(&m.h)
	}

	if len(m.h) == 0 {
		return false
	}

	// Collect every source sitting on the smallest label set.
	want := m.h[0].ls
	var group []*heapEntry
	for len(m.h) > 0 && m.h[0].ls.Equal(want) {
		e := heap.Pop(&m.h).(*heapEntry)
		group = append(group, e)
	}
	// Restore input order, so the newest source is applied last.
	sort.Slice(group, func(i, j int) bool { return group[i].order < group[j].order })

	var all []chunk.Chunk
	for _, e := range group {
		_, chunks := e.src.At()
		all = append(all, chunks...)
	}

	merged, err := rechunk(all, m.samplesPerChunk)
	if err != nil {
		m.err = err
		return false
	}
	m.ls, m.chunks = want, merged

	// Advance every source in the group and put back the ones with more.
	for _, e := range group {
		if e.src.Next() {
			ls, _ := e.src.At()
			e.ls = ls
			heap.Push(&m.h, e)
			continue
		}
		if err := e.src.Err(); err != nil {
			m.err = err
			return false
		}
	}
	return true
}

func (m *mergeSource) At() (model.Labels, []chunk.Chunk) { return m.ls, m.chunks }
func (m *mergeSource) Err() error                        { return m.err }

// rechunk merges the samples of several chunks into fresh chunks of at most
// samplesPerChunk samples each.
//
// A merge cannot simply concatenate chunk lists. Chunks from different blocks
// can overlap in time, and the chunk encoding requires strictly increasing
// timestamps, so overlapping input has to be merged sample by sample and
// re-encoded. When two chunks carry the same timestamp the later one wins.
//
// When the input is a single chunk list already in order - the common case,
// since most series live in exactly one block - the chunks are passed through
// untouched and nothing is decoded or re-encoded at all.
func rechunk(chunks []chunk.Chunk, samplesPerChunk int) ([]chunk.Chunk, error) {
	if len(chunks) == 0 {
		return nil, nil
	}
	if ok, err := alreadyOrdered(chunks); err != nil {
		return nil, err
	} else if ok {
		return chunks, nil
	}

	it, err := newMergeIterator(chunks)
	if err != nil {
		return nil, err
	}

	var (
		out  []chunk.Chunk
		cur  *chunk.XORChunk
		app  chunk.Appender
		n    int
		last int64
		any  bool
	)
	for it.Next() {
		t, v := it.At()
		if any && t <= last {
			// Duplicate timestamps are resolved inside the iterator, so
			// anything reaching here would be a bug in it.
			return nil, fmt.Errorf("compact: merged samples are not increasing: %d after %d", t, last)
		}

		if cur == nil || n >= samplesPerChunk {
			cur = chunk.NewXORChunk()
			app, err = cur.Appender()
			if err != nil {
				return nil, err
			}
			out = append(out, cur)
			n = 0
		}
		if err := app.Append(t, v); err != nil {
			return nil, fmt.Errorf("compact: re-encoding a merged sample: %w", err)
		}
		n++
		last, any = t, true
	}
	if err := it.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

// alreadyOrdered reports whether the chunks are non-overlapping and in
// ascending time order, so they can be reused without re-encoding.
func alreadyOrdered(chunks []chunk.Chunk) (bool, error) {
	var prevMax int64
	for i, c := range chunks {
		it := c.Iterator(nil)
		if !it.Next() {
			if err := it.Err(); err != nil {
				return false, err
			}
			return false, nil // an empty chunk; let the merge path drop it
		}
		mint, _ := it.At()
		maxt := mint
		for it.Next() {
			maxt, _ = it.At()
		}
		if err := it.Err(); err != nil {
			return false, err
		}
		if i > 0 && mint <= prevMax {
			return false, nil
		}
		prevMax = maxt
	}
	return true, nil
}

// mergeIterator merges several chunk iterators by timestamp, resolving ties
// in favour of the later chunk.
type mergeIterator struct {
	its []chunk.Iterator
	// ok[i] tracks whether its[i] is still positioned on a sample.
	ok []bool

	t   int64
	v   float64
	err error

	started bool
}

func newMergeIterator(chunks []chunk.Chunk) (*mergeIterator, error) {
	m := &mergeIterator{
		its: make([]chunk.Iterator, 0, len(chunks)),
		ok:  make([]bool, 0, len(chunks)),
	}
	for _, c := range chunks {
		m.its = append(m.its, c.Iterator(nil))
		m.ok = append(m.ok, false)
	}
	return m, nil
}

func (m *mergeIterator) At() (int64, float64) { return m.t, m.v }
func (m *mergeIterator) Err() error           { return m.err }

func (m *mergeIterator) Next() bool {
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

	// Find the smallest timestamp still available.
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

	// Take the value from the last iterator holding this timestamp - the
	// later chunk came from the newer source - and advance all of them, which
	// is what deduplicates.
	m.t = bestT
	for i, ok := range m.ok {
		if !ok {
			continue
		}
		t, v := m.its[i].At()
		if t != bestT {
			continue
		}
		m.v = v
		m.ok[i] = m.its[i].Next()
		if err := m.its[i].Err(); err != nil {
			m.err = err
			return false
		}
	}
	return true
}
