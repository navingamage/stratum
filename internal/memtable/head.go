// Package memtable implements the head block: the in-memory, writable tier of
// the storage engine.
//
// Samples arrive here, are made durable in the write-ahead log, and are held
// in compressed chunks until the head has accumulated enough of a time range
// to be flushed into an immutable on-disk block. Queries read the head and
// the persisted blocks through the same interfaces, so the boundary is not
// visible to anything above.
package memtable

import (
	"errors"
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/navingamage/stratum/internal/chunk"
	"github.com/navingamage/stratum/internal/index"
	"github.com/navingamage/stratum/internal/model"
	"github.com/navingamage/stratum/internal/wal"
)

// Errors returned by the head.
var (
	// ErrOutOfOrderSample reports a sample older than the series' newest.
	// Ordered chunks are what make the encoding and the query path cheap, so
	// late samples are refused rather than absorbed.
	ErrOutOfOrderSample = errors.New("memtable: sample is out of order")

	// ErrOutOfBounds reports a sample older than the head's retained window.
	// It is distinct from out-of-order because it usually means a
	// misconfigured clock or a replaying agent, not a racing appender.
	ErrOutOfBounds = errors.New("memtable: sample is outside the head's time range")

	// ErrInvalidSample reports a sample the head will never accept: an
	// unusable label set, or a NaN timestamp equivalent.
	ErrInvalidSample = errors.New("memtable: invalid sample")

	// ErrAppenderClosed reports use of an appender after Commit or Rollback.
	ErrAppenderClosed = errors.New("memtable: appender already finished")
)

// Options configures a head block.
type Options struct {
	// ChunkRange is the time span a single chunk may cover, in milliseconds.
	// Zero selects two hours.
	ChunkRange int64

	// SamplesPerChunk is the sample count at which a chunk is cut. Zero
	// selects 120.
	SamplesPerChunk int

	// WAL is the log to record appends in. Nil disables durability, which is
	// useful for tests and for a head built during compaction.
	WAL *wal.WAL
}

// Defaults for Options.
const (
	DefaultChunkRange      = int64(2 * 60 * 60 * 1000) // two hours
	DefaultSamplesPerChunk = 120
)

func (o Options) withDefaults() Options {
	if o.ChunkRange <= 0 {
		o.ChunkRange = DefaultChunkRange
	}
	if o.SamplesPerChunk <= 0 {
		o.SamplesPerChunk = DefaultSamplesPerChunk
	}
	return o
}

// Head is the writable, in-memory block.
type Head struct {
	opts Options

	series   *seriesMap
	postings *index.MemPostings

	lastRef atomic.Uint64

	// minTime and maxTime bound everything the head holds. They are atomic
	// because a query reads them without taking any lock - a query that races
	// an append and misses the newest sample is fine, one that blocks ingest
	// to find out is not.
	minTime atomic.Int64
	maxTime atomic.Int64

	// minValidTime is the floor for accepted samples, raised by truncation.
	minValidTime atomic.Int64

	numSamples atomic.Uint64

	// walBuf recycles the sample-encoding buffers used by appenders.
	walBuf sync.Pool
}

// NewHead returns an empty head block.
func NewHead(opts Options) *Head {
	h := &Head{
		opts:     opts.withDefaults(),
		series:   newSeriesMap(),
		postings: index.NewMemPostings(),
	}
	h.minTime.Store(math.MaxInt64)
	h.maxTime.Store(math.MinInt64)
	h.minValidTime.Store(math.MinInt64)
	h.walBuf.New = func() any {
		b := make([]byte, 0, 4096)
		return &b
	}
	return h
}

// MinTime returns the earliest timestamp held, or MaxInt64 when empty.
func (h *Head) MinTime() int64 { return h.minTime.Load() }

// MaxTime returns the latest timestamp held, or MinInt64 when empty.
func (h *Head) MaxTime() int64 { return h.maxTime.Load() }

// NumSeries returns the number of series held.
func (h *Head) NumSeries() int { return h.series.count() }

// NumSamples returns the number of samples appended since the head was
// created, excluding any dropped by truncation.
func (h *Head) NumSamples() uint64 { return h.numSamples.Load() }

// Index returns the head's inverted index.
func (h *Head) Index() *index.MemPostings { return h.postings }

// SeriesLabels returns the label set for a ref.
func (h *Head) SeriesLabels(ref model.SeriesRef) (model.Labels, bool) {
	s := h.series.getByRef(ref)
	if s == nil {
		return nil, false
	}
	return s.labels, true
}

// updateBounds widens the head's time range to include t.
func (h *Head) updateBounds(t int64) {
	for {
		cur := h.minTime.Load()
		if t >= cur || h.minTime.CompareAndSwap(cur, t) {
			break
		}
	}
	for {
		cur := h.maxTime.Load()
		if t <= cur || h.maxTime.CompareAndSwap(cur, t) {
			break
		}
	}
}

// Appender accumulates samples and applies them atomically.
//
// The two phases exist to get the durability ordering right. Append only
// buffers; Commit writes the WAL record first and only then makes the samples
// visible to queries. Doing it the other way round would let a query observe
// a sample that a crash a millisecond later would erase, which is a worse
// failure than losing the write outright - a caller can retry a lost write,
// but nothing can un-answer a query.
type Appender interface {
	// Append adds a sample. If ref is non-zero and names a known series the
	// labels are ignored, which turns the steady-state append into a map
	// lookup by integer instead of a label-set hash.
	Append(ref model.SeriesRef, ls model.Labels, t int64, v float64) (model.SeriesRef, error)

	// Commit makes every buffered sample durable and then visible.
	Commit() error

	// Rollback discards every buffered sample.
	Rollback() error
}

// Appender returns a new appender over the head.
func (h *Head) Appender() Appender {
	return &headAppender{
		head:    h,
		minTime: h.minValidTime.Load(),
	}
}

type headAppender struct {
	head *Head

	minTime int64
	done    bool

	// samples buffered for this transaction.
	samples []wal.RefSample
	// series created by this transaction, which must reach the WAL before any
	// sample that references them.
	newSeries []wal.RefSeries
	// held is the set of series touched, kept so Rollback can clear their
	// pendingCommit flags.
	held []*memSeries

	// pendingMaxT tracks the newest buffered timestamp per series. A series'
	// own maxTime does not move until Commit, so without this two samples with
	// the same timestamp in one transaction would both pass the ordering check
	// and the second would then be rejected half-way through Commit - after
	// earlier samples had already been applied and logged. Screening here
	// keeps Commit unable to fail on ordering.
	pendingMaxT map[model.SeriesRef]int64
}

func (a *headAppender) Append(ref model.SeriesRef, ls model.Labels, t int64, v float64) (model.SeriesRef, error) {
	if a.done {
		return 0, ErrAppenderClosed
	}
	if t < a.minTime {
		return 0, fmt.Errorf("%w: %d is before the head's floor of %d", ErrOutOfBounds, t, a.minTime)
	}

	s := a.head.series.getByRef(ref)
	if s == nil {
		if err := ls.Validate(); err != nil {
			return 0, fmt.Errorf("%w: %v", ErrInvalidSample, err)
		}
		var created bool
		s, created = a.head.getOrCreateSeries(ls)
		if created {
			a.newSeries = append(a.newSeries, wal.RefSeries{Ref: s.ref, Labels: s.labels})
		}
	}

	// Check ordering before the WAL write, so a rejected sample never reaches
	// the log. Recording it and then refusing it would make replay produce a
	// head that ingest itself would not have accepted.
	s.mtx.Lock()
	last := s.maxTime
	ok := s.appendable(t)
	if ok && !s.pendingCommit {
		s.pendingCommit = true
		a.held = append(a.held, s)
	}
	s.mtx.Unlock()

	// Also order against samples this transaction has already buffered.
	if pending, seen := a.pendingMaxT[s.ref]; seen {
		if t <= pending {
			ok = false
			last = pending
		}
	}

	if !ok {
		return 0, fmt.Errorf("%w: %d is not after %d for %s", ErrOutOfOrderSample, t, last, s.labels)
	}

	if a.pendingMaxT == nil {
		a.pendingMaxT = make(map[model.SeriesRef]int64)
	}
	a.pendingMaxT[s.ref] = t

	a.samples = append(a.samples, wal.RefSample{Ref: s.ref, T: t, V: v})
	return s.ref, nil
}

func (a *headAppender) Commit() error {
	if a.done {
		return ErrAppenderClosed
	}
	a.done = true

	if err := a.log(); err != nil {
		a.release()
		return err
	}

	// Durable; now make it visible.
	for _, smpl := range a.samples {
		s := a.head.series.getByRef(smpl.Ref)
		if s == nil {
			// The series was truncated away between Append and Commit. The
			// sample is already in the WAL, but replay would drop it for the
			// same reason, so the two stay consistent.
			continue
		}
		s.mtx.Lock()
		err := s.append(smpl.T, smpl.V, a.head.opts.ChunkRange, a.head.opts.SamplesPerChunk)
		s.mtx.Unlock()

		if err != nil {
			// The only expected failure is a chunk-level ordering rejection,
			// which Append already screened for; anything reaching here is a
			// bug worth surfacing rather than swallowing.
			return fmt.Errorf("memtable: applying committed sample for %s: %w", s.labels, err)
		}
		a.head.updateBounds(smpl.T)
		a.head.numSamples.Add(1)
	}

	a.release()
	return nil
}

// log writes the transaction to the WAL. Series definitions go first, so a
// replay never meets a sample whose series it has not yet seen.
func (a *headAppender) log() error {
	w := a.head.opts.WAL
	if w == nil || (len(a.newSeries) == 0 && len(a.samples) == 0) {
		return nil
	}

	var enc wal.Encoder
	recs := make([][]byte, 0, 2)

	// Series records only appear when a label set is seen for the first time,
	// which after warm-up is rare, so this buffer is not worth pooling. It
	// must also be distinct from the samples buffer: Log holds both until it
	// returns.
	if len(a.newSeries) > 0 {
		recs = append(recs, enc.Series(a.newSeries, nil))
	}

	// The samples record is built on every commit, so its buffer is recycled.
	// The pool holds pointers rather than slices: pooling a slice value boxes
	// the header on every Put, which is the allocation this is meant to avoid.
	if len(a.samples) > 0 {
		bp := a.head.walBuf.Get().(*[]byte)
		defer a.head.walBuf.Put(bp)

		*bp = enc.Samples(a.samples, (*bp)[:0])
		recs = append(recs, *bp)
	}

	if err := w.Log(recs...); err != nil {
		return fmt.Errorf("memtable: writing to the log: %w", err)
	}
	return nil
}

func (a *headAppender) Rollback() error {
	if a.done {
		return ErrAppenderClosed
	}
	a.done = true
	a.release()
	return nil
}

func (a *headAppender) release() {
	for _, s := range a.held {
		s.mtx.Lock()
		s.pendingCommit = false
		s.mtx.Unlock()
	}
	a.samples = nil
	a.newSeries = nil
	a.held = nil
	a.pendingMaxT = nil
}

// getOrCreateSeries returns the series for a label set, indexing it if new.
func (h *Head) getOrCreateSeries(ls model.Labels) (*memSeries, bool) {
	hash := ls.Hash()
	s, created := h.series.getOrCreate(hash, ls, func() model.SeriesRef {
		return model.SeriesRef(h.lastRef.Add(1))
	})
	if created {
		h.postings.Add(s.ref, s.labels)
	}
	return s, created
}

// getOrCreateWithRef is the replay path: it recreates a series at the exact
// ref the log recorded, so that sample records referring to it resolve.
func (h *Head) getOrCreateWithRef(ref model.SeriesRef, ls model.Labels) *memSeries {
	if s := h.series.getByRef(ref); s != nil {
		return s
	}
	s, created := h.series.getOrCreate(ls.Hash(), ls, func() model.SeriesRef { return ref })
	if created {
		h.postings.Add(s.ref, s.labels)
		// Keep the counter ahead of every ref seen, so refs handed out after
		// replay cannot collide with recovered ones.
		for {
			cur := h.lastRef.Load()
			if uint64(ref) <= cur || h.lastRef.CompareAndSwap(cur, uint64(ref)) {
				break
			}
		}
	}
	return s
}

// ReplayStats summarises what a WAL replay recovered.
type ReplayStats struct {
	Series  int           `json:"series"`
	Samples int           `json:"samples"`
	Dropped int           `json:"dropped"`
	Elapsed time.Duration `json:"elapsed"`
}

// Replay rebuilds the head from a write-ahead log directory.
//
// Samples are applied directly rather than through an Appender: they are
// already durable, and routing them back through the WAL would double the log
// on every restart.
func (h *Head) Replay(dir string) (ReplayStats, error) {
	start := time.Now()
	var stats ReplayStats

	r, err := wal.NewReplayer(dir)
	if err != nil {
		return stats, fmt.Errorf("memtable: opening the log for replay: %w", err)
	}
	defer r.Close()

	var (
		dec        wal.Decoder
		seriesBuf  []wal.RefSeries
		samplesBuf []wal.RefSample
	)

	for r.Next() {
		rec := r.Record()
		switch dec.Type(rec) {
		case wal.RecordSeries:
			seriesBuf, err = dec.Series(rec, seriesBuf[:0])
			if err != nil {
				return stats, fmt.Errorf("memtable: decoding a series record: %w", err)
			}
			for _, rs := range seriesBuf {
				// The decoder already copies label strings out of the replay
				// buffer, so these are safe to retain.
				h.getOrCreateWithRef(rs.Ref, rs.Labels)
				stats.Series++
			}

		case wal.RecordSamples:
			samplesBuf, err = dec.Samples(rec, samplesBuf[:0])
			if err != nil {
				return stats, fmt.Errorf("memtable: decoding a samples record: %w", err)
			}
			for _, smpl := range samplesBuf {
				s := h.series.getByRef(smpl.Ref)
				if s == nil {
					// A sample for a series whose definition never made it to
					// disk. The definition is written first and in the same
					// batch, so this means the log was cut between the two -
					// the sample is unrecoverable but was never acknowledged.
					stats.Dropped++
					continue
				}
				s.mtx.Lock()
				if !s.appendable(smpl.T) {
					s.mtx.Unlock()
					stats.Dropped++
					continue
				}
				err := s.append(smpl.T, smpl.V, h.opts.ChunkRange, h.opts.SamplesPerChunk)
				s.mtx.Unlock()
				if err != nil {
					return stats, fmt.Errorf("memtable: replaying a sample: %w", err)
				}
				h.updateBounds(smpl.T)
				h.numSamples.Add(1)
				stats.Samples++
			}

		case wal.RecordTombstones:
			// Deletions are applied by the block layer during compaction; the
			// head simply carries them forward.

		default:
			return stats, fmt.Errorf("memtable: unrecognised record type in the log")
		}
	}
	if err := r.Err(); err != nil {
		return stats, fmt.Errorf("memtable: replaying the log: %w", err)
	}

	stats.Elapsed = time.Since(start)
	return stats, nil
}

// Truncate drops every sample before mint and raises the floor for new ones.
// Series left with no data at all are removed from the index entirely, which
// is what keeps the head from leaking memory on churning label sets.
func (h *Head) Truncate(mint int64) error {
	if cur := h.minValidTime.Load(); mint <= cur {
		return nil
	}
	h.minValidTime.Store(mint)

	dropped := make(map[model.SeriesRef]struct{})
	newMin := int64(math.MaxInt64)

	h.series.forEach(func(s *memSeries) {
		s.mtx.Lock()
		defer s.mtx.Unlock()

		s.truncateBefore(mint)
		if s.isEmpty() && !s.pendingCommit {
			dropped[s.ref] = struct{}{}
			return
		}
		if s.minTime < newMin {
			newMin = s.minTime
		}
	})

	h.series.delete(dropped)
	h.postings.Delete(dropped)

	h.minTime.Store(newMin)
	if newMin == math.MaxInt64 {
		h.maxTime.Store(math.MinInt64)
	}
	return nil
}

// SeriesChunks returns the chunks of a series overlapping [mint, maxt].
func (h *Head) SeriesChunks(ref model.SeriesRef, mint, maxt int64) []chunk.Chunk {
	s := h.series.getByRef(ref)
	if s == nil {
		return nil
	}
	s.mtx.Lock()
	defer s.mtx.Unlock()
	return s.chunksFor(mint, maxt)
}

// Stats summarises the head, for the status endpoint and for deciding when a
// flush is due.
type Stats struct {
	Series     int         `json:"series"`
	Samples    uint64      `json:"samples"`
	Chunks     int         `json:"chunks"`
	MinTime    int64       `json:"minTime"`
	MaxTime    int64       `json:"maxTime"`
	IndexStats index.Stats `json:"index"`
}

// Stats computes the current head statistics.
func (h *Head) Stats() Stats {
	s := Stats{
		Series:     h.series.count(),
		Samples:    h.numSamples.Load(),
		MinTime:    h.minTime.Load(),
		MaxTime:    h.maxTime.Load(),
		IndexStats: h.postings.Stats(),
	}
	h.series.forEach(func(ms *memSeries) {
		ms.mtx.Lock()
		s.Chunks += len(ms.sealed)
		if ms.head != nil && ms.head.NumSamples() > 0 {
			s.Chunks++
		}
		ms.mtx.Unlock()
	})
	return s
}
