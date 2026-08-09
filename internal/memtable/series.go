package memtable

import (
	"math"
	"sync"

	"github.com/navingamage/stratum/internal/chunk"
	"github.com/navingamage/stratum/internal/model"
)

// memChunk is a sealed chunk of a series, with the time range it covers
// cached so that query-time chunk selection never has to decode anything.
type memChunk struct {
	chunk            chunk.Chunk
	minTime, maxTime int64
}

// overlaps reports whether the chunk can contribute to a time range.
func (c *memChunk) overlaps(mint, maxt int64) bool {
	return c.minTime <= maxt && mint <= c.maxTime
}

// memSeries is one series held in memory: a list of sealed chunks plus the
// open chunk currently being appended to.
//
// Each series carries its own mutex. A single lock over the whole head would
// serialise ingest across every series, and ingest is overwhelmingly the
// parallel case - thousands of independent series being appended to at once
// by different scrape targets.
type memSeries struct {
	mtx sync.Mutex

	ref    model.SeriesRef
	labels model.Labels

	sealed  []*memChunk
	head    *chunk.XORChunk
	headApp chunk.Appender

	// Time bounds over every chunk, sealed and open.
	minTime, maxTime int64

	// headMinTime is the first timestamp in the open chunk, used to decide
	// when the chunk has covered enough wall-clock time to be cut.
	headMinTime int64

	// pendingCommit marks a series with samples written to the WAL but not
	// yet visible. Truncation must not drop it.
	pendingCommit bool
}

func newMemSeries(ref model.SeriesRef, labels model.Labels) *memSeries {
	return &memSeries{
		ref:     ref,
		labels:  labels,
		minTime: math.MaxInt64,
		maxTime: math.MinInt64,
	}
}

// appendable reports whether a sample can be added. Chunks are strictly
// ordered, so a timestamp at or before the last one has to be rejected here;
// the head decides what to do about it.
func (s *memSeries) appendable(t int64) bool {
	return s.maxTime == math.MinInt64 || t > s.maxTime
}

// append adds a sample, cutting a new chunk when the current one is full
// enough. It assumes the caller holds s.mtx.
func (s *memSeries) append(t int64, v float64, chunkRange int64, samplesPerChunk int) error {
	if s.head == nil {
		s.cut(t)
	} else if s.shouldCut(t, chunkRange, samplesPerChunk) {
		s.seal()
		s.cut(t)
	}

	if err := s.headApp.Append(t, v); err != nil {
		return err
	}
	if t < s.minTime {
		s.minTime = t
	}
	if t > s.maxTime {
		s.maxTime = t
	}
	return nil
}

// shouldCut decides when the open chunk has had enough.
//
// Two limits, for two different reasons. The sample count bounds the linear
// scan inside a chunk iterator, which is what lets chunks get away with
// having no internal index. The time range bounds how much of a chunk a
// narrow query has to decode: without it, a series scraped once an hour
// would put a week of data in one chunk and a five-minute query would decode
// all of it.
func (s *memSeries) shouldCut(t int64, chunkRange int64, samplesPerChunk int) bool {
	if s.head.NumSamples() >= samplesPerChunk {
		return true
	}
	return t-s.headMinTime >= chunkRange
}

func (s *memSeries) cut(t int64) {
	s.head = chunk.NewXORChunk()
	app, err := s.head.Appender()
	if err != nil {
		// Only reachable if a fresh chunk reports itself sealed, which cannot
		// happen; a nil appender would panic later and much less clearly.
		panic("memtable: fresh chunk refused an appender: " + err.Error())
	}
	s.headApp = app
	s.headMinTime = t
}

// seal moves the open chunk into the sealed list.
func (s *memSeries) seal() {
	if s.head == nil || s.head.NumSamples() == 0 {
		return
	}
	s.sealed = append(s.sealed, &memChunk{
		chunk:   s.head,
		minTime: s.headMinTime,
		maxTime: s.maxTime,
	})
	s.head = nil
	s.headApp = nil
}

// chunksFor returns every chunk overlapping [mint, maxt]. It assumes the
// caller holds s.mtx, and the returned chunks stay safe to read after it is
// released.
//
// Sealed chunks are immutable, so they are shared as they are. The open chunk
// is not: it is copied, and the copy is wrapped as a sealed chunk.
//
// The copy is not optional. Chunk data is bit-packed, so appending sets bits
// inside the final byte of the buffer - a byte a reader is simultaneously
// entitled to read, because it holds samples already counted in the header.
// That is a genuine data race, not merely a stale read, and no amount of
// ordering on the sample count fixes it. Copying costs one allocation of at
// most a few hundred bytes per series per query, against the alternative of
// holding the series lock for the whole read and stalling ingest behind
// every query.
func (s *memSeries) chunksFor(mint, maxt int64) []chunk.Chunk {
	var out []chunk.Chunk
	for _, c := range s.sealed {
		if c.overlaps(mint, maxt) {
			out = append(out, c.chunk)
		}
	}

	if s.head != nil && s.head.NumSamples() > 0 &&
		s.headMinTime <= maxt && mint <= s.maxTime {
		src := s.head.Bytes()
		snapshot := make([]byte, len(src))
		copy(snapshot, src)

		c, err := chunk.FromData(chunk.EncXOR, snapshot)
		if err != nil {
			// Unreachable: the bytes came from a chunk this process built.
			panic("memtable: snapshot of the open chunk did not parse: " + err.Error())
		}
		out = append(out, c)
	}
	return out
}

// sampleCount returns the total number of samples held by the series.
func (s *memSeries) sampleCount() int {
	n := 0
	for _, c := range s.sealed {
		n += c.chunk.NumSamples()
	}
	if s.head != nil {
		n += s.head.NumSamples()
	}
	return n
}

// truncateBefore drops every chunk that ends before mint, returning how many
// went. It assumes the caller holds s.mtx.
//
// Chunks are dropped whole. A chunk straddling mint keeps its stale samples,
// which queries filter out by timestamp anyway; splitting one would mean
// re-encoding it, and the space is reclaimed a chunk-range later regardless.
func (s *memSeries) truncateBefore(mint int64) int {
	dropped := 0

	keep := 0
	for keep < len(s.sealed) && s.sealed[keep].maxTime < mint {
		keep++
	}
	if keep > 0 {
		// Copy into a fresh slice rather than resliced storage: a query
		// holding the old slice keeps a valid view, and the dropped chunks
		// become collectable instead of being pinned by the array.
		rest := make([]*memChunk, len(s.sealed)-keep)
		copy(rest, s.sealed[keep:])
		s.sealed = rest
		dropped += keep
	}

	// The open chunk goes too when all of it is below the floor. Without this
	// a series that stopped reporting keeps its final chunk - and therefore
	// its index entries - forever, which is exactly the churning-label-set
	// leak truncation exists to prevent.
	if s.head != nil && s.maxTime < mint {
		s.head = nil
		s.headApp = nil
		dropped++
	}

	switch {
	case len(s.sealed) > 0:
		s.minTime = s.sealed[0].minTime
	case s.head != nil:
		s.minTime = s.headMinTime
	default:
		s.minTime = math.MaxInt64
	}
	return dropped
}

// isEmpty reports whether the series holds no samples at all.
func (s *memSeries) isEmpty() bool {
	return len(s.sealed) == 0 && (s.head == nil || s.head.NumSamples() == 0)
}

// stripeSize is the number of independent shards in the series map. It is a
// power of two so the shard index is a mask rather than a division, and large
// enough that a few hundred concurrently-appending goroutines rarely collide.
const stripeSize = 1 << 9

// seriesMap is a hash map from label-set hash and from series ref to
// memSeries, sharded to keep concurrent appends from contending.
//
// Both lookups are needed and neither can be dropped. Ingest arrives with
// labels and needs the hash lookup; the WAL and query paths carry refs. They
// share a shard lock so that a series is created in both indexes atomically.
type seriesMap struct {
	shards [stripeSize]seriesShard
}

type seriesShard struct {
	mtx sync.RWMutex
	// byHash buckets by label-set hash. The value is a slice because hashes
	// collide, rarely; the slice is almost always length one.
	byHash map[uint64][]*memSeries
	byRef  map[model.SeriesRef]*memSeries
}

func newSeriesMap() *seriesMap {
	m := &seriesMap{}
	for i := range m.shards {
		m.shards[i].byHash = make(map[uint64][]*memSeries)
		m.shards[i].byRef = make(map[model.SeriesRef]*memSeries)
	}
	return m
}

func shardForHash(h uint64) int { return int(h & (stripeSize - 1)) }

// mix is the splitmix64 finaliser. Refs are handed out sequentially, so their
// low bits are a fine shard index on their own - but only while refs are
// dense. Mixing keeps the distribution independent of how refs happen to be
// allocated, for the cost of three instructions.
func mix(x uint64) uint64 {
	x ^= x >> 30
	x *= 0xbf58476d1ce4e5b9
	x ^= x >> 27
	x *= 0x94d049bb133111eb
	x ^= x >> 31
	return x
}

func shardForRef(r model.SeriesRef) int {
	return int(mix(uint64(r)) & (stripeSize - 1))
}

// getByHash returns the series with the given hash and labels.
func (m *seriesMap) getByHash(h uint64, ls model.Labels) *memSeries {
	sh := &m.shards[shardForHash(h)]
	sh.mtx.RLock()
	defer sh.mtx.RUnlock()

	for _, s := range sh.byHash[h] {
		if s.labels.Equal(ls) {
			return s
		}
	}
	return nil
}

// getByRef returns the series with the given ref.
func (m *seriesMap) getByRef(ref model.SeriesRef) *memSeries {
	sh := &m.shards[shardForRef(ref)]
	sh.mtx.RLock()
	defer sh.mtx.RUnlock()
	return sh.byRef[ref]
}

// getOrCreate returns the series for a label set, creating it if absent. The
// second result reports whether it was created.
//
// The double-check under the write lock matters: two goroutines appending to
// the same new series would otherwise both create one, and the loser's
// samples would vanish into an orphaned object that no query can reach.
func (m *seriesMap) getOrCreate(h uint64, ls model.Labels, newRef func() model.SeriesRef) (*memSeries, bool) {
	if s := m.getByHash(h, ls); s != nil {
		return s, false
	}

	sh := &m.shards[shardForHash(h)]
	sh.mtx.Lock()
	for _, s := range sh.byHash[h] {
		if s.labels.Equal(ls) {
			sh.mtx.Unlock()
			return s, false
		}
	}
	s := newMemSeries(newRef(), ls)
	sh.byHash[h] = append(sh.byHash[h], s)
	sh.mtx.Unlock()

	// The ref index lives in a different shard, so it is a separate
	// acquisition. A reader that finds the series by hash but not yet by ref
	// is harmless: refs only reach a caller after this returns.
	rs := &m.shards[shardForRef(s.ref)]
	rs.mtx.Lock()
	rs.byRef[s.ref] = s
	rs.mtx.Unlock()

	return s, true
}

// forEach calls fn for every series. It holds one shard lock at a time, so
// the walk sees a moving target rather than a snapshot - acceptable for the
// callers that use it (statistics, truncation, flushing a quiesced head).
func (m *seriesMap) forEach(fn func(*memSeries)) {
	for i := range m.shards {
		sh := &m.shards[i]
		sh.mtx.RLock()
		batch := make([]*memSeries, 0, len(sh.byRef))
		for _, s := range sh.byRef {
			batch = append(batch, s)
		}
		sh.mtx.RUnlock()

		for _, s := range batch {
			fn(s)
		}
	}
}

// delete removes a set of series from both indexes.
func (m *seriesMap) delete(refs map[model.SeriesRef]struct{}) {
	if len(refs) == 0 {
		return
	}
	for i := range m.shards {
		sh := &m.shards[i]
		sh.mtx.Lock()

		for ref := range refs {
			delete(sh.byRef, ref)
		}
		for h, list := range sh.byHash {
			kept := list[:0]
			for _, s := range list {
				if _, drop := refs[s.ref]; !drop {
					kept = append(kept, s)
				}
			}
			if len(kept) == 0 {
				delete(sh.byHash, h)
			} else {
				sh.byHash[h] = kept
			}
		}
		sh.mtx.Unlock()
	}
}

// count returns the number of series held.
func (m *seriesMap) count() int {
	n := 0
	for i := range m.shards {
		sh := &m.shards[i]
		sh.mtx.RLock()
		n += len(sh.byRef)
		sh.mtx.RUnlock()
	}
	return n
}
