// Package tsdb ties the storage layers into a database: a write-ahead log, a
// writable head block, immutable on-disk blocks, and the background work that
// moves data between them.
package tsdb

import (
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/navingamage/stratum/internal/block"
	"github.com/navingamage/stratum/internal/compact"
	"github.com/navingamage/stratum/internal/index"
	"github.com/navingamage/stratum/internal/memtable"
	"github.com/navingamage/stratum/internal/model"
	"github.com/navingamage/stratum/internal/wal"
)

// WALDirName is the write-ahead log subdirectory of a data directory.
const WALDirName = "wal"

// ErrClosed is returned once the database has been closed.
var ErrClosed = errors.New("tsdb: database is closed")

// Options configures a database.
type Options struct {
	// BlockDuration is the time span the head accumulates before being
	// flushed to a block. Zero selects two hours.
	BlockDuration int64

	// SamplesPerChunk is the sample count at which a chunk is cut. Zero
	// selects the head's default.
	SamplesPerChunk int

	// Retention drops blocks entirely older than this. Zero keeps everything.
	Retention time.Duration

	// WALSync selects the log's durability policy.
	WALSync wal.SyncPolicy

	// NoWAL disables the write-ahead log entirely. Ingest is faster and
	// unacknowledged data is lost on a crash; useful for tests and for
	// rebuildable data.
	NoWAL bool

	// BackgroundInterval is how often the maintenance loop runs. Zero selects
	// one minute.
	BackgroundInterval time.Duration

	// Logger receives operational events. Zero uses the default logger.
	Logger *slog.Logger
}

func (o Options) withDefaults() Options {
	if o.BlockDuration <= 0 {
		o.BlockDuration = memtable.DefaultChunkRange
	}
	if o.SamplesPerChunk <= 0 {
		o.SamplesPerChunk = memtable.DefaultSamplesPerChunk
	}
	if o.BackgroundInterval <= 0 {
		o.BackgroundInterval = time.Minute
	}
	if o.Logger == nil {
		o.Logger = slog.Default()
	}
	return o
}

// DB is an open time-series database.
type DB struct {
	dir  string
	opts Options
	log  *slog.Logger

	// mtx guards head, blocks and closed. It is held only for the pointer
	// swaps that publish a new head or a new block list, never for the work
	// of reading or writing data, so ingest and queries do not contend on it.
	mtx    sync.RWMutex
	head   *memtable.Head
	blocks []*liveBlock
	closed bool

	wal       *wal.WAL
	compactor *compact.Compactor

	// compactMtx serialises background maintenance against itself and against
	// an explicit Compact call.
	compactMtx sync.Mutex

	stopc chan struct{}
	donec chan struct{}
}

// Open opens or creates a database in dir.
func Open(dir string, opts Options) (*DB, error) {
	o := opts.withDefaults()

	if err := os.MkdirAll(dir, 0o777); err != nil {
		return nil, fmt.Errorf("tsdb: creating %s: %w", dir, err)
	}
	// Partial blocks left by an interrupted write are removed before anything
	// tries to open them.
	if err := block.CleanTmpDirs(dir); err != nil {
		return nil, fmt.Errorf("tsdb: cleaning partial blocks: %w", err)
	}

	db := &DB{
		dir:   dir,
		opts:  o,
		log:   o.Logger,
		stopc: make(chan struct{}),
		donec: make(chan struct{}),
	}

	if err := db.openBlocks(); err != nil {
		return nil, err
	}

	walDir := filepath.Join(dir, WALDirName)
	headOpts := memtable.Options{
		ChunkRange:      o.BlockDuration,
		SamplesPerChunk: o.SamplesPerChunk,
	}

	if !o.NoWAL {
		// The log is opened first and the head replayed into it afterwards.
		// Opening adds a fresh empty segment, which replay reads as empty and
		// skips; replaying first would mean either building a head with no
		// log attached and then rebuilding it, or replaying twice.
		//
		// Replay applies samples to the head directly rather than through an
		// Appender, so recovering does not write the log back into itself.
		w, err := wal.Open(walDir, wal.Options{Sync: o.WALSync, Logger: o.Logger})
		if err != nil {
			db.closeBlocks()
			return nil, fmt.Errorf("tsdb: opening the log: %w", err)
		}
		db.wal = w
		headOpts.WAL = w
		db.head = memtable.NewHead(headOpts)

		stats, err := db.head.Replay(walDir)
		if err != nil {
			w.Close()
			db.closeBlocks()
			return nil, fmt.Errorf("tsdb: replaying the log: %w", err)
		}
		if stats.Series > 0 || stats.Samples > 0 {
			db.log.Info("replayed the write-ahead log",
				"series", stats.Series, "samples", stats.Samples,
				"dropped", stats.Dropped, "elapsed", stats.Elapsed)
		}
	} else {
		db.head = memtable.NewHead(headOpts)
	}

	db.compactor = compact.New(dir, compact.Options{
		SamplesPerChunk: o.SamplesPerChunk,
		Retention:       o.Retention,
		Logger:          o.Logger,
	})

	go db.backgroundLoop()
	return db, nil
}

// openBlocks opens every complete block in the data directory.
func (db *DB) openBlocks() error {
	dirs, err := block.List(db.dir)
	if err != nil {
		return fmt.Errorf("tsdb: listing blocks: %w", err)
	}
	for _, d := range dirs {
		b, err := block.Open(d)
		if err != nil {
			db.closeBlocks()
			return fmt.Errorf("tsdb: opening block %s: %w", d, err)
		}
		db.blocks = append(db.blocks, newLiveBlock(b))
	}
	if len(db.blocks) > 0 {
		db.log.Info("opened blocks", "count", len(db.blocks))
	}
	return nil
}

func (db *DB) closeBlocks() {
	for _, b := range db.blocks {
		b.unpin()
	}
	db.blocks = nil
}

// liveBlock is an open block with a reference count.
//
// Compaction deletes a block's files as soon as its output is published, but a
// query started a moment earlier may still be reading them - and because
// blocks are memory-mapped, unmapping underneath that query is not a stale
// read, it is a segmentation fault. The database holds one reference, each
// querier holds another, and the mapping is released only when the last one
// goes.
type liveBlock struct {
	*block.Block
	refs atomic.Int64
}

func newLiveBlock(b *block.Block) *liveBlock {
	lb := &liveBlock{Block: b}
	lb.refs.Store(1) // the database's own reference
	return lb
}

func (l *liveBlock) pin() { l.refs.Add(1) }

func (l *liveBlock) unpin() {
	if n := l.refs.Add(-1); n == 0 {
		l.Block.Close()
	}
}

// Dir returns the database's directory.
func (db *DB) Dir() string { return db.dir }

// Head returns the writable head block.
func (db *DB) Head() *memtable.Head {
	db.mtx.RLock()
	defer db.mtx.RUnlock()
	return db.head
}

// Appender returns an appender over the head.
func (db *DB) Appender() memtable.Appender {
	db.mtx.RLock()
	defer db.mtx.RUnlock()
	return db.head.Appender()
}

// Querier returns a querier over [mint, maxt].
//
// The returned querier holds the blocks it needs open. Closing it is what
// allows compaction to reclaim their files, so a leaked querier shows up as a
// data directory that never shrinks.
func (db *DB) Querier(mint, maxt int64) (Querier, error) {
	db.mtx.RLock()
	defer db.mtx.RUnlock()

	if db.closed {
		return nil, ErrClosed
	}

	q := &dbQuerier{
		head: db.head,
		mint: mint,
		maxt: maxt,
	}
	// Pinning happens under the read lock, and reloadBlocks needs the write
	// lock to swap the slice, so a block cannot be dropped between being seen
	// here and being pinned.
	for _, b := range db.blocks {
		if b.Overlaps(mint, maxt) {
			b.pin()
			q.blocks = append(q.blocks, b)
		}
	}
	return q, nil
}

// Blocks returns the metadata of the currently open blocks.
func (db *DB) Blocks() []block.Meta {
	db.mtx.RLock()
	defer db.mtx.RUnlock()

	out := make([]block.Meta, 0, len(db.blocks))
	for _, b := range db.blocks {
		out = append(out, b.Meta())
	}
	return out
}

// backgroundLoop runs maintenance on a timer.
func (db *DB) backgroundLoop() {
	defer close(db.donec)

	t := time.NewTicker(db.opts.BackgroundInterval)
	defer t.Stop()

	for {
		select {
		case <-db.stopc:
			return
		case <-t.C:
			if err := db.Maintain(); err != nil {
				db.log.Error("background maintenance failed", "err", err)
			}
		}
	}
}

// Maintain flushes the head if it is due, compacts blocks and applies
// retention. It is exported so tests and operators can force a pass.
func (db *DB) Maintain() error {
	db.compactMtx.Lock()
	defer db.compactMtx.Unlock()

	if err := db.flushHeadIfDue(); err != nil {
		return err
	}
	if err := db.compactOnce(); err != nil {
		return err
	}
	return db.applyRetention()
}

// flushHeadIfDue writes the head to a block once it spans more than one block
// duration, and truncates what it wrote.
//
// The head is flushed up to a boundary rather than in its entirety: samples
// are still arriving for the current period, and a block covering a range
// that the head will keep extending would overlap the next flush.
func (db *DB) flushHeadIfDue() error {
	db.mtx.RLock()
	head := db.head
	db.mtx.RUnlock()

	mint, maxt := head.MinTime(), head.MaxTime()
	if mint > maxt {
		return nil // empty head
	}
	if maxt-mint < db.opts.BlockDuration {
		return nil
	}

	// Flush everything below the aligned boundary containing the newest
	// sample, leaving the in-progress period in memory.
	boundary := (maxt / db.opts.BlockDuration) * db.opts.BlockDuration
	if boundary <= mint {
		return nil
	}

	// Stop accepting samples below the boundary before reading the head.
	// Reversed, a sample committed between the read and the truncation would
	// be acknowledged and then discarded.
	head.RaiseFloor(boundary)

	meta, err := db.compactor.Flush(head, mint, boundary-1)
	if err != nil {
		return fmt.Errorf("tsdb: flushing the head: %w", err)
	}
	if meta == nil {
		return nil
	}

	if err := db.reloadBlocks(); err != nil {
		return err
	}

	// Only now is it safe to drop the samples from memory: they are in a
	// block that queries can already see.
	if err := head.Truncate(boundary); err != nil {
		return fmt.Errorf("tsdb: truncating the head: %w", err)
	}

	// Everything below the boundary is in a block, so the log segments that
	// carried it are redundant. The current segment is never removed.
	if db.wal != nil {
		if err := db.wal.NextSegment(); err != nil {
			return fmt.Errorf("tsdb: cutting a log segment: %w", err)
		}
		first, last, err := wal.Segments(filepath.Join(db.dir, WALDirName))
		if err != nil {
			return err
		}
		if first >= 0 && last > first {
			if err := db.wal.Truncate(last); err != nil {
				return fmt.Errorf("tsdb: truncating the log: %w", err)
			}
		}
	}
	return nil
}

// compactOnce performs at most one compaction.
func (db *DB) compactOnce() error {
	metas := db.blockMetas()
	plan := db.compactor.Plan(metas)
	if plan == nil {
		return nil
	}
	if _, err := db.compactor.Compact(plan); err != nil {
		return fmt.Errorf("tsdb: compacting: %w", err)
	}
	return db.reloadBlocks()
}

func (db *DB) applyRetention() error {
	if db.opts.Retention <= 0 {
		return nil
	}
	n, err := db.compactor.ApplyRetention(db.blockMetas(), time.Now())
	if err != nil {
		return fmt.Errorf("tsdb: applying retention: %w", err)
	}
	if n == 0 {
		return nil
	}
	return db.reloadBlocks()
}

func (db *DB) blockMetas() []*block.Meta {
	db.mtx.RLock()
	defer db.mtx.RUnlock()

	out := make([]*block.Meta, 0, len(db.blocks))
	for _, b := range db.blocks {
		m := b.Meta()
		out = append(out, &m)
	}
	return out
}

// reloadBlocks re-reads the data directory and swaps in the new block set.
//
// Blocks that are still present are reused rather than reopened, so a
// compaction does not invalidate the mappings of everything it did not touch.
// Blocks that have gone are closed after the swap, so an in-flight query that
// captured the old slice keeps reading valid memory until it finishes.
func (db *DB) reloadBlocks() error {
	dirs, err := block.List(db.dir)
	if err != nil {
		return fmt.Errorf("tsdb: listing blocks: %w", err)
	}
	wanted := make(map[string]struct{}, len(dirs))
	for _, d := range dirs {
		wanted[d] = struct{}{}
	}

	db.mtx.RLock()
	existing := make(map[string]*liveBlock, len(db.blocks))
	for _, b := range db.blocks {
		existing[b.Dir()] = b
	}
	db.mtx.RUnlock()

	var (
		next    []*liveBlock
		opened  []*liveBlock
		failure error
	)
	for _, d := range dirs {
		if b, ok := existing[d]; ok {
			next = append(next, b)
			continue
		}
		b, err := block.Open(d)
		if err != nil {
			failure = fmt.Errorf("tsdb: opening block %s: %w", d, err)
			break
		}
		lb := newLiveBlock(b)
		opened = append(opened, lb)
		next = append(next, lb)
	}
	if failure != nil {
		for _, b := range opened {
			b.unpin()
		}
		return failure
	}

	db.mtx.Lock()
	previous := db.blocks
	db.blocks = next
	db.mtx.Unlock()

	// Dropping the database's reference. Any querier still holding one keeps
	// the mapping alive until it closes.
	for _, b := range previous {
		if _, keep := wanted[b.Dir()]; !keep {
			b.unpin()
		}
	}
	return nil
}

// Compact forces a flush of the whole head and compacts what results. It is
// what Close uses, and what an operator runs before taking a backup.
func (db *DB) Compact() error {
	db.compactMtx.Lock()
	defer db.compactMtx.Unlock()

	db.mtx.RLock()
	head := db.head
	db.mtx.RUnlock()

	mint, maxt := head.MinTime(), head.MaxTime()
	if mint <= maxt {
		head.RaiseFloor(maxt + 1)
		meta, err := db.compactor.Flush(head, mint, maxt)
		if err != nil {
			return fmt.Errorf("tsdb: flushing the head: %w", err)
		}
		if meta != nil {
			if err := db.reloadBlocks(); err != nil {
				return err
			}
			if err := head.Truncate(maxt + 1); err != nil {
				return fmt.Errorf("tsdb: truncating the head: %w", err)
			}
		}
	}
	return db.compactOnce()
}

// Close stops background work, flushes the head and releases every resource.
func (db *DB) Close() error {
	db.mtx.Lock()
	if db.closed {
		db.mtx.Unlock()
		return ErrClosed
	}
	db.closed = true
	db.mtx.Unlock()

	close(db.stopc)
	<-db.donec

	var first error

	// Flushing on close is a convenience, not a correctness requirement: the
	// log already holds everything, and a crash instead of a clean shutdown
	// recovers the same data by replay.
	if err := db.Compact(); err != nil {
		db.log.Error("flushing the head during shutdown failed", "err", err)
		first = err
	}

	if db.wal != nil {
		if err := db.wal.Close(); err != nil && first == nil {
			first = err
		}
	}

	db.mtx.Lock()
	db.closeBlocks()
	db.mtx.Unlock()

	return first
}

// Stats summarises the database, for the status endpoint.
type Stats struct {
	Head       memtable.Stats `json:"head"`
	NumBlocks  int            `json:"numBlocks"`
	BlockBytes int64          `json:"blockBytes"`
	MinTime    int64          `json:"minTime"`
	MaxTime    int64          `json:"maxTime"`
}

// Stats computes the current statistics.
func (db *DB) Stats() Stats {
	db.mtx.RLock()
	defer db.mtx.RUnlock()

	s := Stats{
		Head:      db.head.Stats(),
		NumBlocks: len(db.blocks),
		MinTime:   db.head.MinTime(),
		MaxTime:   db.head.MaxTime(),
	}
	for _, b := range db.blocks {
		m := b.Meta()
		if m.MinTime < s.MinTime {
			s.MinTime = m.MinTime
		}
		if m.MaxTime > s.MaxTime {
			s.MaxTime = m.MaxTime
		}
		s.BlockBytes += int64(b.Chunks().Size())
	}
	return s
}

// dbQuerier answers queries over the head plus a fixed set of blocks.
type dbQuerier struct {
	head       *memtable.Head
	blocks     []*liveBlock
	mint, maxt int64
	closed     bool
}

func (q *dbQuerier) Select(matchers ...*model.Matcher) SeriesSet {
	// Blocks first, oldest to newest, then the head. The merge resolves a
	// tied timestamp in favour of the last input, so the head - which holds
	// the most recent version of anything - wins.
	sets := make([]SeriesSet, 0, len(q.blocks)+1)

	for _, b := range q.blocks {
		p, err := index.PostingsForMatchers(b.Index(), matchers...)
		if err != nil {
			return ErrSeriesSet(err)
		}
		if index.IsEmpty(p) {
			continue
		}
		sets = append(sets, &blockSeriesSet{b: b.Block, p: p, mint: q.mint, maxt: q.maxt})
	}

	hp, err := index.PostingsForMatchers(q.head.Index(), matchers...)
	if err != nil {
		return ErrSeriesSet(err)
	}
	if !index.IsEmpty(hp) {
		sets = append(sets, newHeadSeriesSet(q.head, hp, q.mint, q.maxt))
	}

	return NewMergeSeriesSet(sets...)
}

func (q *dbQuerier) LabelValues(name string, matchers ...*model.Matcher) ([]string, error) {
	seen := make(map[string]struct{})

	add := func(vals []string) {
		for _, v := range vals {
			seen[v] = struct{}{}
		}
	}

	if len(matchers) == 0 {
		for _, b := range q.blocks {
			add(b.Index().LabelValues(name))
		}
		add(q.head.Index().LabelValues(name))
	} else {
		// With matchers the values have to come from the series that match,
		// so this walks them rather than reading the label index.
		set := q.Select(matchers...)
		for set.Next() {
			if v := set.At().Labels().Get(name); v != "" {
				seen[v] = struct{}{}
			}
		}
		if err := set.Err(); err != nil {
			return nil, err
		}
	}

	out := make([]string, 0, len(seen))
	for v := range seen {
		out = append(out, v)
	}
	sort.Strings(out)
	return out, nil
}

func (q *dbQuerier) LabelNames() ([]string, error) {
	seen := make(map[string]struct{})
	for _, b := range q.blocks {
		for _, n := range b.Index().LabelNames() {
			seen[n] = struct{}{}
		}
	}
	for _, n := range q.head.Index().LabelNames() {
		seen[n] = struct{}{}
	}

	out := make([]string, 0, len(seen))
	for n := range seen {
		out = append(out, n)
	}
	sort.Strings(out)
	return out, nil
}

// Close drops the querier's references to the blocks it pinned. Until this
// runs, compaction can delete a block's directory but its mapping stays valid,
// so an in-flight query keeps reading consistent data.
func (q *dbQuerier) Close() error {
	if q.closed {
		return nil
	}
	q.closed = true
	for _, b := range q.blocks {
		b.unpin()
	}
	q.blocks = nil
	return nil
}
