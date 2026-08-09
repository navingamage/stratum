package tsdb

import (
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/navingamage/stratum/internal/block"
	"github.com/navingamage/stratum/internal/memtable"
	"github.com/navingamage/stratum/internal/model"
	"github.com/navingamage/stratum/internal/wal"
)

// quietLogger keeps test output readable; the database logs every flush.
func quietLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func testOptions() Options {
	return Options{
		BlockDuration:      10_000,
		SamplesPerChunk:    8,
		WALSync:            wal.SyncNever,
		BackgroundInterval: time.Hour, // maintenance is driven explicitly
		Logger:             quietLogger(),
	}
}

func openTestDB(t *testing.T, dir string, opts Options) *DB {
	t.Helper()
	db, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() {
		if err := db.Close(); err != nil && !errors.Is(err, ErrClosed) {
			t.Errorf("Close: %v", err)
		}
	})
	return db
}

func seriesLabels(i int) model.Labels {
	return model.FromStrings(
		model.MetricName, "cpu",
		"host", fmt.Sprintf("web-%02d", i),
		"env", map[bool]string{true: "prod", false: "staging"}[i%2 == 0],
	)
}

// appendBatch commits one sample for each of n series at time ts.
func appendBatch(t *testing.T, db *DB, n int, ts int64, valueFor func(i int) float64) {
	t.Helper()
	app := db.Appender()
	for i := 0; i < n; i++ {
		if _, err := app.Append(0, seriesLabels(i), ts, valueFor(i)); err != nil {
			t.Fatalf("Append(series %d, t %d): %v", i, ts, err)
		}
	}
	if err := app.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}
}

// collect drains a series set into a map from label string to samples.
func collect(t *testing.T, set SeriesSet) map[string][]model.Sample {
	t.Helper()
	out := make(map[string][]model.Sample)
	for set.Next() {
		s := set.At()
		var samples []model.Sample
		it := s.Iterator(nil)
		for it.Next() {
			ts, v := it.At()
			samples = append(samples, model.Sample{T: ts, V: v})
		}
		if err := it.Err(); err != nil {
			t.Fatalf("sample iterator: %v", err)
		}
		out[s.Labels().String()] = samples
	}
	if err := set.Err(); err != nil {
		t.Fatalf("series set: %v", err)
	}
	return out
}

func query(t *testing.T, db *DB, mint, maxt int64, ms ...*model.Matcher) map[string][]model.Sample {
	t.Helper()
	q, err := db.Querier(mint, maxt)
	if err != nil {
		t.Fatalf("Querier: %v", err)
	}
	defer q.Close()
	return collect(t, q.Select(ms...))
}

func TestDBAppendAndQuery(t *testing.T) {
	db := openTestDB(t, t.TempDir(), testOptions())

	const (
		series  = 10
		samples = 50
	)
	for j := 0; j < samples; j++ {
		appendBatch(t, db, series, int64(j)*1000, func(i int) float64 {
			return float64(i*1000 + j)
		})
	}

	got := query(t, db, model.MinTime, model.MaxTime,
		model.MustNewMatcher(model.MatchEqual, model.MetricName, "cpu"))
	if len(got) != series {
		t.Fatalf("query returned %d series, want %d", len(got), series)
	}
	for i := 0; i < series; i++ {
		key := seriesLabels(i).String()
		s := got[key]
		if len(s) != samples {
			t.Fatalf("series %s has %d samples, want %d", key, len(s), samples)
		}
		for j, smpl := range s {
			if smpl.T != int64(j)*1000 || smpl.V != float64(i*1000+j) {
				t.Fatalf("series %s sample %d = %v", key, j, smpl)
			}
		}
	}
}

func TestDBQueryTimeRangeIsClipped(t *testing.T) {
	db := openTestDB(t, t.TempDir(), testOptions())
	for j := 0; j < 100; j++ {
		appendBatch(t, db, 1, int64(j)*1000, func(int) float64 { return float64(j) })
	}

	// Chunks hold 8 samples, so a narrow window straddles chunk boundaries
	// and the samples outside it must still be excluded.
	got := query(t, db, 20_000, 25_000,
		model.MustNewMatcher(model.MatchEqual, model.MetricName, "cpu"))

	samples := got[seriesLabels(0).String()]
	if len(samples) != 6 {
		t.Fatalf("got %d samples for [20000, 25000], want 6: %v", len(samples), samples)
	}
	if samples[0].T != 20_000 || samples[len(samples)-1].T != 25_000 {
		t.Errorf("range is [%d, %d], want [20000, 25000]",
			samples[0].T, samples[len(samples)-1].T)
	}
}

func TestDBMatcherSelection(t *testing.T) {
	db := openTestDB(t, t.TempDir(), testOptions())
	appendBatch(t, db, 10, 1000, func(i int) float64 { return float64(i) })

	got := query(t, db, model.MinTime, model.MaxTime,
		model.MustNewMatcher(model.MatchEqual, "env", "prod"))
	if len(got) != 5 {
		t.Errorf("env=prod matched %d series, want 5", len(got))
	}

	got = query(t, db, model.MinTime, model.MaxTime,
		model.MustNewMatcher(model.MatchRegexp, "host", "web-0[12]"))
	if len(got) != 2 {
		t.Errorf("regexp matched %d series, want 2", len(got))
	}

	got = query(t, db, model.MinTime, model.MaxTime,
		model.MustNewMatcher(model.MatchEqual, model.MetricName, "nosuch"))
	if len(got) != 0 {
		t.Errorf("a non-matching query returned %d series", len(got))
	}
}

func TestDBSeriesSetIsSortedByLabels(t *testing.T) {
	db := openTestDB(t, t.TempDir(), testOptions())
	appendBatch(t, db, 20, 1000, func(i int) float64 { return float64(i) })

	q, err := db.Querier(model.MinTime, model.MaxTime)
	if err != nil {
		t.Fatal(err)
	}
	defer q.Close()

	set := q.Select(model.MustNewMatcher(model.MatchEqual, model.MetricName, "cpu"))
	var prev model.Labels
	n := 0
	for set.Next() {
		ls := set.At().Labels()
		if prev != nil && model.Compare(prev, ls) >= 0 {
			t.Fatalf("series set is not sorted: %s then %s", prev, ls)
		}
		prev = ls.Copy()
		n++
	}
	if err := set.Err(); err != nil {
		t.Fatal(err)
	}
	if n != 20 {
		t.Errorf("got %d series, want 20", n)
	}
}

func TestDBLabelNamesAndValues(t *testing.T) {
	db := openTestDB(t, t.TempDir(), testOptions())
	appendBatch(t, db, 6, 1000, func(i int) float64 { return float64(i) })

	q, err := db.Querier(model.MinTime, model.MaxTime)
	if err != nil {
		t.Fatal(err)
	}
	defer q.Close()

	names, err := q.LabelNames()
	if err != nil {
		t.Fatal(err)
	}
	want := []string{model.MetricName, "env", "host"}
	if len(names) != len(want) {
		t.Fatalf("LabelNames() = %v, want %v", names, want)
	}
	for i := range want {
		if names[i] != want[i] {
			t.Errorf("LabelNames() = %v, want %v", names, want)
			break
		}
	}

	values, err := q.LabelValues("env")
	if err != nil {
		t.Fatal(err)
	}
	if len(values) != 2 || values[0] != "prod" || values[1] != "staging" {
		t.Errorf("LabelValues(env) = %v, want [prod staging]", values)
	}

	// Restricted by a matcher, the values must come from matching series only.
	values, err = q.LabelValues("host", model.MustNewMatcher(model.MatchEqual, "env", "prod"))
	if err != nil {
		t.Fatal(err)
	}
	if len(values) != 3 {
		t.Errorf("LabelValues(host, env=prod) = %v, want 3 values", values)
	}
}

// TestDBFlushToBlock covers the head crossing a block boundary: the data must
// move to disk and stay queryable across the transition.
func TestDBFlushToBlock(t *testing.T) {
	dir := t.TempDir()
	db := openTestDB(t, dir, testOptions())

	// BlockDuration is 10s, so 30s of data spans three blocks' worth.
	const samples = 30
	for j := 0; j < samples; j++ {
		appendBatch(t, db, 5, int64(j)*1000, func(i int) float64 {
			return float64(i*100 + j)
		})
	}

	if len(db.Blocks()) != 0 {
		t.Fatalf("blocks exist before any maintenance: %d", len(db.Blocks()))
	}

	if err := db.Maintain(); err != nil {
		t.Fatalf("Maintain: %v", err)
	}
	blocks := db.Blocks()
	if len(blocks) == 0 {
		t.Fatal("Maintain produced no blocks for 30s of data at a 10s block duration")
	}

	// Everything must still be visible, now spanning a block and the head.
	got := query(t, db, model.MinTime, model.MaxTime,
		model.MustNewMatcher(model.MatchEqual, model.MetricName, "cpu"))
	if len(got) != 5 {
		t.Fatalf("query returned %d series after flushing, want 5", len(got))
	}
	for i := 0; i < 5; i++ {
		s := got[seriesLabels(i).String()]
		if len(s) != samples {
			t.Fatalf("series %d has %d samples after flushing, want %d", i, len(s), samples)
		}
		for j, smpl := range s {
			if smpl.T != int64(j)*1000 || smpl.V != float64(i*100+j) {
				t.Fatalf("series %d sample %d = %v, want (%d, %v)",
					i, j, smpl, int64(j)*1000, float64(i*100+j))
			}
		}
	}
}

func TestDBHeadTruncatedAfterFlush(t *testing.T) {
	db := openTestDB(t, t.TempDir(), testOptions())
	for j := 0; j < 40; j++ {
		appendBatch(t, db, 2, int64(j)*1000, func(int) float64 { return float64(j) })
	}

	before := db.Head().MinTime()
	if err := db.Maintain(); err != nil {
		t.Fatal(err)
	}
	after := db.Head().MinTime()

	if after <= before {
		t.Errorf("head min time did not advance after a flush: %d then %d", before, after)
	}
	// The flushed range must be gone from memory but present on disk.
	blocks := db.Blocks()
	if len(blocks) == 0 {
		t.Fatal("no blocks after a flush")
	}

	// The head is expected to retain some overlap with the block. Chunks are
	// dropped whole, so the chunk straddling the flush boundary stays, and
	// the query merge deduplicates it. What must not happen is the head
	// holding the *whole* flushed range - that would mean truncation did
	// nothing.
	blockMax := blocks[len(blocks)-1].MaxTime
	if db.Head().MinTime() > blockMax {
		t.Errorf("head min time %d is past the last block's max time %d; the overlap should be one chunk, not none",
			db.Head().MinTime(), blockMax)
	}
	if db.Head().MinTime() <= blocks[0].MinTime {
		t.Errorf("head min time %d did not advance past the first block's start %d",
			db.Head().MinTime(), blocks[0].MinTime)
	}
}

// TestDBRestartRecoversUnflushedData is the durability property at the
// database level: a restart must not lose anything a commit accepted.
func TestDBRestartRecoversUnflushedData(t *testing.T) {
	dir := t.TempDir()

	db, err := Open(dir, testOptions())
	if err != nil {
		t.Fatal(err)
	}

	const (
		series  = 8
		samples = 25
	)
	for j := 0; j < samples; j++ {
		appendBatch(t, db, series, int64(j)*1000, func(i int) float64 {
			return float64(i*10 + j)
		})
	}
	want := query(t, db, model.MinTime, model.MaxTime,
		model.MustNewMatcher(model.MatchEqual, model.MetricName, "cpu"))

	// Close without an explicit compaction, so the recovery path is what is
	// under test rather than the flush.
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	db2 := openTestDB(t, dir, testOptions())
	got := query(t, db2, model.MinTime, model.MaxTime,
		model.MustNewMatcher(model.MatchEqual, model.MetricName, "cpu"))

	if len(got) != len(want) {
		t.Fatalf("after restart got %d series, want %d", len(got), len(want))
	}
	for key, wantSamples := range want {
		gotSamples := got[key]
		if len(gotSamples) != len(wantSamples) {
			t.Fatalf("series %s has %d samples after restart, want %d",
				key, len(gotSamples), len(wantSamples))
		}
		for i := range wantSamples {
			if gotSamples[i] != wantSamples[i] {
				t.Fatalf("series %s sample %d = %v, want %v",
					key, i, gotSamples[i], wantSamples[i])
			}
		}
	}
}

// TestDBRestartAfterCrash simulates a process killed without a clean close:
// the WAL is left as-is and the database must recover from it.
func TestDBRestartAfterCrash(t *testing.T) {
	dir := t.TempDir()

	opts := testOptions()
	opts.WALSync = wal.SyncAlways // acknowledged writes must be on disk

	db, err := Open(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	for j := 0; j < 20; j++ {
		appendBatch(t, db, 4, int64(j)*1000, func(i int) float64 {
			return float64(i*10 + j)
		})
	}

	// Simulate a crash: stop the background loop, so nothing flushes the head
	// to a block or truncates the log. Recovery then has to come entirely from
	// replay, which is the point of the test.
	close(db.stopc)
	<-db.donec

	// Release the log's file handle without flushing anything. With
	// SyncAlways every acknowledged write is already on disk, so this loses no
	// data and stays faithful to the crash - but it does hand the descriptor
	// back, which Windows requires before the directory can be removed.
	if err := db.wal.Close(); err != nil {
		t.Fatalf("releasing the log: %v", err)
	}

	db2 := openTestDB(t, dir, opts)
	got := query(t, db2, model.MinTime, model.MaxTime,
		model.MustNewMatcher(model.MatchEqual, model.MetricName, "cpu"))

	if len(got) != 4 {
		t.Fatalf("recovered %d series, want 4", len(got))
	}
	for i := 0; i < 4; i++ {
		s := got[seriesLabels(i).String()]
		if len(s) != 20 {
			t.Fatalf("series %d recovered %d samples, want 20", i, len(s))
		}
		for j, smpl := range s {
			if smpl.T != int64(j)*1000 || smpl.V != float64(i*10+j) {
				t.Fatalf("series %d sample %d = %v", i, j, smpl)
			}
		}
	}
}

func TestDBRestartWithBlocksAndWAL(t *testing.T) {
	dir := t.TempDir()

	db, err := Open(dir, testOptions())
	if err != nil {
		t.Fatal(err)
	}
	// Enough to flush a block, then more that stays in the head.
	for j := 0; j < 40; j++ {
		appendBatch(t, db, 3, int64(j)*1000, func(i int) float64 { return float64(i*100 + j) })
	}
	if err := db.Maintain(); err != nil {
		t.Fatal(err)
	}
	if len(db.Blocks()) == 0 {
		t.Fatal("no blocks after maintenance")
	}
	for j := 40; j < 50; j++ {
		appendBatch(t, db, 3, int64(j)*1000, func(i int) float64 { return float64(i*100 + j) })
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	db2 := openTestDB(t, dir, testOptions())
	got := query(t, db2, model.MinTime, model.MaxTime,
		model.MustNewMatcher(model.MatchEqual, model.MetricName, "cpu"))
	if len(got) != 3 {
		t.Fatalf("recovered %d series, want 3", len(got))
	}
	for i := 0; i < 3; i++ {
		s := got[seriesLabels(i).String()]
		if len(s) != 50 {
			t.Fatalf("series %d has %d samples across blocks and the head, want 50", i, len(s))
		}
		for j, smpl := range s {
			if smpl.T != int64(j)*1000 || smpl.V != float64(i*100+j) {
				t.Fatalf("series %d sample %d = %v, want (%d, %v)",
					i, j, smpl, int64(j)*1000, float64(i*100+j))
			}
		}
	}
}

func TestDBCompactMergesBlocks(t *testing.T) {
	dir := t.TempDir()
	db := openTestDB(t, dir, testOptions())

	for j := 0; j < 60; j++ {
		appendBatch(t, db, 3, int64(j)*1000, func(i int) float64 { return float64(i*100 + j) })
		if j%12 == 11 {
			if err := db.Maintain(); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := db.Compact(); err != nil {
		t.Fatalf("Compact: %v", err)
	}

	blocks := db.Blocks()
	if len(blocks) == 0 {
		t.Fatal("no blocks after compaction")
	}

	// The data must survive whatever shape compaction chose.
	got := query(t, db, model.MinTime, model.MaxTime,
		model.MustNewMatcher(model.MatchEqual, model.MetricName, "cpu"))
	for i := 0; i < 3; i++ {
		s := got[seriesLabels(i).String()]
		if len(s) != 60 {
			t.Fatalf("series %d has %d samples after compaction, want 60", i, len(s))
		}
		for j, smpl := range s {
			if smpl.T != int64(j)*1000 || smpl.V != float64(i*100+j) {
				t.Fatalf("series %d sample %d = %v", i, j, smpl)
			}
		}
	}
}

func TestDBRetention(t *testing.T) {
	dir := t.TempDir()
	old := time.Now().UnixMilli() - int64(6*time.Hour/time.Millisecond)

	// Phase one: build blocks with retention disabled, so the flush is
	// observable on its own.
	func() {
		db := openTestDB(t, dir, testOptions())
		for j := 0; j < 20; j++ {
			appendBatch(t, db, 2, old+int64(j)*1000, func(int) float64 { return 1 })
		}
		if err := db.Maintain(); err != nil {
			t.Fatal(err)
		}
		if len(db.Blocks()) == 0 {
			t.Fatal("no blocks were produced")
		}
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
	}()

	// Phase two: reopen with a retention window the data falls outside of.
	opts := testOptions()
	opts.Retention = time.Hour

	db := openTestDB(t, dir, opts)
	if len(db.Blocks()) == 0 {
		t.Fatal("the blocks did not survive the reopen")
	}
	if err := db.Maintain(); err != nil {
		t.Fatal(err)
	}
	if got := len(db.Blocks()); got != 0 {
		t.Errorf("%d blocks survived a 1h retention window on 6h-old data", got)
	}

	// The files must actually be gone, not merely closed.
	dirs, err := block.List(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(dirs) != 0 {
		t.Errorf("%d block directories remain on disk", len(dirs))
	}
}

// TestDBQuerierPinsBlocks is the concurrency property that keeps compaction
// from unmapping a block underneath a running query. Because blocks are
// memory-mapped, getting this wrong is a segmentation fault rather than a
// stale read.
func TestDBQuerierPinsBlocks(t *testing.T) {
	dir := t.TempDir()
	db := openTestDB(t, dir, testOptions())

	for j := 0; j < 40; j++ {
		appendBatch(t, db, 5, int64(j)*1000, func(i int) float64 { return float64(i*100 + j) })
	}
	if err := db.Maintain(); err != nil {
		t.Fatal(err)
	}
	if len(db.Blocks()) == 0 {
		t.Fatal("no blocks to pin")
	}

	// Open a querier and start reading.
	q, err := db.Querier(model.MinTime, model.MaxTime)
	if err != nil {
		t.Fatal(err)
	}
	set := q.Select(model.MustNewMatcher(model.MatchEqual, model.MetricName, "cpu"))
	if !set.Next() {
		t.Fatal("the query returned no series")
	}

	// Compact underneath it, which deletes the block directories the querier
	// is reading from.
	if err := db.Compact(); err != nil {
		t.Fatal(err)
	}

	// The in-flight query must still be able to read every sample.
	total := 0
	it := set.At().Iterator(nil)
	for it.Next() {
		total++
	}
	if err := it.Err(); err != nil {
		t.Fatalf("reading a pinned block after compaction: %v", err)
	}
	for set.Next() {
		it := set.At().Iterator(nil)
		for it.Next() {
			total++
		}
		if err := it.Err(); err != nil {
			t.Fatalf("reading a pinned block after compaction: %v", err)
		}
	}
	if err := set.Err(); err != nil {
		t.Fatal(err)
	}
	if total != 5*40 {
		t.Errorf("read %d samples through a pinned querier, want %d", total, 5*40)
	}

	if err := q.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestDBConcurrentAppendAndQuery(t *testing.T) {
	db := openTestDB(t, t.TempDir(), testOptions())

	const (
		writers = 4
		perW    = 200
	)
	var accepted, rejected atomic.Int64
	var wg sync.WaitGroup
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			ls := model.FromStrings(model.MetricName, "cpu", "worker", fmt.Sprint(w))
			for i := 0; i < perW; i++ {
				app := db.Appender()
				_, err := app.Append(0, ls, int64(i)*1000, float64(i))
				if err != nil {
					// A concurrent flush can raise the head floor past a
					// straggler's timestamp. Being told so is the correct
					// outcome; the guarantee under test is that a sample is
					// never both acknowledged and lost.
					if errors.Is(err, memtable.ErrOutOfBounds) {
						app.Rollback()
						rejected.Add(1)
						continue
					}
					t.Errorf("writer %d: %v", w, err)
					app.Rollback()
					return
				}
				if err := app.Commit(); err != nil {
					if errors.Is(err, memtable.ErrOutOfBounds) {
						rejected.Add(1)
						continue
					}
					t.Errorf("writer %d commit: %v", w, err)
					return
				}
				accepted.Add(1)
			}
		}(w)
	}

	stop := make(chan struct{})
	var readers sync.WaitGroup
	for r := 0; r < 3; r++ {
		readers.Add(1)
		go func() {
			defer readers.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				q, err := db.Querier(model.MinTime, model.MaxTime)
				if err != nil {
					t.Errorf("Querier: %v", err)
					return
				}
				set := q.Select(model.MustNewMatcher(model.MatchEqual, model.MetricName, "cpu"))
				for set.Next() {
					it := set.At().Iterator(nil)
					for it.Next() {
					}
					if err := it.Err(); err != nil {
						t.Errorf("iterator: %v", err)
						q.Close()
						return
					}
				}
				if err := set.Err(); err != nil {
					t.Errorf("series set: %v", err)
					q.Close()
					return
				}
				q.Close()
			}
		}()
	}

	// Maintenance concurrent with both.
	var maint sync.WaitGroup
	maint.Add(1)
	go func() {
		defer maint.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			if err := db.Maintain(); err != nil {
				t.Errorf("Maintain: %v", err)
				return
			}
			time.Sleep(time.Millisecond)
		}
	}()

	wg.Wait()
	close(stop)
	readers.Wait()
	maint.Wait()

	if got := accepted.Load() + rejected.Load(); got != writers*perW {
		t.Errorf("accounted for %d of %d attempted appends", got, writers*perW)
	}
	t.Logf("%d appends accepted, %d rejected by a concurrent flush",
		accepted.Load(), rejected.Load())

	got := query(t, db, model.MinTime, model.MaxTime,
		model.MustNewMatcher(model.MatchEqual, model.MetricName, "cpu"))
	if len(got) != writers {
		t.Fatalf("got %d series, want %d", len(got), writers)
	}

	// Every accepted sample must be readable, ordered and correct. Rejected
	// ones must be absent rather than partially present.
	total := 0
	for w := 0; w < writers; w++ {
		key := model.FromStrings(model.MetricName, "cpu", "worker", fmt.Sprint(w)).String()
		samples := got[key]
		total += len(samples)

		for i := 1; i < len(samples); i++ {
			if samples[i].T <= samples[i-1].T {
				t.Fatalf("worker %d is not strictly ordered at %d: %d after %d",
					w, i, samples[i].T, samples[i-1].T)
			}
		}
		for _, smpl := range samples {
			if smpl.V != float64(smpl.T/1000) {
				t.Fatalf("worker %d sample at %d has value %v, want %v",
					w, smpl.T, smpl.V, float64(smpl.T/1000))
			}
		}
	}
	if int64(total) != accepted.Load() {
		t.Errorf("query returned %d samples but %d were accepted", total, accepted.Load())
	}
}

// TestDBConcurrentAppendAndQueryWithoutMaintenance pins the exact counts that
// the test above deliberately relaxes: with no flush racing the writers,
// every append must succeed and every sample must be readable.
func TestDBConcurrentAppendAndQueryWithoutMaintenance(t *testing.T) {
	db := openTestDB(t, t.TempDir(), testOptions())

	const (
		writers = 4
		perW    = 200
	)
	var wg sync.WaitGroup
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			ls := model.FromStrings(model.MetricName, "cpu", "worker", fmt.Sprint(w))
			for i := 0; i < perW; i++ {
				app := db.Appender()
				if _, err := app.Append(0, ls, int64(i)*1000, float64(i)); err != nil {
					t.Errorf("writer %d: %v", w, err)
					app.Rollback()
					return
				}
				if err := app.Commit(); err != nil {
					t.Errorf("writer %d commit: %v", w, err)
					return
				}
			}
		}(w)
	}

	stop := make(chan struct{})
	var readers sync.WaitGroup
	for r := 0; r < 3; r++ {
		readers.Add(1)
		go func() {
			defer readers.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				q, err := db.Querier(model.MinTime, model.MaxTime)
				if err != nil {
					t.Errorf("Querier: %v", err)
					return
				}
				set := q.Select(model.MustNewMatcher(model.MatchEqual, model.MetricName, "cpu"))
				for set.Next() {
					it := set.At().Iterator(nil)
					for it.Next() {
					}
					if err := it.Err(); err != nil {
						t.Errorf("iterator: %v", err)
						q.Close()
						return
					}
				}
				if err := set.Err(); err != nil {
					t.Errorf("series set: %v", err)
				}
				q.Close()
			}
		}()
	}

	wg.Wait()
	close(stop)
	readers.Wait()

	got := query(t, db, model.MinTime, model.MaxTime,
		model.MustNewMatcher(model.MatchEqual, model.MetricName, "cpu"))
	if len(got) != writers {
		t.Fatalf("got %d series, want %d", len(got), writers)
	}
	for w := 0; w < writers; w++ {
		key := model.FromStrings(model.MetricName, "cpu", "worker", fmt.Sprint(w)).String()
		if len(got[key]) != perW {
			t.Errorf("worker %d has %d samples, want %d", w, len(got[key]), perW)
		}
	}
}

func TestDBWithoutWAL(t *testing.T) {
	dir := t.TempDir()

	opts := testOptions()
	opts.NoWAL = true

	db := openTestDB(t, dir, opts)
	appendBatch(t, db, 3, 1000, func(i int) float64 { return float64(i) })

	got := query(t, db, model.MinTime, model.MaxTime,
		model.MustNewMatcher(model.MatchEqual, model.MetricName, "cpu"))
	if len(got) != 3 {
		t.Errorf("got %d series, want 3", len(got))
	}
	if _, err := os.Stat(filepath.Join(dir, WALDirName)); !os.IsNotExist(err) {
		t.Error("a log directory was created with NoWAL set")
	}
}

func TestDBCloseIsIdempotentAndBlocksUse(t *testing.T) {
	db, err := Open(t.TempDir(), testOptions())
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := db.Close(); !errors.Is(err, ErrClosed) {
		t.Errorf("second Close = %v, want ErrClosed", err)
	}
	if _, err := db.Querier(model.MinTime, model.MaxTime); !errors.Is(err, ErrClosed) {
		t.Errorf("Querier after Close = %v, want ErrClosed", err)
	}
}

func TestDBCleansPartialBlocksOnOpen(t *testing.T) {
	dir := t.TempDir()

	// A leftover directory from a write that was interrupted.
	id, err := block.NewID()
	if err != nil {
		t.Fatal(err)
	}
	tmp := filepath.Join(dir, id.String()+block.TmpSuffix)
	if err := os.MkdirAll(tmp, 0o777); err != nil {
		t.Fatal(err)
	}

	db := openTestDB(t, dir, testOptions())
	if _, err := os.Stat(tmp); !os.IsNotExist(err) {
		t.Error("Open left a partial block directory in place")
	}
	if len(db.Blocks()) != 0 {
		t.Errorf("a partial block was opened: %d blocks", len(db.Blocks()))
	}
}

func TestDBStats(t *testing.T) {
	db := openTestDB(t, t.TempDir(), testOptions())
	for j := 0; j < 30; j++ {
		appendBatch(t, db, 4, int64(j)*1000, func(int) float64 { return 1 })
	}

	s := db.Stats()
	if s.Head.Series != 4 {
		t.Errorf("head series = %d, want 4", s.Head.Series)
	}
	if s.Head.Samples != 120 {
		t.Errorf("head samples = %d, want 120", s.Head.Samples)
	}

	if err := db.Maintain(); err != nil {
		t.Fatal(err)
	}
	s = db.Stats()
	if s.NumBlocks == 0 {
		t.Error("stats report no blocks after a flush")
	}
	if s.BlockBytes == 0 {
		t.Error("stats report zero block bytes after a flush")
	}
	if s.MinTime != 0 {
		t.Errorf("MinTime = %d, want 0 (blocks must be included)", s.MinTime)
	}
}

// TestDBDeduplicatesAcrossHeadAndBlocks covers the merge: after a flush the
// head still holds the boundary chunk, so the same samples exist in both
// tiers and must be returned once.
func TestDBDeduplicatesAcrossHeadAndBlocks(t *testing.T) {
	db := openTestDB(t, t.TempDir(), testOptions())

	const samples = 45
	for j := 0; j < samples; j++ {
		appendBatch(t, db, 2, int64(j)*1000, func(i int) float64 { return float64(i*1000 + j) })
	}
	if err := db.Maintain(); err != nil {
		t.Fatal(err)
	}

	got := query(t, db, model.MinTime, model.MaxTime,
		model.MustNewMatcher(model.MatchEqual, model.MetricName, "cpu"))
	for i := 0; i < 2; i++ {
		s := got[seriesLabels(i).String()]
		if len(s) != samples {
			t.Fatalf("series %d returned %d samples, want %d (duplicates across tiers?)",
				i, len(s), samples)
		}
		// Strictly increasing, with no repeats.
		for j := 1; j < len(s); j++ {
			if s[j].T <= s[j-1].T {
				t.Fatalf("series %d is not strictly ordered at %d: %d after %d",
					i, j, s[j].T, s[j-1].T)
			}
		}
		for j, smpl := range s {
			if smpl.V != float64(i*1000+j) {
				t.Fatalf("series %d sample %d = %v, want %v", i, j, smpl.V, float64(i*1000+j))
			}
		}
	}
}

func TestDBEmptyQuery(t *testing.T) {
	db := openTestDB(t, t.TempDir(), testOptions())

	q, err := db.Querier(model.MinTime, model.MaxTime)
	if err != nil {
		t.Fatal(err)
	}
	defer q.Close()

	set := q.Select(model.MustNewMatcher(model.MatchEqual, model.MetricName, "cpu"))
	if set.Next() {
		t.Error("an empty database returned a series")
	}
	if err := set.Err(); err != nil {
		t.Errorf("Err() = %v, want nil", err)
	}

	names, err := q.LabelNames()
	if err != nil {
		t.Fatal(err)
	}
	if len(names) != 0 {
		t.Errorf("LabelNames() = %v on an empty database", names)
	}
}

func BenchmarkDBAppend(b *testing.B) {
	dir := b.TempDir()
	opts := testOptions()
	opts.Logger = quietLogger()

	db, err := Open(dir, opts)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	const series = 500
	refs := make([]model.SeriesRef, series)
	app := db.Appender()
	for i := range refs {
		r, err := app.Append(0, seriesLabels(i), 0, 0)
		if err != nil {
			b.Fatal(err)
		}
		refs[i] = r
	}
	if err := app.Commit(); err != nil {
		b.Fatal(err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 1; i <= b.N; i++ {
		app := db.Appender()
		for j, ref := range refs {
			if _, err := app.Append(ref, nil, int64(i)*15_000, float64(j)); err != nil {
				b.Fatal(err)
			}
		}
		if err := app.Commit(); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	b.ReportMetric(float64(b.N*series)/b.Elapsed().Seconds(), "samples/sec")
}

func BenchmarkDBQuery(b *testing.B) {
	dir := b.TempDir()
	opts := testOptions()
	opts.BlockDuration = 2 * 60 * 60 * 1000
	opts.SamplesPerChunk = 120

	db, err := Open(dir, opts)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	const (
		series  = 1000
		samples = 240
	)
	app := db.Appender()
	for j := 0; j < samples; j++ {
		for i := 0; i < series; i++ {
			if _, err := app.Append(0, seriesLabels(i), int64(j)*15_000, float64(i+j)); err != nil {
				b.Fatal(err)
			}
		}
	}
	if err := app.Commit(); err != nil {
		b.Fatal(err)
	}

	m := model.MustNewMatcher(model.MatchEqual, "env", "prod")

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		q, err := db.Querier(model.MinTime, model.MaxTime)
		if err != nil {
			b.Fatal(err)
		}
		set := q.Select(m)
		n := 0
		for set.Next() {
			it := set.At().Iterator(nil)
			for it.Next() {
				n++
			}
		}
		if err := set.Err(); err != nil {
			b.Fatal(err)
		}
		q.Close()
	}
}

// sortedKeys is a helper for failure messages.
func sortedKeys(m map[string][]model.Sample) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

var _ = sortedKeys
