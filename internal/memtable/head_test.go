package memtable

import (
	"errors"
	"fmt"
	"math"
	"os"
	"sync"
	"testing"

	"github.com/navingamage/stratum/internal/chunk"
	"github.com/navingamage/stratum/internal/index"
	"github.com/navingamage/stratum/internal/model"
	"github.com/navingamage/stratum/internal/wal"
)

func testLabels(i int) model.Labels {
	return model.FromStrings(model.MetricName, "cpu", "host", fmt.Sprintf("web-%d", i))
}

// appendSamples commits one sample per call, which is the shape ingest uses.
func appendOne(t *testing.T, h *Head, ls model.Labels, ts int64, v float64) model.SeriesRef {
	t.Helper()
	app := h.Appender()
	ref, err := app.Append(0, ls, ts, v)
	if err != nil {
		t.Fatalf("Append: %v", err)
	}
	if err := app.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	return ref
}

// readSeries pulls every sample of a series out through its chunks.
func readSeries(t *testing.T, h *Head, ref model.SeriesRef, mint, maxt int64) []model.Sample {
	t.Helper()
	var out []model.Sample
	for _, c := range h.SeriesChunks(ref, mint, maxt) {
		it := c.Iterator(nil)
		for it.Next() {
			ts, v := it.At()
			if ts >= mint && ts <= maxt {
				out = append(out, model.Sample{T: ts, V: v})
			}
		}
		if err := it.Err(); err != nil {
			t.Fatalf("chunk iterator: %v", err)
		}
	}
	return out
}

func TestHeadAppendAndRead(t *testing.T) {
	h := NewHead(Options{})

	ls := testLabels(1)
	var ref model.SeriesRef
	for i := 0; i < 500; i++ {
		ref = appendOne(t, h, ls, int64(i)*15_000, float64(i))
	}

	if got := h.NumSeries(); got != 1 {
		t.Errorf("NumSeries() = %d, want 1", got)
	}
	if got := h.NumSamples(); got != 500 {
		t.Errorf("NumSamples() = %d, want 500", got)
	}
	if got, want := h.MinTime(), int64(0); got != want {
		t.Errorf("MinTime() = %d, want %d", got, want)
	}
	if got, want := h.MaxTime(), int64(499)*15_000; got != want {
		t.Errorf("MaxTime() = %d, want %d", got, want)
	}

	got := readSeries(t, h, ref, model.MinTime, model.MaxTime)
	if len(got) != 500 {
		t.Fatalf("read %d samples, want 500", len(got))
	}
	for i, s := range got {
		if s.T != int64(i)*15_000 || s.V != float64(i) {
			t.Fatalf("sample %d = (%d, %v), want (%d, %v)", i, s.T, s.V, int64(i)*15_000, float64(i))
		}
	}
}

func TestHeadAppendByRefSkipsLabels(t *testing.T) {
	h := NewHead(Options{})
	ref := appendOne(t, h, testLabels(1), 1000, 1)

	// The steady-state path: a known ref, and labels not supplied at all.
	app := h.Appender()
	got, err := app.Append(ref, nil, 2000, 2)
	if err != nil {
		t.Fatalf("Append by ref: %v", err)
	}
	if got != ref {
		t.Errorf("Append returned ref %d, want %d", got, ref)
	}
	if err := app.Commit(); err != nil {
		t.Fatal(err)
	}
	if n := len(readSeries(t, h, ref, model.MinTime, model.MaxTime)); n != 2 {
		t.Errorf("read %d samples, want 2", n)
	}
	if got := h.NumSeries(); got != 1 {
		t.Errorf("appending by ref created a second series: NumSeries() = %d", got)
	}
}

func TestHeadRejectsOutOfOrder(t *testing.T) {
	h := NewHead(Options{})
	ls := testLabels(1)
	appendOne(t, h, ls, 2000, 1)

	for _, ts := range []int64{2000, 1999, 0} {
		app := h.Appender()
		if _, err := app.Append(0, ls, ts, 2); !errors.Is(err, ErrOutOfOrderSample) {
			t.Errorf("Append(%d) = %v, want ErrOutOfOrderSample", ts, err)
		}
		app.Rollback()
	}
	if got := h.NumSamples(); got != 1 {
		t.Errorf("NumSamples() = %d, want 1", got)
	}
}

// TestHeadRejectsDuplicateWithinOneTransaction covers the case a series'
// committed maxTime cannot catch: two samples with the same timestamp
// buffered in one appender. Both would pass a naive check and the second
// would fail during Commit, after earlier samples were already applied.
func TestHeadRejectsDuplicateWithinOneTransaction(t *testing.T) {
	h := NewHead(Options{})
	ls := testLabels(1)

	app := h.Appender()
	if _, err := app.Append(0, ls, 1000, 1); err != nil {
		t.Fatal(err)
	}
	if _, err := app.Append(0, ls, 1000, 2); !errors.Is(err, ErrOutOfOrderSample) {
		t.Errorf("second Append at the same timestamp = %v, want ErrOutOfOrderSample", err)
	}
	if _, err := app.Append(0, ls, 999, 2); !errors.Is(err, ErrOutOfOrderSample) {
		t.Errorf("Append going backwards = %v, want ErrOutOfOrderSample", err)
	}
	// The transaction must still commit cleanly with the samples that passed.
	if err := app.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	if got := h.NumSamples(); got != 1 {
		t.Errorf("NumSamples() = %d, want 1", got)
	}
}

func TestHeadRollbackDiscards(t *testing.T) {
	h := NewHead(Options{})
	ls := testLabels(1)

	app := h.Appender()
	if _, err := app.Append(0, ls, 1000, 1); err != nil {
		t.Fatal(err)
	}
	if err := app.Rollback(); err != nil {
		t.Fatal(err)
	}
	if got := h.NumSamples(); got != 0 {
		t.Errorf("NumSamples() after Rollback = %d, want 0", got)
	}
	// Rolling back does not un-create the series; the label set is still
	// indexed, which matches what a retry would expect.
	if got := h.NumSeries(); got != 1 {
		t.Errorf("NumSeries() = %d, want 1", got)
	}
}

func TestHeadAppenderIsSingleUse(t *testing.T) {
	h := NewHead(Options{})
	app := h.Appender()
	if _, err := app.Append(0, testLabels(1), 1000, 1); err != nil {
		t.Fatal(err)
	}
	if err := app.Commit(); err != nil {
		t.Fatal(err)
	}

	if _, err := app.Append(0, testLabels(1), 2000, 1); !errors.Is(err, ErrAppenderClosed) {
		t.Errorf("Append after Commit = %v, want ErrAppenderClosed", err)
	}
	if err := app.Commit(); !errors.Is(err, ErrAppenderClosed) {
		t.Errorf("second Commit = %v, want ErrAppenderClosed", err)
	}
	if err := app.Rollback(); !errors.Is(err, ErrAppenderClosed) {
		t.Errorf("Rollback after Commit = %v, want ErrAppenderClosed", err)
	}
}

func TestHeadRejectsInvalidLabels(t *testing.T) {
	h := NewHead(Options{})
	app := h.Appender()

	bad := model.Labels{{Name: "not a name", Value: "x"}}
	if _, err := app.Append(0, bad, 1000, 1); !errors.Is(err, ErrInvalidSample) {
		t.Errorf("Append with an invalid label name = %v, want ErrInvalidSample", err)
	}
}

func TestHeadChunkCutting(t *testing.T) {
	t.Run("by sample count", func(t *testing.T) {
		h := NewHead(Options{SamplesPerChunk: 10, ChunkRange: math.MaxInt64})
		ls := testLabels(1)
		for i := 0; i < 35; i++ {
			appendOne(t, h, ls, int64(i)*1000, float64(i))
		}
		if got, want := h.Stats().Chunks, 4; got != want {
			t.Errorf("Chunks = %d, want %d (3 sealed + 1 open)", got, want)
		}
	})

	t.Run("by time range", func(t *testing.T) {
		// One sample every 10 minutes with a 1-hour chunk range: a new chunk
		// every 6 samples, regardless of the count limit.
		h := NewHead(Options{SamplesPerChunk: 1000, ChunkRange: 60 * 60 * 1000})
		ls := testLabels(1)
		for i := 0; i < 18; i++ {
			appendOne(t, h, ls, int64(i)*10*60*1000, float64(i))
		}
		if got := h.Stats().Chunks; got < 3 {
			t.Errorf("Chunks = %d, want at least 3", got)
		}
	})

	t.Run("all samples survive cutting", func(t *testing.T) {
		h := NewHead(Options{SamplesPerChunk: 7})
		ls := testLabels(1)
		var ref model.SeriesRef
		for i := 0; i < 100; i++ {
			ref = appendOne(t, h, ls, int64(i)*1000, float64(i))
		}
		got := readSeries(t, h, ref, model.MinTime, model.MaxTime)
		if len(got) != 100 {
			t.Fatalf("read %d samples across chunks, want 100", len(got))
		}
		for i, s := range got {
			if s.T != int64(i)*1000 || s.V != float64(i) {
				t.Fatalf("sample %d = (%d, %v)", i, s.T, s.V)
			}
		}
	})
}

func TestHeadChunkSelectionByTimeRange(t *testing.T) {
	h := NewHead(Options{SamplesPerChunk: 10})
	ls := testLabels(1)
	var ref model.SeriesRef
	for i := 0; i < 100; i++ {
		ref = appendOne(t, h, ls, int64(i)*1000, float64(i))
	}

	// A narrow window must not require decoding every chunk.
	chunks := h.SeriesChunks(ref, 50_000, 55_000)
	if len(chunks) == 0 {
		t.Fatal("no chunks returned for a range that has data")
	}
	if len(chunks) > 3 {
		t.Errorf("a 5-sample window selected %d chunks of 10 samples each", len(chunks))
	}

	got := readSeries(t, h, ref, 50_000, 55_000)
	if len(got) != 6 {
		t.Fatalf("read %d samples for [50000, 55000], want 6", len(got))
	}
	if got[0].T != 50_000 || got[len(got)-1].T != 55_000 {
		t.Errorf("range is [%d, %d], want [50000, 55000]", got[0].T, got[len(got)-1].T)
	}
}

func TestHeadIndexIntegration(t *testing.T) {
	h := NewHead(Options{})
	for i := 0; i < 10; i++ {
		ls := model.FromStrings(
			model.MetricName, "cpu",
			"host", fmt.Sprintf("web-%d", i),
			"env", map[bool]string{true: "prod", false: "staging"}[i%2 == 0],
		)
		appendOne(t, h, ls, 1000, float64(i))
	}

	p, err := index.PostingsForMatchers(h.Index(),
		model.MustNewMatcher(model.MatchEqual, "env", "prod"))
	if err != nil {
		t.Fatalf("PostingsForMatchers: %v", err)
	}
	refs, err := index.ExpandPostings(p)
	if err != nil {
		t.Fatal(err)
	}
	if len(refs) != 5 {
		t.Errorf("matched %d series, want 5", len(refs))
	}
	for _, ref := range refs {
		ls, ok := h.SeriesLabels(ref)
		if !ok {
			t.Fatalf("ref %d has no labels", ref)
		}
		if ls.Get("env") != "prod" {
			t.Errorf("ref %d has env=%q", ref, ls.Get("env"))
		}
	}
}

func TestHeadTruncate(t *testing.T) {
	h := NewHead(Options{SamplesPerChunk: 10})

	kept := testLabels(1)
	var keptRef model.SeriesRef
	for i := 0; i < 100; i++ {
		keptRef = appendOne(t, h, kept, int64(i)*1000, float64(i))
	}
	// A series entirely below the truncation point.
	old := testLabels(2)
	oldRef := appendOne(t, h, old, 1000, 1)

	if err := h.Truncate(50_000); err != nil {
		t.Fatalf("Truncate: %v", err)
	}

	// The wholly-old series is gone from the series map and the index.
	if _, ok := h.SeriesLabels(oldRef); ok {
		t.Error("a series with no remaining data survived truncation")
	}
	all, err := index.ExpandPostings(h.Index().All())
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range all {
		if r == oldRef {
			t.Error("the truncated series is still in the index")
		}
	}

	// The surviving series kept its recent data.
	got := readSeries(t, h, keptRef, model.MinTime, model.MaxTime)
	if len(got) == 0 {
		t.Fatal("the surviving series lost all data")
	}
	if got[len(got)-1].T != 99_000 {
		t.Errorf("newest sample is %d, want 99000", got[len(got)-1].T)
	}

	// New samples below the floor are refused.
	app := h.Appender()
	if _, err := app.Append(0, testLabels(3), 1000, 1); !errors.Is(err, ErrOutOfBounds) {
		t.Errorf("Append below the truncation floor = %v, want ErrOutOfBounds", err)
	}
	app.Rollback()
}

func TestHeadTruncateIsMonotonic(t *testing.T) {
	h := NewHead(Options{})
	appendOne(t, h, testLabels(1), 10_000, 1)

	if err := h.Truncate(5_000); err != nil {
		t.Fatal(err)
	}
	// Going backwards must be a no-op, not a re-opening of the floor.
	if err := h.Truncate(1_000); err != nil {
		t.Fatal(err)
	}
	app := h.Appender()
	if _, err := app.Append(0, testLabels(2), 2_000, 1); !errors.Is(err, ErrOutOfBounds) {
		t.Errorf("Append below the earlier floor = %v, want ErrOutOfBounds", err)
	}
	app.Rollback()
}

func TestHeadConcurrentAppends(t *testing.T) {
	h := NewHead(Options{SamplesPerChunk: 20})

	const (
		writers = 8
		perW    = 300
	)
	var wg sync.WaitGroup
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			ls := testLabels(w)
			for i := 0; i < perW; i++ {
				app := h.Appender()
				if _, err := app.Append(0, ls, int64(i)*1000, float64(i)); err != nil {
					t.Errorf("writer %d append %d: %v", w, i, err)
					app.Rollback()
					return
				}
				if err := app.Commit(); err != nil {
					t.Errorf("writer %d commit %d: %v", w, i, err)
					return
				}
			}
		}(w)
	}

	// Readers run concurrently: queries must never block ingest or crash.
	var readers sync.WaitGroup
	stop := make(chan struct{})
	for r := 0; r < 4; r++ {
		readers.Add(1)
		go func() {
			defer readers.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				_ = h.Stats()
				p, err := index.PostingsForMatchers(h.Index(),
					model.MustNewMatcher(model.MatchEqual, model.MetricName, "cpu"))
				if err != nil {
					t.Errorf("query: %v", err)
					return
				}
				refs, err := index.ExpandPostings(p)
				if err != nil {
					t.Errorf("expand: %v", err)
					return
				}
				for _, ref := range refs {
					for _, c := range h.SeriesChunks(ref, model.MinTime, model.MaxTime) {
						it := c.Iterator(nil)
						for it.Next() {
						}
						if err := it.Err(); err != nil {
							t.Errorf("iterating a chunk during ingest: %v", err)
							return
						}
					}
				}
			}
		}()
	}

	wg.Wait()
	close(stop)
	readers.Wait()

	if got := h.NumSeries(); got != writers {
		t.Errorf("NumSeries() = %d, want %d", got, writers)
	}
	if got := h.NumSamples(); got != writers*perW {
		t.Errorf("NumSamples() = %d, want %d", got, writers*perW)
	}
	for w := 0; w < writers; w++ {
		ref := model.SeriesRef(0)
		all, _ := index.ExpandPostings(h.Index().All())
		for _, r := range all {
			if ls, _ := h.SeriesLabels(r); ls.Equal(testLabels(w)) {
				ref = r
			}
		}
		if n := len(readSeries(t, h, ref, model.MinTime, model.MaxTime)); n != perW {
			t.Errorf("writer %d series has %d samples, want %d", w, n, perW)
		}
	}
}

// TestHeadWALRecovery is the durability property end to end: everything a
// committed append reported as accepted must come back after a restart.
func TestHeadWALRecovery(t *testing.T) {
	dir := t.TempDir()

	w, err := wal.Open(dir, wal.Options{Sync: wal.SyncNever, SegmentSize: 16 * wal.PageSize})
	if err != nil {
		t.Fatal(err)
	}
	h := NewHead(Options{WAL: w, SamplesPerChunk: 13})

	type expected struct {
		labels  model.Labels
		samples []model.Sample
	}
	want := make([]expected, 0, 20)

	for s := 0; s < 20; s++ {
		ls := testLabels(s)
		e := expected{labels: ls}
		app := h.Appender()
		for i := 0; i < 100; i++ {
			ts, v := int64(i)*15_000, float64(s)+float64(i)/10
			if _, err := app.Append(0, ls, ts, v); err != nil {
				t.Fatalf("append: %v", err)
			}
			e.samples = append(e.samples, model.Sample{T: ts, V: v})
		}
		if err := app.Commit(); err != nil {
			t.Fatalf("commit: %v", err)
		}
		want = append(want, e)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	// Restart: a fresh head replaying the same log.
	h2 := NewHead(Options{SamplesPerChunk: 13})
	stats, err := h2.Replay(dir)
	if err != nil {
		t.Fatalf("Replay: %v", err)
	}
	t.Logf("replayed %d series and %d samples in %v (%d dropped)",
		stats.Series, stats.Samples, stats.Elapsed, stats.Dropped)

	if stats.Dropped != 0 {
		t.Errorf("replay dropped %d samples", stats.Dropped)
	}
	if h2.NumSeries() != len(want) {
		t.Fatalf("recovered %d series, want %d", h2.NumSeries(), len(want))
	}
	if h2.NumSamples() != h.NumSamples() {
		t.Errorf("recovered %d samples, want %d", h2.NumSamples(), h.NumSamples())
	}
	if h2.MinTime() != h.MinTime() || h2.MaxTime() != h.MaxTime() {
		t.Errorf("recovered bounds [%d, %d], want [%d, %d]",
			h2.MinTime(), h2.MaxTime(), h.MinTime(), h.MaxTime())
	}

	// Every series must be reachable by its labels and hold the same samples.
	all, err := index.ExpandPostings(h2.Index().All())
	if err != nil {
		t.Fatal(err)
	}
	byLabels := make(map[string]model.SeriesRef, len(all))
	for _, ref := range all {
		ls, _ := h2.SeriesLabels(ref)
		byLabels[ls.String()] = ref
	}
	for _, e := range want {
		ref, ok := byLabels[e.labels.String()]
		if !ok {
			t.Fatalf("series %s did not survive replay", e.labels)
		}
		got := readSeries(t, h2, ref, model.MinTime, model.MaxTime)
		if len(got) != len(e.samples) {
			t.Fatalf("series %s recovered %d samples, want %d", e.labels, len(got), len(e.samples))
		}
		for i := range got {
			if got[i] != e.samples[i] {
				t.Fatalf("series %s sample %d = %v, want %v", e.labels, i, got[i], e.samples[i])
			}
		}
	}
}

// TestHeadWALRecoveryAfterTornWrite simulates a crash mid-write by truncating
// the log at many offsets. Recovery must always succeed and always produce a
// prefix of the ingested data.
func TestHeadWALRecoveryAfterTornWrite(t *testing.T) {
	dir := t.TempDir()
	w, err := wal.Open(dir, wal.Options{Sync: wal.SyncNever, SegmentSize: 1024 * wal.PageSize})
	if err != nil {
		t.Fatal(err)
	}
	h := NewHead(Options{WAL: w})

	ls := testLabels(1)
	const n = 400
	app := h.Appender()
	for i := 0; i < n; i++ {
		if _, err := app.Append(0, ls, int64(i)*1000, float64(i)); err != nil {
			t.Fatal(err)
		}
	}
	if err := app.Commit(); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	full, err := os.ReadFile(wal.SegmentName(dir, 0))
	if err != nil {
		t.Fatal(err)
	}

	for _, frac := range []float64{0, 0.1, 0.25, 0.5, 0.75, 0.9, 0.99, 1.0} {
		cut := int(float64(len(full)) * frac)
		t.Run(fmt.Sprintf("%.0f%%", frac*100), func(t *testing.T) {
			d := t.TempDir()
			if err := os.WriteFile(wal.SegmentName(d, 0), full[:cut], 0o666); err != nil {
				t.Fatal(err)
			}

			h2 := NewHead(Options{})
			if _, err := h2.Replay(d); err != nil {
				t.Fatalf("replay of a log truncated at %d bytes: %v", cut, err)
			}

			if h2.NumSeries() == 0 {
				return // nothing recovered, which is valid for an early cut
			}
			all, _ := index.ExpandPostings(h2.Index().All())
			got := readSeries(t, h2, all[0], model.MinTime, model.MaxTime)
			if len(got) > n {
				t.Fatalf("recovered %d samples, more than the %d written", len(got), n)
			}
			// Whatever survived must be an exact prefix.
			for i := range got {
				if got[i].T != int64(i)*1000 || got[i].V != float64(i) {
					t.Fatalf("sample %d = (%d, %v), want (%d, %v)",
						i, got[i].T, got[i].V, int64(i)*1000, float64(i))
				}
			}
		})
	}
}

func TestHeadReplayIsIdempotent(t *testing.T) {
	dir := t.TempDir()
	w, err := wal.Open(dir, wal.Options{Sync: wal.SyncNever})
	if err != nil {
		t.Fatal(err)
	}
	h := NewHead(Options{WAL: w})
	for i := 0; i < 50; i++ {
		appendOne(t, h, testLabels(1), int64(i)*1000, float64(i))
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	// Replaying the same log into the same head twice must not double the
	// samples: the second pass finds every timestamp already present and
	// drops it as out of order.
	h2 := NewHead(Options{})
	if _, err := h2.Replay(dir); err != nil {
		t.Fatal(err)
	}
	first := h2.NumSamples()

	stats, err := h2.Replay(dir)
	if err != nil {
		t.Fatal(err)
	}
	if h2.NumSamples() != first {
		t.Errorf("a second replay changed the sample count from %d to %d", first, h2.NumSamples())
	}
	if stats.Dropped != 50 {
		t.Errorf("second replay dropped %d samples, want 50", stats.Dropped)
	}
}

func TestHeadReplayOfEmptyDir(t *testing.T) {
	h := NewHead(Options{})
	stats, err := h.Replay(t.TempDir())
	if err != nil {
		t.Fatalf("Replay of an empty directory: %v", err)
	}
	if stats.Series != 0 || stats.Samples != 0 {
		t.Errorf("recovered %+v from an empty directory", stats)
	}
}

func TestHeadStats(t *testing.T) {
	h := NewHead(Options{SamplesPerChunk: 10})
	for s := 0; s < 5; s++ {
		for i := 0; i < 25; i++ {
			appendOne(t, h, testLabels(s), int64(i)*1000, float64(i))
		}
	}

	st := h.Stats()
	if st.Series != 5 {
		t.Errorf("Series = %d, want 5", st.Series)
	}
	if st.Samples != 125 {
		t.Errorf("Samples = %d, want 125", st.Samples)
	}
	// 25 samples at 10 per chunk: 2 sealed + 1 open, per series.
	if st.Chunks != 15 {
		t.Errorf("Chunks = %d, want 15", st.Chunks)
	}
	if st.IndexStats.LabelNames != 2 {
		t.Errorf("index LabelNames = %d, want 2", st.IndexStats.LabelNames)
	}
}

func TestHeadWithoutWALStillWorks(t *testing.T) {
	// A nil WAL is the configuration compaction uses to build a head it never
	// intends to persist.
	h := NewHead(Options{WAL: nil})
	ref := appendOne(t, h, testLabels(1), 1000, 42)
	if got := readSeries(t, h, ref, model.MinTime, model.MaxTime); len(got) != 1 {
		t.Errorf("read %d samples, want 1", len(got))
	}
}

func TestSeriesChunksForUnknownRef(t *testing.T) {
	h := NewHead(Options{})
	if got := h.SeriesChunks(999, model.MinTime, model.MaxTime); got != nil {
		t.Errorf("SeriesChunks for an unknown ref = %v, want nil", got)
	}
	if _, ok := h.SeriesLabels(999); ok {
		t.Error("SeriesLabels reported an unknown ref as present")
	}
}

func BenchmarkHeadAppendNewSeries(b *testing.B) {
	h := NewHead(Options{})
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		app := h.Appender()
		if _, err := app.Append(0, testLabels(i), 1000, 1); err != nil {
			b.Fatal(err)
		}
		if err := app.Commit(); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkHeadAppendExistingSeries(b *testing.B) {
	h := NewHead(Options{})
	ls := testLabels(1)
	ref, err := func() (model.SeriesRef, error) {
		app := h.Appender()
		r, err := app.Append(0, ls, 0, 0)
		if err != nil {
			return 0, err
		}
		return r, app.Commit()
	}()
	if err != nil {
		b.Fatal(err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 1; i <= b.N; i++ {
		app := h.Appender()
		if _, err := app.Append(ref, nil, int64(i)*1000, float64(i)); err != nil {
			b.Fatal(err)
		}
		if err := app.Commit(); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkHeadAppendBatched is the realistic ingest shape: a scrape's worth
// of series committed as one transaction.
func BenchmarkHeadAppendBatched(b *testing.B) {
	const seriesPerScrape = 500

	h := NewHead(Options{})
	refs := make([]model.SeriesRef, seriesPerScrape)
	app := h.Appender()
	for i := range refs {
		r, err := app.Append(0, testLabels(i), 0, 0)
		if err != nil {
			b.Fatal(err)
		}
		refs[i] = r
	}
	if err := app.Commit(); err != nil {
		b.Fatal(err)
	}

	b.ReportAllocs()
	b.SetBytes(seriesPerScrape * 16)
	b.ResetTimer()
	for i := 1; i <= b.N; i++ {
		app := h.Appender()
		for j, ref := range refs {
			if _, err := app.Append(ref, nil, int64(i)*15_000, float64(j)); err != nil {
				b.Fatal(err)
			}
		}
		if err := app.Commit(); err != nil {
			b.Fatal(err)
		}
	}
}

var _ chunk.Chunk = (*chunk.XORChunk)(nil)
