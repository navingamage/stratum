package compact

import (
	"fmt"
	"math"
	"path/filepath"
	"sort"
	"testing"
	"time"

	"github.com/navingamage/stratum/internal/block"
	"github.com/navingamage/stratum/internal/chunk"
	"github.com/navingamage/stratum/internal/index"
	"github.com/navingamage/stratum/internal/memtable"
	"github.com/navingamage/stratum/internal/model"
)

const hour = int64(60 * 60 * 1000)

// buildHead fills a head with the given series data.
func buildHead(t *testing.T, samplesPerChunk int, series map[string][]model.Sample) *memtable.Head {
	t.Helper()
	h := memtable.NewHead(memtable.Options{SamplesPerChunk: samplesPerChunk})

	app := h.Appender()
	// Sorted so appends are deterministic across runs.
	names := make([]string, 0, len(series))
	for n := range series {
		names = append(names, n)
	}
	sort.Strings(names)

	for _, n := range names {
		ls := model.FromStrings(model.MetricName, "m", "host", n)
		for _, s := range series[n] {
			if _, err := app.Append(0, ls, s.T, s.V); err != nil {
				t.Fatalf("append %s at %d: %v", n, s.T, err)
			}
		}
	}
	if err := app.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}
	return h
}

// readBlock returns every series in a block as label string to samples.
func readBlock(t *testing.T, dir string) map[string][]model.Sample {
	t.Helper()
	b, err := block.Open(dir)
	if err != nil {
		t.Fatalf("Open %s: %v", dir, err)
	}
	defer b.Close()

	out := make(map[string][]model.Sample)
	for i := 0; i < b.Index().NumSeries(); i++ {
		ls, chunks, err := b.SeriesChunks(model.SeriesRef(i), model.MinTime, model.MaxTime)
		if err != nil {
			t.Fatalf("SeriesChunks(%d): %v", i, err)
		}
		var samples []model.Sample
		for _, c := range chunks {
			it := c.Iterator(nil)
			for it.Next() {
				ts, v := it.At()
				samples = append(samples, model.Sample{T: ts, V: v})
			}
			if err := it.Err(); err != nil {
				t.Fatalf("iterating: %v", err)
			}
		}
		out[ls.String()] = samples
	}
	return out
}

func TestFlushHeadToBlock(t *testing.T) {
	dir := t.TempDir()

	want := map[string][]model.Sample{
		"a": {{T: 0, V: 1}, {T: 1000, V: 2}, {T: 2000, V: 3}},
		"b": {{T: 500, V: 10}, {T: 1500, V: 20}},
	}
	h := buildHead(t, 2, want)

	c := New(dir, Options{SamplesPerChunk: 2})
	meta, err := c.Flush(h, model.MinTime, model.MaxTime)
	if err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if meta == nil {
		t.Fatal("Flush produced no block")
	}
	if meta.Stats.NumSeries != 2 {
		t.Errorf("NumSeries = %d, want 2", meta.Stats.NumSeries)
	}
	if meta.Stats.NumSamples != 5 {
		t.Errorf("NumSamples = %d, want 5", meta.Stats.NumSamples)
	}
	if meta.MinTime != 0 || meta.MaxTime != 2000 {
		t.Errorf("bounds [%d, %d], want [0, 2000]", meta.MinTime, meta.MaxTime)
	}
	if meta.Compaction.Level != 1 {
		t.Errorf("level = %d, want 1", meta.Compaction.Level)
	}

	got := readBlock(t, filepath.Join(dir, meta.ID.String()))
	for name, wantSamples := range want {
		key := model.FromStrings(model.MetricName, "m", "host", name).String()
		gotSamples := got[key]
		if len(gotSamples) != len(wantSamples) {
			t.Fatalf("series %s has %d samples, want %d", name, len(gotSamples), len(wantSamples))
		}
		for i := range gotSamples {
			if gotSamples[i] != wantSamples[i] {
				t.Errorf("series %s sample %d = %v, want %v", name, i, gotSamples[i], wantSamples[i])
			}
		}
	}
}

func TestFlushRespectsTimeBounds(t *testing.T) {
	dir := t.TempDir()
	h := buildHead(t, 2, map[string][]model.Sample{
		"a": {{T: 0, V: 1}, {T: 1000, V: 2}, {T: 2000, V: 3}, {T: 3000, V: 4}},
	})

	c := New(dir, Options{SamplesPerChunk: 2})
	meta, err := c.Flush(h, 1000, 2000)
	if err != nil {
		t.Fatalf("Flush: %v", err)
	}
	// Chunks are selected whole, so a chunk straddling the boundary brings
	// its neighbours along. The block must at least not exceed the head.
	if meta.MinTime < 0 || meta.MaxTime > 3000 {
		t.Errorf("bounds [%d, %d] fall outside the head", meta.MinTime, meta.MaxTime)
	}
	if meta.Stats.NumSamples == 0 {
		t.Error("a bounded flush produced no samples")
	}
}

func TestFlushEmptyHeadProducesNoBlock(t *testing.T) {
	dir := t.TempDir()
	h := memtable.NewHead(memtable.Options{})

	c := New(dir, Options{})
	meta, err := c.Flush(h, model.MinTime, model.MaxTime)
	if err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if meta != nil {
		t.Errorf("flushing an empty head produced block %v", meta)
	}
	dirs, _ := block.List(dir)
	if len(dirs) != 0 {
		t.Errorf("flushing an empty head left %d blocks", len(dirs))
	}
}

// writeBlockFrom flushes a head's worth of data into dir and returns the meta.
func writeBlockFrom(t *testing.T, dir string, series map[string][]model.Sample) *block.Meta {
	t.Helper()
	h := buildHead(t, 4, series)
	c := New(dir, Options{SamplesPerChunk: 4})
	meta, err := c.Flush(h, model.MinTime, model.MaxTime)
	if err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if meta == nil {
		t.Fatal("Flush produced no block")
	}
	return meta
}

func TestCompactDisjointBlocks(t *testing.T) {
	dir := t.TempDir()

	m1 := writeBlockFrom(t, dir, map[string][]model.Sample{
		"a": {{T: 0, V: 1}, {T: 1000, V: 2}},
		"b": {{T: 0, V: 10}},
	})
	m2 := writeBlockFrom(t, dir, map[string][]model.Sample{
		"a": {{T: 2000, V: 3}, {T: 3000, V: 4}},
		"c": {{T: 2000, V: 30}},
	})

	c := New(dir, Options{SamplesPerChunk: 4})
	meta, err := c.Compact(&Plan{Metas: []*block.Meta{m1, m2}})
	if err != nil {
		t.Fatalf("Compact: %v", err)
	}
	if meta == nil {
		t.Fatal("Compact produced no block")
	}

	// The inputs must be gone and only the output left.
	dirs, err := block.List(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(dirs) != 1 {
		t.Fatalf("%d blocks remain after compaction, want 1", len(dirs))
	}

	if meta.Compaction.Level != 2 {
		t.Errorf("level = %d, want 2", meta.Compaction.Level)
	}
	if len(meta.Compaction.Parents) != 2 {
		t.Errorf("parents = %v, want 2 entries", meta.Compaction.Parents)
	}
	if len(meta.Compaction.Sources) != 2 {
		t.Errorf("sources = %v, want 2 entries", meta.Compaction.Sources)
	}
	if meta.Stats.NumSeries != 3 {
		t.Errorf("NumSeries = %d, want 3", meta.Stats.NumSeries)
	}
	if meta.Stats.NumSamples != 6 {
		t.Errorf("NumSamples = %d, want 6", meta.Stats.NumSamples)
	}

	got := readBlock(t, dirs[0])
	key := func(n string) string {
		return model.FromStrings(model.MetricName, "m", "host", n).String()
	}
	wantA := []model.Sample{{T: 0, V: 1}, {T: 1000, V: 2}, {T: 2000, V: 3}, {T: 3000, V: 4}}
	if len(got[key("a")]) != len(wantA) {
		t.Fatalf("series a has %d samples, want %d", len(got[key("a")]), len(wantA))
	}
	for i, s := range got[key("a")] {
		if s != wantA[i] {
			t.Errorf("series a sample %d = %v, want %v", i, s, wantA[i])
		}
	}
	if len(got[key("b")]) != 1 || len(got[key("c")]) != 1 {
		t.Errorf("series b/c did not survive: %d and %d samples",
			len(got[key("b")]), len(got[key("c")]))
	}
}

// TestCompactOverlappingBlocksDeduplicates is the case that forces samples to
// be merged and re-encoded rather than chunk lists concatenated.
func TestCompactOverlappingBlocksDeduplicates(t *testing.T) {
	dir := t.TempDir()

	m1 := writeBlockFrom(t, dir, map[string][]model.Sample{
		"a": {{T: 0, V: 1}, {T: 1000, V: 2}, {T: 2000, V: 3}},
	})
	// Overlaps m1 and disagrees at t=2000. The later block wins.
	m2 := writeBlockFrom(t, dir, map[string][]model.Sample{
		"a": {{T: 1500, V: 99}, {T: 2000, V: 42}, {T: 2500, V: 5}},
	})

	c := New(dir, Options{SamplesPerChunk: 4})
	meta, err := c.Compact(&Plan{Metas: []*block.Meta{m1, m2}})
	if err != nil {
		t.Fatalf("Compact: %v", err)
	}

	dirs, _ := block.List(dir)
	got := readBlock(t, dirs[0])
	key := model.FromStrings(model.MetricName, "m", "host", "a").String()

	want := []model.Sample{
		{T: 0, V: 1}, {T: 1000, V: 2}, {T: 1500, V: 99}, {T: 2000, V: 42}, {T: 2500, V: 5},
	}
	if len(got[key]) != len(want) {
		t.Fatalf("got %d samples, want %d: %v", len(got[key]), len(want), got[key])
	}
	for i := range want {
		if got[key][i] != want[i] {
			t.Errorf("sample %d = %v, want %v", i, got[key][i], want[i])
		}
	}
	if meta.Stats.NumSamples != 5 {
		t.Errorf("NumSamples = %d, want 5 (duplicates must be merged away)", meta.Stats.NumSamples)
	}

	// Timestamps must be strictly increasing in the output, or the chunk
	// encoding would have rejected them.
	for i := 1; i < len(got[key]); i++ {
		if got[key][i].T <= got[key][i-1].T {
			t.Fatalf("output is not strictly ordered at index %d", i)
		}
	}
}

func TestCompactManyBlocks(t *testing.T) {
	dir := t.TempDir()

	var metas []*block.Meta
	for b := 0; b < 5; b++ {
		series := make(map[string][]model.Sample)
		for s := 0; s < 10; s++ {
			name := fmt.Sprintf("host-%02d", s)
			var samples []model.Sample
			for i := 0; i < 20; i++ {
				t := int64(b*20+i) * 1000
				samples = append(samples, model.Sample{T: t, V: float64(s*10000) + float64(t)})
			}
			series[name] = samples
		}
		metas = append(metas, writeBlockFrom(t, dir, series))
	}

	c := New(dir, Options{SamplesPerChunk: 8})
	meta, err := c.Compact(&Plan{Metas: metas})
	if err != nil {
		t.Fatalf("Compact: %v", err)
	}
	if meta.Stats.NumSeries != 10 {
		t.Errorf("NumSeries = %d, want 10", meta.Stats.NumSeries)
	}
	if meta.Stats.NumSamples != 10*100 {
		t.Errorf("NumSamples = %d, want %d", meta.Stats.NumSamples, 10*100)
	}
	if meta.Compaction.Level != 2 {
		t.Errorf("level = %d, want 2", meta.Compaction.Level)
	}
	if len(meta.Compaction.Sources) != 5 {
		t.Errorf("sources = %d, want 5", len(meta.Compaction.Sources))
	}

	dirs, _ := block.List(dir)
	if len(dirs) != 1 {
		t.Fatalf("%d blocks remain, want 1", len(dirs))
	}
	got := readBlock(t, dirs[0])
	for s := 0; s < 10; s++ {
		key := model.FromStrings(model.MetricName, "m", "host", fmt.Sprintf("host-%02d", s)).String()
		samples := got[key]
		if len(samples) != 100 {
			t.Fatalf("series %d has %d samples, want 100", s, len(samples))
		}
		for i, smpl := range samples {
			wantT := int64(i) * 1000
			if smpl.T != wantT || smpl.V != float64(s*10000)+float64(wantT) {
				t.Fatalf("series %d sample %d = %v, want t=%d", s, i, smpl, wantT)
			}
		}
	}
}

// TestCompactedBlockIsQueryable checks the output is a fully functional block
// and not merely well-formed bytes.
func TestCompactedBlockIsQueryable(t *testing.T) {
	dir := t.TempDir()

	m1 := writeBlockFrom(t, dir, map[string][]model.Sample{
		"a": {{T: 0, V: 1}}, "b": {{T: 0, V: 2}},
	})
	m2 := writeBlockFrom(t, dir, map[string][]model.Sample{
		"b": {{T: 1000, V: 3}}, "c": {{T: 1000, V: 4}},
	})

	c := New(dir, Options{})
	if _, err := c.Compact(&Plan{Metas: []*block.Meta{m1, m2}}); err != nil {
		t.Fatal(err)
	}

	dirs, _ := block.List(dir)
	b, err := block.Open(dirs[0])
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()

	p, err := index.PostingsForMatchers(b.Index(),
		model.MustNewMatcher(model.MatchEqual, "host", "b"))
	if err != nil {
		t.Fatal(err)
	}
	refs, err := index.ExpandPostings(p)
	if err != nil {
		t.Fatal(err)
	}
	if len(refs) != 1 {
		t.Fatalf("host=b matched %d series, want 1", len(refs))
	}
	ls, chunks, err := b.SeriesChunks(refs[0], model.MinTime, model.MaxTime)
	if err != nil {
		t.Fatal(err)
	}
	if ls.Get("host") != "b" {
		t.Errorf("matched series has host=%q", ls.Get("host"))
	}
	n := 0
	for _, ch := range chunks {
		it := ch.Iterator(nil)
		for it.Next() {
			n++
		}
	}
	if n != 2 {
		t.Errorf("series b has %d samples after compaction, want 2", n)
	}
}

func TestCompactPreservesSourceLineage(t *testing.T) {
	dir := t.TempDir()

	m1 := writeBlockFrom(t, dir, map[string][]model.Sample{"a": {{T: 0, V: 1}}})
	m2 := writeBlockFrom(t, dir, map[string][]model.Sample{"a": {{T: 1000, V: 2}}})

	c := New(dir, Options{})
	level2, err := c.Compact(&Plan{Metas: []*block.Meta{m1, m2}})
	if err != nil {
		t.Fatal(err)
	}

	m3 := writeBlockFrom(t, dir, map[string][]model.Sample{"a": {{T: 2000, V: 3}}})
	level3, err := c.Compact(&Plan{Metas: []*block.Meta{level2, m3}})
	if err != nil {
		t.Fatal(err)
	}

	if level3.Compaction.Level != 3 {
		t.Errorf("level = %d, want 3", level3.Compaction.Level)
	}
	// Sources must be the level-1 ancestors, not the intermediate block.
	if len(level3.Compaction.Sources) != 3 {
		t.Fatalf("sources = %v, want 3 level-1 ancestors", level3.Compaction.Sources)
	}
	want := map[string]bool{m1.ID.String(): true, m2.ID.String(): true, m3.ID.String(): true}
	for _, s := range level3.Compaction.Sources {
		if !want[s.String()] {
			t.Errorf("source %s is not a level-1 ancestor", s)
		}
	}
	// And sorted, so the metadata is stable.
	for i := 1; i < len(level3.Compaction.Sources); i++ {
		if level3.Compaction.Sources[i].Compare(level3.Compaction.Sources[i-1]) <= 0 {
			t.Error("sources are not sorted")
		}
	}
}

func TestPlan(t *testing.T) {
	meta := func(mint, maxt int64, level int) *block.Meta {
		id, _ := block.NewID()
		return &block.Meta{ID: id, MinTime: mint, MaxTime: maxt,
			Compaction: block.Compaction{Level: level}}
	}

	c := New(t.TempDir(), Options{Ranges: []int64{2 * hour, 6 * hour, 18 * hour}})

	t.Run("nothing to do with one block", func(t *testing.T) {
		if p := c.Plan([]*block.Meta{meta(0, 2*hour, 1)}); p != nil {
			t.Errorf("Plan returned %v for a single block", p)
		}
	})

	t.Run("nothing to do for a partial window", func(t *testing.T) {
		// Two tiny blocks that together cover far less than the 2h range.
		blocks := []*block.Meta{
			meta(0, 60*1000, 1),
			meta(120*1000, 180*1000, 1),
		}
		if p := c.Plan(blocks); p != nil {
			t.Errorf("Plan compacted an incomplete window: %v", p.Metas)
		}
	})

	t.Run("groups a full window", func(t *testing.T) {
		// Ranges are inclusive of both ends, so consecutive blocks must not
		// share a boundary timestamp - two blocks holding a sample at the
		// same instant really do overlap, and the planner says so.
		blocks := []*block.Meta{
			meta(0, 40*60*1000-1, 1),
			meta(40*60*1000, 80*60*1000-1, 1),
			meta(80*60*1000, 2*hour-1, 1),
		}
		p := c.Plan(blocks)
		if p == nil {
			t.Fatal("Plan returned nothing for a full 2h window")
		}
		if len(p.Metas) != 3 {
			t.Errorf("plan has %d blocks, want 3", len(p.Metas))
		}
		if p.Range != 2*hour {
			t.Errorf("range = %d, want %d", p.Range, 2*hour)
		}
	})

	t.Run("does not group across window boundaries", func(t *testing.T) {
		// One block in each of two adjacent 2h windows. Neither window holds
		// two blocks, so the 2h level has nothing to do.
		blocks := []*block.Meta{
			meta(0, 90*60*1000, 1),
			meta(2*hour, 2*hour+90*60*1000, 1),
		}
		if p := planRange(blocks, 2*hour); p != nil {
			t.Errorf("the 2h level grouped across a window boundary: %v", p.Metas)
		}

		// The same two blocks do belong together one level up, which is the
		// whole point of the ladder: they share a 6h window and between them
		// cover enough of it to be worth merging.
		p := planRange(blocks, 6*hour)
		if p == nil {
			t.Fatal("the 6h level did not group two blocks sharing a 6h window")
		}
		if len(p.Metas) != 2 {
			t.Errorf("plan has %d blocks, want 2", len(p.Metas))
		}

		// Blocks far enough apart to share no window are left alone entirely.
		far := []*block.Meta{
			meta(0, 90*60*1000, 1),
			meta(30*hour, 30*hour+90*60*1000, 1),
		}
		if p := c.Plan(far); p != nil {
			t.Errorf("Plan grouped blocks 30h apart: %v", p.Metas)
		}
	})

	t.Run("overlapping blocks take priority", func(t *testing.T) {
		blocks := []*block.Meta{
			meta(0, 3*hour, 2),
			meta(2*hour, 5*hour, 2), // overlaps the first
			meta(10*hour, 11*hour, 1),
		}
		p := c.Plan(blocks)
		if p == nil {
			t.Fatal("Plan ignored overlapping blocks")
		}
		if len(p.Metas) != 2 {
			t.Fatalf("plan has %d blocks, want the 2 that overlap", len(p.Metas))
		}
		if p.Metas[0].MinTime != 0 || p.Metas[1].MinTime != 2*hour {
			t.Errorf("plan selected the wrong blocks: %d and %d",
				p.Metas[0].MinTime, p.Metas[1].MinTime)
		}
	})

	t.Run("skips blocks already at the target size", func(t *testing.T) {
		blocks := []*block.Meta{
			meta(0, 2*hour-1, 2),      // already a full 2h block
			meta(2*hour, 4*hour-1, 2), // and another, in the next window
		}
		// Neither is a candidate at 2h; at 6h they are in the same window and
		// together cover 4h, which is over the half-range threshold.
		p := c.Plan(blocks)
		if p == nil {
			t.Fatal("Plan returned nothing for two 2h blocks in one 6h window")
		}
		if p.Range != 6*hour {
			t.Errorf("range = %d, want %d", p.Range, 6*hour)
		}
	})

	t.Run("bounds", func(t *testing.T) {
		p := &Plan{Metas: []*block.Meta{meta(1000, 2000, 1), meta(500, 3000, 1)}}
		mint, maxt := p.Bounds()
		if mint != 500 || maxt != 3000 {
			t.Errorf("Bounds() = (%d, %d), want (500, 3000)", mint, maxt)
		}
	})
}

func TestApplyRetention(t *testing.T) {
	dir := t.TempDir()
	now := time.Now()

	old := writeBlockFrom(t, dir, map[string][]model.Sample{
		"a": {{T: now.Add(-48 * time.Hour).UnixMilli(), V: 1}},
	})
	recent := writeBlockFrom(t, dir, map[string][]model.Sample{
		"b": {{T: now.Add(-1 * time.Hour).UnixMilli(), V: 2}},
	})

	c := New(dir, Options{Retention: 24 * time.Hour})
	n, err := c.ApplyRetention([]*block.Meta{old, recent}, now)
	if err != nil {
		t.Fatalf("ApplyRetention: %v", err)
	}
	if n != 1 {
		t.Errorf("deleted %d blocks, want 1", n)
	}

	dirs, _ := block.List(dir)
	if len(dirs) != 1 {
		t.Fatalf("%d blocks remain, want 1", len(dirs))
	}
	if filepath.Base(dirs[0]) != recent.ID.String() {
		t.Errorf("the wrong block survived: %s", filepath.Base(dirs[0]))
	}
}

func TestApplyRetentionDisabled(t *testing.T) {
	dir := t.TempDir()
	m := writeBlockFrom(t, dir, map[string][]model.Sample{"a": {{T: 0, V: 1}}})

	c := New(dir, Options{Retention: 0})
	n, err := c.ApplyRetention([]*block.Meta{m}, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Errorf("retention deleted %d blocks while disabled", n)
	}
}

// TestRechunkPassesThroughOrderedInput covers the fast path: a series that
// lives in one block must not be decoded and re-encoded at all.
func TestRechunkPassesThroughOrderedInput(t *testing.T) {
	var chunks []chunk.Chunk
	ts := int64(0)
	for i := 0; i < 3; i++ {
		c := chunk.NewXORChunk()
		app, _ := c.Appender()
		for j := 0; j < 10; j++ {
			if err := app.Append(ts, float64(ts)); err != nil {
				t.Fatal(err)
			}
			ts += 1000
		}
		chunks = append(chunks, c)
	}

	got, err := rechunk(chunks, 10)
	if err != nil {
		t.Fatalf("rechunk: %v", err)
	}
	if len(got) != len(chunks) {
		t.Fatalf("got %d chunks, want %d", len(got), len(chunks))
	}
	for i := range got {
		if got[i] != chunks[i] {
			t.Errorf("chunk %d was re-encoded; ordered input should pass through untouched", i)
		}
	}
}

func TestRechunkMergesOverlapping(t *testing.T) {
	mk := func(samples ...model.Sample) chunk.Chunk {
		c := chunk.NewXORChunk()
		app, _ := c.Appender()
		for _, s := range samples {
			if err := app.Append(s.T, s.V); err != nil {
				t.Fatal(err)
			}
		}
		return c
	}

	chunks := []chunk.Chunk{
		mk(model.Sample{T: 0, V: 1}, model.Sample{T: 2000, V: 3}),
		mk(model.Sample{T: 1000, V: 2}, model.Sample{T: 2000, V: 99}), // later wins at 2000
		mk(model.Sample{T: 3000, V: 4}),
	}

	got, err := rechunk(chunks, 2)
	if err != nil {
		t.Fatalf("rechunk: %v", err)
	}

	var samples []model.Sample
	for _, c := range got {
		it := c.Iterator(nil)
		for it.Next() {
			ts, v := it.At()
			samples = append(samples, model.Sample{T: ts, V: v})
		}
		if err := it.Err(); err != nil {
			t.Fatal(err)
		}
	}

	want := []model.Sample{{T: 0, V: 1}, {T: 1000, V: 2}, {T: 2000, V: 99}, {T: 3000, V: 4}}
	if len(samples) != len(want) {
		t.Fatalf("got %v, want %v", samples, want)
	}
	for i := range want {
		if samples[i] != want[i] {
			t.Errorf("sample %d = %v, want %v", i, samples[i], want[i])
		}
	}

	// Chunks must respect the size limit.
	for i, c := range got {
		if c.NumSamples() > 2 {
			t.Errorf("chunk %d has %d samples, want at most 2", i, c.NumSamples())
		}
	}
}

func TestRechunkEmpty(t *testing.T) {
	got, err := rechunk(nil, 10)
	if err != nil {
		t.Fatal(err)
	}
	if got != nil {
		t.Errorf("rechunk(nil) = %v, want nil", got)
	}
}

// TestFlushThenCompactRoundTrip exercises the whole path: ingest into a head,
// flush to blocks, compact them, and confirm every sample survives.
func TestFlushThenCompactRoundTrip(t *testing.T) {
	dir := t.TempDir()

	const (
		series   = 20
		perFlush = 50
		flushes  = 4
		interval = int64(15_000)
	)

	want := make(map[string][]model.Sample)
	c := New(dir, Options{SamplesPerChunk: 16})

	for f := 0; f < flushes; f++ {
		batch := make(map[string][]model.Sample)
		for s := 0; s < series; s++ {
			name := fmt.Sprintf("host-%02d", s)
			for i := 0; i < perFlush; i++ {
				ts := int64(f*perFlush+i) * interval
				smpl := model.Sample{T: ts, V: math.Sin(float64(ts)/1e6) * float64(s+1)}
				batch[name] = append(batch[name], smpl)
				want[name] = append(want[name], smpl)
			}
		}
		h := buildHead(t, 16, batch)
		if _, err := c.Flush(h, model.MinTime, model.MaxTime); err != nil {
			t.Fatalf("flush %d: %v", f, err)
		}
	}

	dirs, _ := block.List(dir)
	if len(dirs) != flushes {
		t.Fatalf("%d blocks after %d flushes", len(dirs), flushes)
	}

	// Compact everything into one block.
	var metas []*block.Meta
	for _, d := range dirs {
		m, err := block.ReadMeta(d)
		if err != nil {
			t.Fatal(err)
		}
		metas = append(metas, m)
	}
	meta, err := c.Compact(&Plan{Metas: metas})
	if err != nil {
		t.Fatalf("Compact: %v", err)
	}
	if meta.Stats.NumSeries != series {
		t.Errorf("NumSeries = %d, want %d", meta.Stats.NumSeries, series)
	}
	if meta.Stats.NumSamples != uint64(series*perFlush*flushes) {
		t.Errorf("NumSamples = %d, want %d", meta.Stats.NumSamples, series*perFlush*flushes)
	}

	dirs, _ = block.List(dir)
	if len(dirs) != 1 {
		t.Fatalf("%d blocks after compaction, want 1", len(dirs))
	}
	got := readBlock(t, dirs[0])

	for s := 0; s < series; s++ {
		name := fmt.Sprintf("host-%02d", s)
		key := model.FromStrings(model.MetricName, "m", "host", name).String()
		gotSamples, wantSamples := got[key], want[name]
		if len(gotSamples) != len(wantSamples) {
			t.Fatalf("series %s has %d samples, want %d", name, len(gotSamples), len(wantSamples))
		}
		for i := range wantSamples {
			if gotSamples[i] != wantSamples[i] {
				t.Fatalf("series %s sample %d = %v, want %v",
					name, i, gotSamples[i], wantSamples[i])
			}
		}
	}
}

func BenchmarkCompactTwoBlocks(b *testing.B) {
	build := func(dir string, offset int64) *block.Meta {
		h := memtable.NewHead(memtable.Options{SamplesPerChunk: 120})
		app := h.Appender()
		for s := 0; s < 500; s++ {
			ls := model.FromStrings(model.MetricName, "m", "host", fmt.Sprintf("h-%04d", s))
			for i := 0; i < 120; i++ {
				if _, err := app.Append(0, ls, offset+int64(i)*15_000, float64(i)); err != nil {
					b.Fatal(err)
				}
			}
		}
		if err := app.Commit(); err != nil {
			b.Fatal(err)
		}
		c := New(dir, Options{SamplesPerChunk: 120})
		m, err := c.Flush(h, model.MinTime, model.MaxTime)
		if err != nil {
			b.Fatal(err)
		}
		return m
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		dir := b.TempDir()
		m1 := build(dir, 0)
		m2 := build(dir, 120*15_000)
		c := New(dir, Options{SamplesPerChunk: 120})
		b.StartTimer()

		if _, err := c.Compact(&Plan{Metas: []*block.Meta{m1, m2}}); err != nil {
			b.Fatal(err)
		}
	}
}
