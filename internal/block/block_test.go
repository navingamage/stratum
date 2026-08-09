package block

import (
	"errors"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"testing"

	"github.com/navingamage/stratum/internal/chunk"
	"github.com/navingamage/stratum/internal/index"
	"github.com/navingamage/stratum/internal/model"
)

// testSeries is one series' worth of data for building a block.
type testSeries struct {
	labels  model.Labels
	samples []model.Sample
}

// sliceSource turns a slice of test series into a SeriesSource.
type sliceSource struct {
	series []testSeries
	pos    int
	// chunkSize is how many samples go into each chunk.
	chunkSize int
	err       error
}

func newSliceSource(series []testSeries, chunkSize int) *sliceSource {
	sorted := append([]testSeries(nil), series...)
	sort.Slice(sorted, func(i, j int) bool {
		return model.Compare(sorted[i].labels, sorted[j].labels) < 0
	})
	return &sliceSource{series: sorted, chunkSize: chunkSize, pos: -1}
}

func (s *sliceSource) Symbols() []string {
	set := make(map[string]struct{})
	for _, ts := range s.series {
		for _, l := range ts.labels {
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

func (s *sliceSource) Next() bool {
	s.pos++
	return s.pos < len(s.series)
}

func (s *sliceSource) At() (model.Labels, []chunk.Chunk) {
	ts := s.series[s.pos]

	var out []chunk.Chunk
	for i := 0; i < len(ts.samples); i += s.chunkSize {
		end := min(i+s.chunkSize, len(ts.samples))
		c := chunk.NewXORChunk()
		app, err := c.Appender()
		if err != nil {
			s.err = err
			return ts.labels, nil
		}
		for _, smpl := range ts.samples[i:end] {
			if err := app.Append(smpl.T, smpl.V); err != nil {
				s.err = err
				return ts.labels, nil
			}
		}
		out = append(out, c)
	}
	return ts.labels, out
}

func (s *sliceSource) Err() error { return s.err }

// makeSeries builds n series with samples per series.
func makeSeries(n, samples int) []testSeries {
	out := make([]testSeries, 0, n)
	for i := 0; i < n; i++ {
		ts := testSeries{
			labels: model.FromStrings(
				model.MetricName, "cpu_seconds_total",
				"host", fmt.Sprintf("web-%03d", i),
				"env", map[bool]string{true: "prod", false: "staging"}[i%3 == 0],
			),
		}
		for j := 0; j < samples; j++ {
			ts.samples = append(ts.samples, model.Sample{
				T: int64(j) * 15_000,
				V: float64(i*1000 + j),
			})
		}
		out = append(out, ts)
	}
	return out
}

// writeTestBlock builds a block and opens it.
func writeTestBlock(t *testing.T, series []testSeries, chunkSize int) *Block {
	t.Helper()
	dir := t.TempDir()

	meta, err := Write(dir, newSliceSource(series, chunkSize), Compaction{Level: 1})
	if err != nil {
		t.Fatalf("Write: %v", err)
	}
	if meta == nil {
		t.Fatal("Write returned no metadata for a non-empty source")
	}

	dirs, err := List(dir)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(dirs) != 1 {
		t.Fatalf("List returned %d blocks, want 1", len(dirs))
	}

	b, err := Open(dirs[0])
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { b.Close() })
	return b
}

// readAllSamples pulls every sample of a series out of a block.
func readAllSamples(t *testing.T, b *Block, id model.SeriesRef) (model.Labels, []model.Sample) {
	t.Helper()
	ls, chunks, err := b.SeriesChunks(id, model.MinTime, model.MaxTime)
	if err != nil {
		t.Fatalf("SeriesChunks: %v", err)
	}
	var out []model.Sample
	for _, c := range chunks {
		it := c.Iterator(nil)
		for it.Next() {
			ts, v := it.At()
			out = append(out, model.Sample{T: ts, V: v})
		}
		if err := it.Err(); err != nil {
			t.Fatalf("chunk iterator: %v", err)
		}
	}
	return ls, out
}

func TestBlockRoundTrip(t *testing.T) {
	want := makeSeries(50, 300)
	b := writeTestBlock(t, want, 120)

	meta := b.Meta()
	if meta.Stats.NumSeries != 50 {
		t.Errorf("NumSeries = %d, want 50", meta.Stats.NumSeries)
	}
	if meta.Stats.NumSamples != 50*300 {
		t.Errorf("NumSamples = %d, want %d", meta.Stats.NumSamples, 50*300)
	}
	if meta.MinTime != 0 || meta.MaxTime != 299*15_000 {
		t.Errorf("bounds [%d, %d], want [0, %d]", meta.MinTime, meta.MaxTime, 299*15_000)
	}
	if meta.Compaction.Level != 1 {
		t.Errorf("compaction level = %d, want 1", meta.Compaction.Level)
	}

	// Every series must come back with its labels and samples intact.
	sorted := newSliceSource(want, 120).series
	for i := range sorted {
		ls, got := readAllSamples(t, b, model.SeriesRef(i))
		if !ls.Equal(sorted[i].labels) {
			t.Fatalf("series %d labels = %s, want %s", i, ls, sorted[i].labels)
		}
		if len(got) != len(sorted[i].samples) {
			t.Fatalf("series %d has %d samples, want %d", i, len(got), len(sorted[i].samples))
		}
		for j := range got {
			if got[j] != sorted[i].samples[j] {
				t.Fatalf("series %d sample %d = %v, want %v", i, j, got[j], sorted[i].samples[j])
			}
		}
	}
}

func TestBlockIndexQueries(t *testing.T) {
	b := writeTestBlock(t, makeSeries(30, 10), 120)
	idx := b.Index()

	if got := idx.NumSeries(); got != 30 {
		t.Errorf("NumSeries = %d, want 30", got)
	}

	want := []string{model.MetricName, "env", "host"}
	if got := idx.LabelNames(); !reflect.DeepEqual(got, want) {
		t.Errorf("LabelNames = %v, want %v", got, want)
	}
	if got := idx.LabelValues("env"); !reflect.DeepEqual(got, []string{"prod", "staging"}) {
		t.Errorf("LabelValues(env) = %v", got)
	}
	if got := idx.LabelValues("nosuch"); got != nil {
		t.Errorf("LabelValues(nosuch) = %v, want nil", got)
	}

	all, err := index.ExpandPostings(idx.All())
	if err != nil {
		t.Fatal(err)
	}
	if len(all) != 30 {
		t.Errorf("All() returned %d series, want 30", len(all))
	}

	// The same matcher resolution the head uses must work against a block.
	p, err := index.PostingsForMatchers(idx,
		model.MustNewMatcher(model.MatchEqual, "env", "prod"))
	if err != nil {
		t.Fatal(err)
	}
	refs, err := index.ExpandPostings(p)
	if err != nil {
		t.Fatal(err)
	}
	if len(refs) != 10 {
		t.Errorf("env=prod matched %d series, want 10", len(refs))
	}
	for _, ref := range refs {
		ls, _, err := b.Series(ref)
		if err != nil {
			t.Fatal(err)
		}
		if ls.Get("env") != "prod" {
			t.Errorf("series %d has env=%q", ref, ls.Get("env"))
		}
	}
}

// TestBlockPostingsAgainstBruteForce checks that resolving matchers against a
// persisted block gives exactly the same answer as filtering every series by
// hand - the property that lets the query engine treat blocks and the head
// interchangeably.
func TestBlockPostingsAgainstBruteForce(t *testing.T) {
	series := makeSeries(60, 5)
	b := writeTestBlock(t, series, 120)
	idx := b.Index()

	matchers := [][]*model.Matcher{
		{model.MustNewMatcher(model.MatchEqual, "env", "prod")},
		{model.MustNewMatcher(model.MatchNotEqual, "env", "prod")},
		{model.MustNewMatcher(model.MatchRegexp, "host", "web-00[0-5]")},
		{model.MustNewMatcher(model.MatchNotRegexp, "host", "web-0[0-4].*")},
		{model.MustNewMatcher(model.MatchEqual, model.MetricName, "cpu_seconds_total"),
			model.MustNewMatcher(model.MatchEqual, "env", "staging")},
		{model.MustNewMatcher(model.MatchEqual, "nosuch", "x")},
		{model.MustNewMatcher(model.MatchNotEqual, "nosuch", "x")},
	}

	for _, ms := range matchers {
		t.Run(model.MatchersString(ms), func(t *testing.T) {
			p, err := index.PostingsForMatchers(idx, ms...)
			if err != nil {
				t.Fatal(err)
			}
			got, err := index.ExpandPostings(p)
			if err != nil {
				t.Fatal(err)
			}

			var want []model.SeriesRef
			for i := 0; i < idx.NumSeries(); i++ {
				ls, _, err := b.Series(model.SeriesRef(i))
				if err != nil {
					t.Fatal(err)
				}
				ok := true
				for _, m := range ms {
					if !m.MatchesLabels(ls) {
						ok = false
						break
					}
				}
				if ok {
					want = append(want, model.SeriesRef(i))
				}
			}

			if !reflect.DeepEqual(got, want) {
				t.Errorf("got %v, want %v", got, want)
			}
		})
	}
}

func TestBlockChunkSelectionByTime(t *testing.T) {
	// 10 chunks of 30 samples each, spanning 0..299 * 15s.
	b := writeTestBlock(t, makeSeries(1, 300), 30)

	_, metas, err := b.Series(0)
	if err != nil {
		t.Fatal(err)
	}
	if len(metas) != 10 {
		t.Fatalf("series has %d chunks, want 10", len(metas))
	}

	// A window covering one chunk must load only the chunks that overlap it.
	_, chunks, err := b.SeriesChunks(0, 60*15_000, 65*15_000)
	if err != nil {
		t.Fatal(err)
	}
	if len(chunks) != 1 {
		t.Errorf("a 6-sample window loaded %d chunks, want 1", len(chunks))
	}

	// Outside the block entirely.
	_, chunks, err = b.SeriesChunks(0, 1<<40, 1<<41)
	if err != nil {
		t.Fatal(err)
	}
	if len(chunks) != 0 {
		t.Errorf("a window past the block loaded %d chunks", len(chunks))
	}
}

func TestBlockOverlaps(t *testing.T) {
	b := writeTestBlock(t, makeSeries(1, 100), 120)
	mint, maxt := b.MinTime(), b.MaxTime()

	cases := []struct {
		lo, hi int64
		want   bool
	}{
		{mint, maxt, true},
		{mint - 1000, mint, true},
		{maxt, maxt + 1000, true},
		{mint - 2000, mint - 1000, false},
		{maxt + 1000, maxt + 2000, false},
		{model.MinTime, model.MaxTime, true},
	}
	for _, tc := range cases {
		if got := b.Overlaps(tc.lo, tc.hi); got != tc.want {
			t.Errorf("Overlaps(%d, %d) = %v, want %v", tc.lo, tc.hi, got, tc.want)
		}
	}
}

func TestWriteRejectsUnorderedSeries(t *testing.T) {
	// Deliberately unsorted: the reader binary searches series, so an
	// unsorted block would fail as a wrong answer rather than an error.
	src := &sliceSource{
		series: []testSeries{
			{labels: model.FromStrings("host", "z"), samples: []model.Sample{{T: 1, V: 1}}},
			{labels: model.FromStrings("host", "a"), samples: []model.Sample{{T: 1, V: 1}}},
		},
		chunkSize: 10,
		pos:       -1,
	}
	if _, err := Write(t.TempDir(), src, Compaction{Level: 1}); err == nil {
		t.Error("Write accepted out-of-order series, want an error")
	}
}

func TestWriteEmptySourceProducesNoBlock(t *testing.T) {
	dir := t.TempDir()
	meta, err := Write(dir, newSliceSource(nil, 10), Compaction{Level: 1})
	if err != nil {
		t.Fatalf("Write: %v", err)
	}
	if meta != nil {
		t.Errorf("Write returned metadata %v for an empty source", meta)
	}

	// Nothing must be left behind, or compaction planning would keep
	// rediscovering an empty block forever.
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Errorf("an empty source left %d entries behind", len(entries))
	}
}

func TestWriteIsAtomic(t *testing.T) {
	dir := t.TempDir()
	if _, err := Write(dir, newSliceSource(makeSeries(5, 10), 10), Compaction{Level: 1}); err != nil {
		t.Fatal(err)
	}

	// No .tmp directory may survive a successful write.
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if filepath.Ext(e.Name()) == TmpSuffix {
			t.Errorf("a temporary directory %s survived a successful write", e.Name())
		}
	}
}

func TestListSkipsIncompleteBlocks(t *testing.T) {
	dir := t.TempDir()
	if _, err := Write(dir, newSliceSource(makeSeries(3, 5), 10), Compaction{Level: 1}); err != nil {
		t.Fatal(err)
	}

	// A partial block: correctly named, but with no meta.json.
	id, _ := NewID()
	partial := filepath.Join(dir, id.String())
	if err := os.MkdirAll(partial, 0o777); err != nil {
		t.Fatal(err)
	}
	// A leftover temp directory from an interrupted write.
	tmp := filepath.Join(dir, id.String()+TmpSuffix)
	if err := os.MkdirAll(tmp, 0o777); err != nil {
		t.Fatal(err)
	}
	// Something that is not a block at all.
	if err := os.MkdirAll(filepath.Join(dir, "wal"), 0o777); err != nil {
		t.Fatal(err)
	}

	dirs, err := List(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(dirs) != 1 {
		t.Errorf("List returned %d blocks, want 1: %v", len(dirs), dirs)
	}

	if err := CleanTmpDirs(dir); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(tmp); !os.IsNotExist(err) {
		t.Error("CleanTmpDirs left the temporary directory in place")
	}
}

func TestListReturnsBlocksInCreationOrder(t *testing.T) {
	dir := t.TempDir()
	var ids []string
	for i := 0; i < 8; i++ {
		meta, err := Write(dir, newSliceSource(makeSeries(2, 3), 10), Compaction{Level: 1})
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, meta.ID.String())
	}

	dirs, err := List(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(dirs) != len(ids) {
		t.Fatalf("List returned %d blocks, want %d", len(dirs), len(ids))
	}
	// IDs sort in creation order, so no sorting by metadata is needed. This
	// is the property compaction planning depends on.
	for i, d := range dirs {
		if filepath.Base(d) != ids[i] {
			t.Errorf("block %d is %s, want %s (listing is not in creation order)",
				i, filepath.Base(d), ids[i])
		}
	}
}

func TestDelete(t *testing.T) {
	dir := t.TempDir()
	meta, err := Write(dir, newSliceSource(makeSeries(3, 5), 10), Compaction{Level: 1})
	if err != nil {
		t.Fatal(err)
	}
	blockDir := filepath.Join(dir, meta.ID.String())

	if err := Delete(blockDir); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if dirs, _ := List(dir); len(dirs) != 0 {
		t.Errorf("List returned %d blocks after Delete", len(dirs))
	}
	// Deleting again is not an error: compaction may retry after a crash.
	if err := Delete(blockDir); err != nil {
		t.Errorf("second Delete = %v, want nil", err)
	}
}

func TestOpenRejectsCorruptFiles(t *testing.T) {
	corrupt := func(t *testing.T, name string, mutate func([]byte) []byte) error {
		t.Helper()
		dir := t.TempDir()
		meta, err := Write(dir, newSliceSource(makeSeries(5, 20), 10), Compaction{Level: 1})
		if err != nil {
			t.Fatal(err)
		}
		path := filepath.Join(dir, meta.ID.String(), name)
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, mutate(data), 0o666); err != nil {
			t.Fatal(err)
		}
		b, err := Open(filepath.Join(dir, meta.ID.String()))
		if err == nil {
			b.Close()
		}
		return err
	}

	t.Run("index magic", func(t *testing.T) {
		err := corrupt(t, IndexFilename, func(b []byte) []byte {
			b[0] ^= 0xFF
			return b
		})
		if !errors.Is(err, ErrCorruptBlock) {
			t.Errorf("Open with a bad index magic = %v, want ErrCorruptBlock", err)
		}
	})

	t.Run("chunk magic", func(t *testing.T) {
		err := corrupt(t, ChunksFilename, func(b []byte) []byte {
			b[0] ^= 0xFF
			return b
		})
		if !errors.Is(err, ErrCorruptBlock) {
			t.Errorf("Open with a bad chunk magic = %v, want ErrCorruptBlock", err)
		}
	})

	t.Run("truncated index", func(t *testing.T) {
		err := corrupt(t, IndexFilename, func(b []byte) []byte { return b[:len(b)/2] })
		if err == nil {
			t.Error("Open of a truncated index succeeded, want an error")
		}
	})

	t.Run("table of contents", func(t *testing.T) {
		err := corrupt(t, IndexFilename, func(b []byte) []byte {
			b[len(b)-tocSize] ^= 0xFF
			return b
		})
		if !errors.Is(err, ErrCorruptBlock) {
			t.Errorf("Open with a corrupt table of contents = %v, want ErrCorruptBlock", err)
		}
	})
}

// TestChunkChecksumDetectsCorruption covers the per-chunk checksum: a query
// touching a damaged chunk must fail loudly rather than return wrong numbers.
func TestChunkChecksumDetectsCorruption(t *testing.T) {
	dir := t.TempDir()
	meta, err := Write(dir, newSliceSource(makeSeries(5, 50), 20), Compaction{Level: 1})
	if err != nil {
		t.Fatal(err)
	}
	blockDir := filepath.Join(dir, meta.ID.String())

	path := filepath.Join(blockDir, ChunksFilename)
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	// Flip a bit inside the first chunk's payload, past the header.
	data[chunksHeaderSize+8] ^= 0x01
	if err := os.WriteFile(path, data, 0o666); err != nil {
		t.Fatal(err)
	}

	b, err := Open(blockDir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer b.Close()

	// Opening succeeds - chunks are verified on read, not on open - but the
	// affected read fails.
	var sawError bool
	for i := 0; i < b.Index().NumSeries(); i++ {
		if _, _, err := b.SeriesChunks(model.SeriesRef(i), model.MinTime, model.MaxTime); err != nil {
			if !errors.Is(err, ErrCorruptBlock) {
				t.Fatalf("SeriesChunks = %v, want ErrCorruptBlock", err)
			}
			sawError = true
		}
	}
	if !sawError {
		t.Error("a corrupted chunk was read without complaint")
	}
}

func TestIndexReaderRejectsUnknownSymbol(t *testing.T) {
	dir := t.TempDir()
	iw, err := NewIndexWriter(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := iw.AddSymbols([]string{"host"}); err != nil {
		t.Fatal(err)
	}
	// The value is not in the symbol table.
	err = iw.AddSeries(model.FromStrings("host", "web-1"), []ChunkMeta{{Ref: 0, MinTime: 0, MaxTime: 1}})
	if err == nil {
		t.Error("AddSeries accepted an undeclared symbol, want an error")
	}
	iw.Close()
}

func TestSeriesOutOfRange(t *testing.T) {
	b := writeTestBlock(t, makeSeries(3, 5), 10)
	if _, _, err := b.Series(99); !errors.Is(err, ErrCorruptBlock) {
		t.Errorf("Series(99) = %v, want ErrCorruptBlock", err)
	}
}

func TestBlockIDRoundTrip(t *testing.T) {
	for i := 0; i < 100; i++ {
		id, err := NewID()
		if err != nil {
			t.Fatal(err)
		}
		s := id.String()
		if len(s) != 26 {
			t.Fatalf("ID renders to %d characters, want 26: %q", len(s), s)
		}
		parsed, err := ParseID(s)
		if err != nil {
			t.Fatalf("ParseID(%q): %v", s, err)
		}
		if parsed != id {
			t.Fatalf("round trip changed the ID: %x -> %q -> %x", id, s, parsed)
		}
	}
}

// TestBlockIDsAreMonotonic is the property block listing relies on: IDs must
// sort in creation order, including several generated within one millisecond.
func TestBlockIDsAreMonotonic(t *testing.T) {
	const n = 5000
	ids := make([]ID, n)
	for i := range ids {
		id, err := NewID()
		if err != nil {
			t.Fatal(err)
		}
		ids[i] = id
	}

	for i := 1; i < n; i++ {
		if ids[i].Compare(ids[i-1]) <= 0 {
			t.Fatalf("id %d does not sort after id %d: %s then %s",
				i, i-1, ids[i-1], ids[i])
		}
		// Lexical order of the rendered form must agree with byte order, or
		// a directory listing would not be in creation order.
		if ids[i].String() <= ids[i-1].String() {
			t.Fatalf("rendered id %d does not sort after id %d: %s then %s",
				i, i-1, ids[i-1], ids[i])
		}
	}
}

func TestParseIDRejectsBadInput(t *testing.T) {
	for _, s := range []string{"", "short", "0123456789012345678901234!", "IIIIIIIIIIIIIIIIIIIIIIIIII"} {
		if _, err := ParseID(s); err == nil {
			t.Errorf("ParseID(%q) succeeded, want an error", s)
		}
	}
}

func TestIDTime(t *testing.T) {
	id, err := NewID()
	if err != nil {
		t.Fatal(err)
	}
	// The encoded timestamp must be recoverable and recent.
	delta := id.Time().UnixMilli()
	now := timeNowMilli()
	if delta > now || now-delta > 5000 {
		t.Errorf("ID timestamp is %d, want something close to %d", delta, now)
	}
}

func timeNowMilli() int64 {
	id, _ := NewID()
	return id.Time().UnixMilli()
}

func TestMetaRoundTrip(t *testing.T) {
	dir := t.TempDir()
	id, _ := NewID()
	src, _ := NewID()

	want := &Meta{
		ID:      id,
		MinTime: 100,
		MaxTime: 200,
		Stats:   Stats{NumSamples: 10, NumSeries: 2, NumChunks: 3},
		Compaction: Compaction{
			Level:   2,
			Sources: []ID{src},
			Parents: []ID{src},
		},
	}
	if err := WriteMeta(dir, want); err != nil {
		t.Fatalf("WriteMeta: %v", err)
	}

	got, err := ReadMeta(dir)
	if err != nil {
		t.Fatalf("ReadMeta: %v", err)
	}
	if got.ID != want.ID || got.MinTime != want.MinTime || got.MaxTime != want.MaxTime {
		t.Errorf("got %+v, want %+v", got, want)
	}
	if got.Stats != want.Stats {
		t.Errorf("stats = %+v, want %+v", got.Stats, want.Stats)
	}
	if got.Compaction.Level != 2 || len(got.Compaction.Sources) != 1 || got.Compaction.Sources[0] != src {
		t.Errorf("compaction = %+v", got.Compaction)
	}

	// No temporary file may survive.
	if _, err := os.Stat(filepath.Join(dir, MetaFilename+".tmp")); !os.IsNotExist(err) {
		t.Error("WriteMeta left a temporary file behind")
	}
}

func TestReadMetaRejectsFutureVersion(t *testing.T) {
	dir := t.TempDir()
	body := `{"version": 999, "id": "00000000000000000000000000"}`
	if err := os.WriteFile(filepath.Join(dir, MetaFilename), []byte(body), 0o666); err != nil {
		t.Fatal(err)
	}
	if _, err := ReadMeta(dir); err == nil {
		t.Error("ReadMeta accepted a future schema version, want an error")
	}
}

func BenchmarkBlockWrite(b *testing.B) {
	series := makeSeries(1000, 120)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		dir := b.TempDir()
		if _, err := Write(dir, newSliceSource(series, 120), Compaction{Level: 1}); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkBlockPostings(b *testing.B) {
	dir := b.TempDir()
	series := make([]testSeries, 0, 20_000)
	rng := rand.New(rand.NewSource(1))
	for i := 0; i < 20_000; i++ {
		series = append(series, testSeries{
			labels: model.FromStrings(
				model.MetricName, "http_requests_total",
				"pod", fmt.Sprintf("pod-%05d", i),
				"zone", fmt.Sprintf("zone-%d", rng.Intn(4)),
			),
			samples: []model.Sample{{T: 1000, V: 1}},
		})
	}
	if _, err := Write(dir, newSliceSource(series, 120), Compaction{Level: 1}); err != nil {
		b.Fatal(err)
	}
	dirs, _ := List(dir)
	blk, err := Open(dirs[0])
	if err != nil {
		b.Fatal(err)
	}
	defer blk.Close()

	b.Run("point lookup", func(b *testing.B) {
		m := model.MustNewMatcher(model.MatchEqual, "pod", "pod-10000")
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			p, err := index.PostingsForMatchers(blk.Index(), m)
			if err != nil {
				b.Fatal(err)
			}
			for p.Next() {
			}
		}
	})

	b.Run("selective intersection", func(b *testing.B) {
		ms := []*model.Matcher{
			model.MustNewMatcher(model.MatchEqual, "zone", "zone-1"),
			model.MustNewMatcher(model.MatchEqual, "pod", "pod-10000"),
		}
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			p, err := index.PostingsForMatchers(blk.Index(), ms...)
			if err != nil {
				b.Fatal(err)
			}
			for p.Next() {
			}
		}
	})
}
