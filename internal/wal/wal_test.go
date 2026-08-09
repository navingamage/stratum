package wal

import (
	"bytes"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

// testOpts keeps segments small so multi-segment behaviour is reachable
// without writing hundreds of megabytes.
func testOpts() Options {
	return Options{
		SegmentSize: 4 * PageSize,
		Sync:        SyncNever,
	}
}

// writeRecords opens a log in a temp dir, writes recs, and closes it.
func writeRecords(t *testing.T, opts Options, recs ...[]byte) string {
	t.Helper()
	dir := t.TempDir()

	w, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	for i, r := range recs {
		if err := w.Log(r); err != nil {
			t.Fatalf("Log(%d): %v", i, err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	return dir
}

// readAll replays every record in dir.
func readAll(t *testing.T, dir string) ([][]byte, error) {
	t.Helper()
	r, err := NewReplayer(dir)
	if err != nil {
		return nil, err
	}
	defer r.Close()

	var out [][]byte
	for r.Next() {
		out = append(out, append([]byte(nil), r.Record()...))
	}
	return out, r.Err()
}

func equalRecords(a, b [][]byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if !bytes.Equal(a[i], b[i]) {
			return false
		}
	}
	return true
}

func TestRoundTrip(t *testing.T) {
	recs := [][]byte{
		[]byte("first"),
		[]byte(""), // a zero-length record must survive as a record
		[]byte("a longer record with some content"),
		bytes.Repeat([]byte("x"), 1000),
	}
	dir := writeRecords(t, testOpts(), recs...)

	got, err := readAll(t, dir)
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	if !equalRecords(got, recs) {
		t.Errorf("replayed %d records, want %d", len(got), len(recs))
		for i := range got {
			if i < len(recs) && !bytes.Equal(got[i], recs[i]) {
				t.Errorf("record %d: got %q, want %q", i, got[i], recs[i])
			}
		}
	}
}

// TestFragmentation covers records that must be split across pages, which is
// where the first/middle/last fragment logic lives.
func TestFragmentation(t *testing.T) {
	sizes := []int{
		PageSize - recordHeaderSize - 1, // just under one page
		PageSize - recordHeaderSize,     // exactly one page
		PageSize - recordHeaderSize + 1, // just over: two fragments
		PageSize,
		2 * PageSize,
		5*PageSize + 13, // several middle fragments
	}
	for _, size := range sizes {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			rec := make([]byte, size)
			for i := range rec {
				rec[i] = byte(i * 31)
			}
			// A short record either side, so a mis-sized fragment corrupts a
			// neighbour rather than going unnoticed.
			recs := [][]byte{[]byte("before"), rec, []byte("after")}

			opts := testOpts()
			opts.SegmentSize = 16 * PageSize
			dir := writeRecords(t, opts, recs...)

			got, err := readAll(t, dir)
			if err != nil {
				t.Fatalf("replay: %v", err)
			}
			if !equalRecords(got, recs) {
				t.Fatalf("round trip failed for size %d: got %d records", size, len(got))
			}
		})
	}
}

func TestMultipleSegments(t *testing.T) {
	// testOpts caps a segment at 4 pages (128KiB); 400 records of ~800 bytes
	// spills across several.
	var recs [][]byte
	for i := 0; i < 400; i++ {
		recs = append(recs, []byte(fmt.Sprintf("record-%04d-%s", i, bytes.Repeat([]byte("p"), 800))))
	}
	dir := writeRecords(t, testOpts(), recs...)

	first, last, err := Segments(dir)
	if err != nil {
		t.Fatalf("Segments: %v", err)
	}
	if last-first < 2 {
		t.Fatalf("expected several segments, got %d..%d", first, last)
	}

	got, err := readAll(t, dir)
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	if !equalRecords(got, recs) {
		t.Errorf("replayed %d records, want %d", len(got), len(recs))
	}
}

func TestRecordTooBig(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(dir, testOpts())
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	if err := w.Log(make([]byte, MaxRecordSize+1)); !errors.Is(err, ErrRecordTooBig) {
		t.Errorf("Log of an oversized record = %v, want ErrRecordTooBig", err)
	}
}

func TestLogBatchIsAllOrNothing(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(dir, testOpts())
	if err != nil {
		t.Fatal(err)
	}

	// The second record is rejected on size, so neither should be written.
	err = w.Log([]byte("ok"), make([]byte, MaxRecordSize+1))
	if !errors.Is(err, ErrRecordTooBig) {
		t.Fatalf("Log = %v, want ErrRecordTooBig", err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	got, err := readAll(t, dir)
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	if len(got) != 0 {
		t.Errorf("a rejected batch wrote %d records", len(got))
	}
}

func TestClosedLogRejectsWrites(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(dir, testOpts())
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	if err := w.Log([]byte("x")); !errors.Is(err, ErrClosed) {
		t.Errorf("Log after Close = %v, want ErrClosed", err)
	}
	if err := w.Sync(); !errors.Is(err, ErrClosed) {
		t.Errorf("Sync after Close = %v, want ErrClosed", err)
	}
	if err := w.Close(); !errors.Is(err, ErrClosed) {
		t.Errorf("double Close = %v, want ErrClosed", err)
	}
}

// TestTornWriteRecovery is the property the whole page format exists for.
// Truncating the log at any byte offset simulates a process killed mid-write.
// Replay must return a prefix of the records and report no error - never a
// partial record, never a spurious corruption report.
func TestTornWriteRecovery(t *testing.T) {
	var recs [][]byte
	for i := 0; i < 60; i++ {
		// Mixed sizes so both packed and fragmented records get truncated.
		n := 10 + (i*997)%(3*PageSize)
		rec := make([]byte, n)
		for j := range rec {
			rec[j] = byte(i)
		}
		recs = append(recs, rec)
	}

	opts := testOpts()
	opts.SegmentSize = 64 * PageSize
	dir := writeRecords(t, opts, recs...)

	// Work on a copy of the single segment.
	segPath := SegmentName(dir, 0)
	full, err := os.ReadFile(segPath)
	if err != nil {
		t.Fatalf("reading segment: %v", err)
	}

	rng := rand.New(rand.NewSource(20260809))
	offsets := []int{0, 1, recordHeaderSize - 1, recordHeaderSize, PageSize - 1, PageSize, PageSize + 1}
	for i := 0; i < 120; i++ {
		offsets = append(offsets, rng.Intn(len(full)+1))
	}

	for _, off := range offsets {
		if off > len(full) {
			continue
		}
		t.Run(fmt.Sprint(off), func(t *testing.T) {
			truncDir := t.TempDir()
			if err := os.WriteFile(SegmentName(truncDir, 0), full[:off], 0o666); err != nil {
				t.Fatal(err)
			}

			got, err := readAll(t, truncDir)
			if err != nil {
				t.Fatalf("truncated at %d: replay reported %v, want a clean end", off, err)
			}
			if len(got) > len(recs) {
				t.Fatalf("truncated at %d: replay produced %d records, more than the %d written",
					off, len(got), len(recs))
			}
			// Whatever survived must be an exact prefix. A record that decodes
			// to the wrong bytes is far worse than one that is missing.
			for i := range got {
				if !bytes.Equal(got[i], recs[i]) {
					t.Fatalf("truncated at %d: record %d differs from the original (%d vs %d bytes)",
						off, i, len(got[i]), len(recs[i]))
				}
			}
		})
	}
}

// TestCorruptionIsReported checks the other half of the judgement: damage in
// the middle of the log must not be mistaken for a torn tail, because
// silently dropping the remainder would turn corruption into invisible data
// loss.
func TestCorruptionIsReported(t *testing.T) {
	var recs [][]byte
	for i := 0; i < 50; i++ {
		recs = append(recs, []byte(fmt.Sprintf("record-%03d-payload-data", i)))
	}
	dir := writeRecords(t, testOpts(), recs...)

	segPath := SegmentName(dir, 0)
	full, err := os.ReadFile(segPath)
	if err != nil {
		t.Fatal(err)
	}

	// Corrupt a payload byte early in the log, well before the tail.
	corrupt := append([]byte(nil), full...)
	corrupt[recordHeaderSize+2] ^= 0xFF

	badDir := t.TempDir()
	if err := os.WriteFile(SegmentName(badDir, 0), corrupt, 0o666); err != nil {
		t.Fatal(err)
	}

	_, err = readAll(t, badDir)
	if !errors.Is(err, ErrCorrupt) {
		t.Errorf("replay of corrupt data = %v, want ErrCorrupt", err)
	}
}

func TestCorruptHeaderIsReported(t *testing.T) {
	dir := writeRecords(t, testOpts(),
		[]byte("aaaaaaaaaaaaaaaa"), []byte("bbbbbbbbbbbbbbbb"))

	full, err := os.ReadFile(SegmentName(dir, 0))
	if err != nil {
		t.Fatal(err)
	}

	// Flip a bit in the length field. The checksum covers the header, so this
	// is caught rather than steering the decoder into the wrong byte count.
	corrupt := append([]byte(nil), full...)
	corrupt[2] ^= 0x04

	badDir := t.TempDir()
	if err := os.WriteFile(SegmentName(badDir, 0), corrupt, 0o666); err != nil {
		t.Fatal(err)
	}
	if _, err := readAll(t, badDir); !errors.Is(err, ErrCorrupt) {
		t.Errorf("replay with a corrupt header = %v, want ErrCorrupt", err)
	}
}

func TestSegmentGapIsReported(t *testing.T) {
	dir := t.TempDir()
	for _, i := range []int{0, 1, 3} {
		if err := os.WriteFile(SegmentName(dir, i), make([]byte, PageSize), 0o666); err != nil {
			t.Fatal(err)
		}
	}
	if _, _, err := Segments(dir); !errors.Is(err, ErrCorrupt) {
		t.Errorf("Segments with a gap = %v, want ErrCorrupt", err)
	}
}

func TestSegmentsIgnoresNonSegmentFiles(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(SegmentName(dir, 0), nil, 0o666); err != nil {
		t.Fatal(err)
	}
	// Checkpoint directories and stray files live alongside segments.
	if err := os.WriteFile(filepath.Join(dir, "checkpoint.000123"), nil, 0o666); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(dir, "subdir"), 0o777); err != nil {
		t.Fatal(err)
	}

	first, last, err := Segments(dir)
	if err != nil {
		t.Fatalf("Segments: %v", err)
	}
	if first != 0 || last != 0 {
		t.Errorf("Segments = (%d, %d), want (0, 0)", first, last)
	}
}

func TestSegmentsOnEmptyDir(t *testing.T) {
	first, last, err := Segments(t.TempDir())
	if err != nil {
		t.Fatalf("Segments: %v", err)
	}
	if first != -1 || last != -1 {
		t.Errorf("Segments on an empty dir = (%d, %d), want (-1, -1)", first, last)
	}
}

func TestReopenStartsANewSegment(t *testing.T) {
	dir := t.TempDir()

	w, err := Open(dir, testOpts())
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Log([]byte("first run")); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	w2, err := Open(dir, testOpts())
	if err != nil {
		t.Fatal(err)
	}
	if err := w2.Log([]byte("second run")); err != nil {
		t.Fatal(err)
	}
	if err := w2.Close(); err != nil {
		t.Fatal(err)
	}

	_, last, err := Segments(dir)
	if err != nil {
		t.Fatal(err)
	}
	if last != 1 {
		t.Errorf("last segment = %d, want 1 (reopen should not append to an existing segment)", last)
	}

	got, err := readAll(t, dir)
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	want := [][]byte{[]byte("first run"), []byte("second run")}
	if !equalRecords(got, want) {
		t.Errorf("got %q, want %q", got, want)
	}
}

func TestNextSegment(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(dir, testOpts())
	if err != nil {
		t.Fatal(err)
	}

	if err := w.Log([]byte("a")); err != nil {
		t.Fatal(err)
	}
	if err := w.NextSegment(); err != nil {
		t.Fatalf("NextSegment: %v", err)
	}
	if err := w.Log([]byte("b")); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	_, last, _ := Segments(dir)
	if last != 1 {
		t.Errorf("last segment = %d, want 1", last)
	}
	got, err := readAll(t, dir)
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	if !equalRecords(got, [][]byte{[]byte("a"), []byte("b")}) {
		t.Errorf("got %q", got)
	}
}

func TestTruncate(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(dir, testOpts())
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	for i := 0; i < 4; i++ {
		if err := w.Log([]byte(fmt.Sprintf("segment %d", i))); err != nil {
			t.Fatal(err)
		}
		if err := w.NextSegment(); err != nil {
			t.Fatal(err)
		}
	}

	if err := w.Truncate(3); err != nil {
		t.Fatalf("Truncate: %v", err)
	}
	first, _, err := Segments(dir)
	if err != nil {
		t.Fatal(err)
	}
	if first != 3 {
		t.Errorf("first segment after Truncate(3) = %d, want 3", first)
	}
}

func TestTruncateKeepsTheActiveSegment(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(dir, testOpts())
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	if err := w.Log([]byte("only")); err != nil {
		t.Fatal(err)
	}
	// Far past the active segment: it must survive regardless.
	if err := w.Truncate(999); err != nil {
		t.Fatalf("Truncate: %v", err)
	}
	if _, err := os.Stat(SegmentName(dir, 0)); err != nil {
		t.Errorf("the active segment was deleted: %v", err)
	}
}

func TestSyncAlways(t *testing.T) {
	dir := t.TempDir()
	opts := testOpts()
	opts.Sync = SyncAlways

	w, err := Open(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Log([]byte("durable")); err != nil {
		t.Fatal(err)
	}

	// With SyncAlways the record is on disk before Log returns, so a reader
	// that never sees a Close must still find it.
	got, err := readAll(t, dir)
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	if len(got) != 1 || !bytes.Equal(got[0], []byte("durable")) {
		t.Errorf("got %q, want [durable]", got)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestSyncIntervalLoopShutsDownCleanly(t *testing.T) {
	dir := t.TempDir()
	opts := testOpts()
	opts.Sync = SyncInterval
	opts.SyncInterval = time.Millisecond

	w, err := Open(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 50; i++ {
		if err := w.Log([]byte("tick")); err != nil {
			t.Fatal(err)
		}
	}
	time.Sleep(10 * time.Millisecond)

	// Close must stop the sync goroutine before closing the file, or the
	// ticker races with the final flush.
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestConcurrentLog(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(dir, testOpts())
	if err != nil {
		t.Fatal(err)
	}

	const (
		writers = 8
		perW    = 100
	)
	var wg sync.WaitGroup
	for g := 0; g < writers; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < perW; i++ {
				rec := []byte(fmt.Sprintf("w%02d-r%04d-%s", g, i, bytes.Repeat([]byte("z"), i%500)))
				if err := w.Log(rec); err != nil {
					t.Errorf("Log: %v", err)
					return
				}
			}
		}(g)
	}
	wg.Wait()
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	got, err := readAll(t, dir)
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	if len(got) != writers*perW {
		t.Fatalf("replayed %d records, want %d", len(got), writers*perW)
	}
	// Interleaving is fine; corruption is not. Every record must be intact.
	seen := make(map[string]bool, len(got))
	for _, r := range got {
		seen[string(r)] = true
	}
	for g := 0; g < writers; g++ {
		for i := 0; i < perW; i++ {
			want := fmt.Sprintf("w%02d-r%04d-%s", g, i, bytes.Repeat([]byte("z"), i%500))
			if !seen[want] {
				t.Fatalf("record from writer %d index %d is missing or damaged", g, i)
			}
		}
	}
}

// FuzzWALRecovery writes a fuzzer-chosen sequence of records, truncates the
// log at a fuzzer-chosen point, and requires replay to yield a clean prefix.
func FuzzWALRecovery(f *testing.F) {
	f.Add([]byte{3, 10, 200, 5}, uint32(100))
	f.Add([]byte{1, 255}, uint32(0))
	f.Add([]byte{}, uint32(7))

	f.Fuzz(func(t *testing.T, sizes []byte, truncAt uint32) {
		if len(sizes) > 64 {
			sizes = sizes[:64]
		}

		// Interpret each byte as a record length, scaled to reach across page
		// boundaries without producing enormous logs.
		var recs [][]byte
		for i, s := range sizes {
			n := int(s) * 300
			rec := make([]byte, n)
			for j := range rec {
				rec[j] = byte(i)
			}
			recs = append(recs, rec)
		}

		dir := t.TempDir()
		opts := Options{SegmentSize: 1024 * PageSize, Sync: SyncNever}
		w, err := Open(dir, opts)
		if err != nil {
			t.Fatal(err)
		}
		for _, r := range recs {
			if err := w.Log(r); err != nil {
				t.Fatalf("Log: %v", err)
			}
		}
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}

		full, err := os.ReadFile(SegmentName(dir, 0))
		if err != nil {
			t.Fatal(err)
		}
		cut := int(truncAt) % (len(full) + 1)

		truncDir := t.TempDir()
		if err := os.WriteFile(SegmentName(truncDir, 0), full[:cut], 0o666); err != nil {
			t.Fatal(err)
		}

		got, err := readAll(t, truncDir)
		if err != nil {
			t.Fatalf("truncated at %d of %d: %v", cut, len(full), err)
		}
		if len(got) > len(recs) {
			t.Fatalf("truncated at %d: got %d records, want at most %d", cut, len(got), len(recs))
		}
		for i := range got {
			if !bytes.Equal(got[i], recs[i]) {
				t.Fatalf("truncated at %d: record %d differs (%d vs %d bytes)",
					cut, i, len(got[i]), len(recs[i]))
			}
		}
	})
}

func BenchmarkLog(b *testing.B) {
	for _, size := range []int{64, 512, 4096} {
		b.Run(fmt.Sprintf("%dB", size), func(b *testing.B) {
			dir := b.TempDir()
			w, err := Open(dir, Options{Sync: SyncNever})
			if err != nil {
				b.Fatal(err)
			}
			defer w.Close()

			rec := make([]byte, size)
			b.SetBytes(int64(size))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := w.Log(rec); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkLogBatched(b *testing.B) {
	dir := b.TempDir()
	w, err := Open(dir, Options{Sync: SyncNever})
	if err != nil {
		b.Fatal(err)
	}
	defer w.Close()

	// One flush per batch instead of per record: the shape ingest actually
	// uses, since samples arrive in scrape-sized groups.
	batch := make([][]byte, 32)
	for i := range batch {
		batch[i] = make([]byte, 128)
	}
	b.SetBytes(int64(32 * 128))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := w.Log(batch...); err != nil {
			b.Fatal(err)
		}
	}
}
