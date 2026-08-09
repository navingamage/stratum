package wal

import (
	"errors"
	"math"
	"math/rand"
	"reflect"
	"testing"

	"github.com/navingamage/stratum/internal/model"
)

func TestRecordTypeRoundTrip(t *testing.T) {
	var (
		enc Encoder
		dec Decoder
	)

	series := enc.Series([]RefSeries{{Ref: 1, Labels: model.FromStrings("a", "b")}}, nil)
	samples := enc.Samples([]RefSample{{Ref: 1, T: 2, V: 3}}, nil)
	tombs := enc.Tombstones([]Tombstone{{Ref: 1, Min: 2, Max: 3}}, nil)

	for _, tc := range []struct {
		rec  []byte
		want RecordType
	}{
		{series, RecordSeries},
		{samples, RecordSamples},
		{tombs, RecordTombstones},
		{nil, RecordInvalid},
		{[]byte{99}, RecordInvalid},
	} {
		if got := dec.Type(tc.rec); got != tc.want {
			t.Errorf("Type() = %v, want %v", got, tc.want)
		}
	}
}

func TestSeriesRecordRoundTrip(t *testing.T) {
	var (
		enc Encoder
		dec Decoder
	)

	want := []RefSeries{
		{Ref: 0, Labels: model.EmptyLabels()},
		{Ref: 1, Labels: model.FromStrings(model.MetricName, "cpu", "host", "web-1")},
		{Ref: math.MaxUint32, Labels: model.FromStrings("k", "")},
		{Ref: 1 << 40, Labels: model.FromStrings("unicode", "héllo ☃", "empty", "")},
	}

	got, err := dec.Series(enc.Series(want, nil), nil)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(got) != len(want) {
		t.Fatalf("got %d series, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i].Ref != want[i].Ref {
			t.Errorf("series %d: ref = %d, want %d", i, got[i].Ref, want[i].Ref)
		}
		if !got[i].Labels.Equal(want[i].Labels) {
			t.Errorf("series %d: labels = %s, want %s", i, got[i].Labels, want[i].Labels)
		}
	}
}

func TestSamplesRecordRoundTrip(t *testing.T) {
	var (
		enc Encoder
		dec Decoder
	)

	want := []RefSample{
		{Ref: 100, T: 1_754_700_000_000, V: 1.5},
		{Ref: 101, T: 1_754_700_000_000, V: math.NaN()},
		{Ref: 99, T: 1_754_699_999_000, V: math.Inf(-1)}, // ref and time both go backwards
		{Ref: 100, T: 1_754_700_015_000, V: math.MaxFloat64},
		{Ref: 1 << 50, T: -5, V: math.Copysign(0, -1)},
	}

	got, err := dec.Samples(enc.Samples(want, nil), nil)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(got) != len(want) {
		t.Fatalf("got %d samples, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i].Ref != want[i].Ref || got[i].T != want[i].T {
			t.Errorf("sample %d: got (ref %d, t %d), want (ref %d, t %d)",
				i, got[i].Ref, got[i].T, want[i].Ref, want[i].T)
		}
		// Bit comparison: the WAL is lossless, so NaN payloads and signed
		// zero have to survive exactly.
		if math.Float64bits(got[i].V) != math.Float64bits(want[i].V) {
			t.Errorf("sample %d: value bits %#x, want %#x",
				i, math.Float64bits(got[i].V), math.Float64bits(want[i].V))
		}
	}
}

func TestTombstonesRecordRoundTrip(t *testing.T) {
	var (
		enc Encoder
		dec Decoder
	)
	want := []Tombstone{
		{Ref: 1, Min: 0, Max: 100},
		{Ref: 2, Min: -50, Max: 1 << 40},
	}
	got, err := dec.Tombstones(enc.Tombstones(want, nil), nil)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
}

func TestEmptyBatchesRoundTrip(t *testing.T) {
	var (
		enc Encoder
		dec Decoder
	)

	got, err := dec.Samples(enc.Samples(nil, nil), nil)
	if err != nil {
		t.Errorf("decoding an empty samples record: %v", err)
	}
	if len(got) != 0 {
		t.Errorf("empty samples record yielded %d samples", len(got))
	}

	gotS, err := dec.Series(enc.Series(nil, nil), nil)
	if err != nil {
		t.Errorf("decoding an empty series record: %v", err)
	}
	if len(gotS) != 0 {
		t.Errorf("empty series record yielded %d series", len(gotS))
	}
}

func TestDecoderRejectsWrongType(t *testing.T) {
	var (
		enc Encoder
		dec Decoder
	)
	samples := enc.Samples([]RefSample{{Ref: 1, T: 2, V: 3}}, nil)

	if _, err := dec.Series(samples, nil); !errors.Is(err, ErrMalformedRecord) {
		t.Errorf("Series() on a samples record = %v, want ErrMalformedRecord", err)
	}
	if _, err := dec.Tombstones(samples, nil); !errors.Is(err, ErrMalformedRecord) {
		t.Errorf("Tombstones() on a samples record = %v, want ErrMalformedRecord", err)
	}
}

func TestDecoderRejectsTruncated(t *testing.T) {
	var (
		enc Encoder
		dec Decoder
	)

	wantSeries := []RefSeries{
		{Ref: 1, Labels: model.FromStrings("a", "b", "c", "d")},
		{Ref: 2, Labels: model.FromStrings("e", "f")},
	}
	wantSamples := []RefSample{{Ref: 1, T: 2, V: 3}, {Ref: 2, T: 3, V: 4}}

	series := enc.Series(wantSeries, nil)
	samples := enc.Samples(wantSamples, nil)

	// Some prefixes land on an entry boundary and legitimately decode to a
	// shorter batch - a 17-byte samples prefix is exactly the batch header,
	// which is a valid empty batch. The property that matters is that a
	// prefix either errors or decodes to a genuine prefix of the original.
	// Anything else means the decoder invented data.
	for n := 1; n <= len(series); n++ {
		got, err := dec.Series(series[:n], nil)
		if err != nil {
			continue
		}
		if len(got) > len(wantSeries) {
			t.Fatalf("Series() on a %d-byte prefix produced %d series, more than the %d encoded",
				n, len(got), len(wantSeries))
		}
		for i := range got {
			if got[i].Ref != wantSeries[i].Ref || !got[i].Labels.Equal(wantSeries[i].Labels) {
				t.Fatalf("Series() on a %d-byte prefix: entry %d is %v, want %v",
					n, i, got[i], wantSeries[i])
			}
		}
	}

	for n := 1; n <= len(samples); n++ {
		got, err := dec.Samples(samples[:n], nil)
		if err != nil {
			continue
		}
		if len(got) > len(wantSamples) {
			t.Fatalf("Samples() on a %d-byte prefix produced %d samples, more than the %d encoded",
				n, len(got), len(wantSamples))
		}
		for i := range got {
			if got[i] != wantSamples[i] {
				t.Fatalf("Samples() on a %d-byte prefix: entry %d is %v, want %v",
					n, i, got[i], wantSamples[i])
			}
		}
	}
}

// TestDecoderRejectsAbsurdLabelCount covers the allocation guard: a corrupt
// label count must not become a huge make() before anything has been read.
func TestDecoderRejectsAbsurdLabelCount(t *testing.T) {
	var dec Decoder
	// Type byte, an 8-byte ref, then a uvarint claiming a huge label count.
	rec := []byte{byte(RecordSeries), 0, 0, 0, 0, 0, 0, 0, 1, 0xFF, 0xFF, 0xFF, 0x7F}
	if _, err := dec.Series(rec, nil); !errors.Is(err, ErrMalformedRecord) {
		t.Errorf("Series() with an absurd label count = %v, want ErrMalformedRecord", err)
	}
}

// TestSamplesRecordIsCompact checks the delta encoding earns its place: a
// scrape-shaped batch (consecutive refs, one timestamp) should approach the
// 8 bytes of the float value plus about two bytes of overhead.
func TestSamplesRecordIsCompact(t *testing.T) {
	var enc Encoder

	const n = 1000
	samples := make([]RefSample, 0, n)
	for i := 0; i < n; i++ {
		samples = append(samples, RefSample{
			Ref: model.SeriesRef(i),
			T:   1_754_700_000_000,
			V:   float64(i),
		})
	}

	rec := enc.Samples(samples, nil)
	perSample := float64(len(rec)) / n
	t.Logf("%d bytes for %d samples (%.2f bytes/sample)", len(rec), n, perSample)

	// 8 for the value + 2 for the ref delta + 1 for the zero time delta.
	if perSample > 11.5 {
		t.Errorf("%.2f bytes/sample, want <= 11.5", perSample)
	}
}

// TestRecordsThroughTheLog joins the two layers: encoded records must survive
// a real write and replay unchanged.
func TestRecordsThroughTheLog(t *testing.T) {
	var (
		enc Encoder
		dec Decoder
	)

	series := []RefSeries{
		{Ref: 1, Labels: model.FromStrings(model.MetricName, "cpu", "host", "web-1")},
		{Ref: 2, Labels: model.FromStrings(model.MetricName, "cpu", "host", "web-2")},
	}
	samples := []RefSample{
		{Ref: 1, T: 1000, V: 0.5},
		{Ref: 2, T: 1000, V: 0.7},
		{Ref: 1, T: 2000, V: 0.6},
	}

	dir := writeRecords(t, testOpts(),
		enc.Series(series, nil),
		enc.Samples(samples, nil),
	)

	recs, err := readAll(t, dir)
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	if len(recs) != 2 {
		t.Fatalf("replayed %d records, want 2", len(recs))
	}

	gotSeries, err := dec.Series(recs[0], nil)
	if err != nil {
		t.Fatalf("decode series: %v", err)
	}
	if len(gotSeries) != 2 || !gotSeries[0].Labels.Equal(series[0].Labels) {
		t.Errorf("series did not survive the round trip: %v", gotSeries)
	}

	gotSamples, err := dec.Samples(recs[1], nil)
	if err != nil {
		t.Fatalf("decode samples: %v", err)
	}
	if !reflect.DeepEqual(gotSamples, samples) {
		t.Errorf("samples: got %v, want %v", gotSamples, samples)
	}
}

// FuzzRecordDecode feeds arbitrary bytes to the payload decoders. Framing
// checksums mean these bytes are what was written, so this is guarding
// against a format-version mismatch or a writer bug rather than storage
// damage - either way it must not panic.
func FuzzRecordDecode(f *testing.F) {
	var enc Encoder
	f.Add(enc.Series([]RefSeries{{Ref: 1, Labels: model.FromStrings("a", "b")}}, nil))
	f.Add(enc.Samples([]RefSample{{Ref: 1, T: 2, V: 3}}, nil))
	f.Add(enc.Tombstones([]Tombstone{{Ref: 1, Min: 2, Max: 3}}, nil))
	f.Add([]byte{1})
	f.Add([]byte{2})
	f.Add([]byte{})

	f.Fuzz(func(t *testing.T, rec []byte) {
		var dec Decoder
		switch dec.Type(rec) {
		case RecordSeries:
			_, _ = dec.Series(rec, nil)
		case RecordSamples:
			_, _ = dec.Samples(rec, nil)
		case RecordTombstones:
			_, _ = dec.Tombstones(rec, nil)
		}
	})
}

func BenchmarkEncodeSamples(b *testing.B) {
	var enc Encoder
	samples := make([]RefSample, 128)
	rng := rand.New(rand.NewSource(1))
	for i := range samples {
		samples[i] = RefSample{
			Ref: model.SeriesRef(i),
			T:   1_754_700_000_000,
			V:   rng.Float64(),
		}
	}

	buf := make([]byte, 0, 4096)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf = enc.Samples(samples, buf[:0])
	}
}

func BenchmarkDecodeSamples(b *testing.B) {
	var (
		enc Encoder
		dec Decoder
	)
	samples := make([]RefSample, 128)
	for i := range samples {
		samples[i] = RefSample{Ref: model.SeriesRef(i), T: 1_754_700_000_000, V: float64(i)}
	}
	rec := enc.Samples(samples, nil)

	into := make([]RefSample, 0, 128)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var err error
		into, err = dec.Samples(rec, into[:0])
		if err != nil {
			b.Fatal(err)
		}
	}
}
