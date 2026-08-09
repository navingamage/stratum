package chunk

import (
	"encoding/binary"
	"errors"
	"math"
	"math/rand"
	"testing"
)

type sample struct {
	t int64
	v float64
}

// appendAll fills a fresh chunk and fails the test on any append error.
func appendAll(t *testing.T, samples []sample) *XORChunk {
	t.Helper()
	c := NewXORChunk()
	app, err := c.Appender()
	if err != nil {
		t.Fatalf("Appender: %v", err)
	}
	for i, s := range samples {
		if err := app.Append(s.t, s.v); err != nil {
			t.Fatalf("Append(%d): %v", i, err)
		}
	}
	return c
}

// collect drains an iterator into a slice.
func collect(t *testing.T, c Chunk) []sample {
	t.Helper()
	var got []sample
	it := c.Iterator(nil)
	for it.Next() {
		ts, v := it.At()
		got = append(got, sample{ts, v})
	}
	if err := it.Err(); err != nil {
		t.Fatalf("iterator: %v", err)
	}
	return got
}

// equalSamples compares bit-exactly so that NaN payloads and signed zero are
// held to the same standard as ordinary values. The encoder is lossless, so
// anything weaker would hide real bugs.
func equalSamples(a, b []sample) (int, bool) {
	if len(a) != len(b) {
		return -1, false
	}
	for i := range a {
		if a[i].t != b[i].t || math.Float64bits(a[i].v) != math.Float64bits(b[i].v) {
			return i, false
		}
	}
	return -1, true
}

func TestXORRoundTripRegularInterval(t *testing.T) {
	// The shape stratum is tuned for: fixed scrape interval, slowly rising
	// counter. Every timestamp after the second costs a single bit.
	var want []sample
	ts := int64(1_754_700_000_000)
	v := 12345.0
	for i := 0; i < 500; i++ {
		want = append(want, sample{ts, v})
		ts += 15_000
		v += 1
	}

	c := appendAll(t, want)
	if got := c.NumSamples(); got != len(want) {
		t.Errorf("NumSamples() = %d, want %d", got, len(want))
	}
	got := collect(t, c)
	if i, ok := equalSamples(want, got); !ok {
		t.Fatalf("mismatch at index %d: got %v, want %v", i, got, want)
	}
}

func TestXORRoundTripJitteredRandomWalk(t *testing.T) {
	// Closer to a real gauge: the interval wobbles and the value moves in
	// small floating-point increments, which exercises the leading/trailing
	// window re-use path heavily.
	rng := rand.New(rand.NewSource(1))
	var want []sample
	ts := int64(1_754_700_000_000)
	v := 0.5
	for i := 0; i < 2000; i++ {
		want = append(want, sample{ts, v})
		ts += 15_000 + int64(rng.Intn(400)) - 200
		v += rng.NormFloat64() * 0.01
	}

	c := appendAll(t, want)
	got := collect(t, c)
	if i, ok := equalSamples(want, got); !ok {
		t.Fatalf("mismatch at index %d", i)
	}
}

func TestXORRoundTripPathologicalValues(t *testing.T) {
	// Values chosen to break the XOR window logic if the sign, exponent or
	// significand boundaries are handled sloppily.
	vals := []float64{
		0, math.Copysign(0, -1), 1, -1,
		math.NaN(), math.Inf(1), math.Inf(-1),
		math.MaxFloat64, -math.MaxFloat64,
		math.SmallestNonzeroFloat64, -math.SmallestNonzeroFloat64,
		math.Float64frombits(0xFFFFFFFFFFFFFFFF), // all bits set
		math.Float64frombits(0x7FF8000000000001), // NaN with a payload
		1e308, 1e-308, 0.1, 1.0 / 3.0,
	}

	var want []sample
	// Timestamps hit each delta-of-delta bucket in turn, including the
	// 64-bit escape.
	deltas := []int64{1, 2, 1 << 13, 1 << 16, 1 << 19, 1 << 40}
	ts := int64(-1_000_000) // negative epochs must work too
	for i, v := range vals {
		want = append(want, sample{ts, v})
		ts += deltas[i%len(deltas)]
	}

	c := appendAll(t, want)
	got := collect(t, c)
	if i, ok := equalSamples(want, got); !ok {
		t.Fatalf("mismatch at index %d: got %#v, want %#v", i, got[i], want[i])
	}
}

func TestXORDeltaOfDeltaBuckets(t *testing.T) {
	// Drive the interval so that each control-prefix bucket is used, then
	// confirm the decoder reconstructs the exact timestamps.
	for _, tc := range []struct {
		name  string
		delta int64
	}{
		{"zero", 0},
		{"14bit", 8000},
		{"17bit", 60000},
		{"20bit", 500000},
		{"64bit", 1 << 45},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var want []sample
			ts := int64(0)
			step := int64(1000)
			for i := 0; i < 64; i++ {
				want = append(want, sample{ts, float64(i)})
				// Alternate the jump direction so negative deltas of delta
				// are exercised as well.
				if i%2 == 0 {
					step += tc.delta
				} else if step > tc.delta {
					step -= tc.delta
				}
				ts += step
			}

			c := appendAll(t, want)
			got := collect(t, c)
			if i, ok := equalSamples(want, got); !ok {
				t.Fatalf("mismatch at index %d", i)
			}
		})
	}
}

func TestXORCompressionRatio(t *testing.T) {
	// These are regression guards, not aspirations: the numbers below are
	// roughly 2x worse than what the encoder actually achieves, so they fail
	// only if a change genuinely breaks the encoding rather than on noise.
	t.Run("constant value, fixed interval", func(t *testing.T) {
		var samples []sample
		ts := int64(1_754_700_000_000)
		for i := 0; i < 1000; i++ {
			samples = append(samples, sample{ts, 42.0})
			ts += 15_000
		}
		c := appendAll(t, samples)

		bps := float64(len(c.Bytes())) / float64(len(samples))
		t.Logf("constant series: %d bytes for %d samples (%.3f bytes/sample, %.1fx vs raw)",
			len(c.Bytes()), len(samples), bps, 16/bps)
		if bps > 0.5 {
			t.Errorf("%.3f bytes/sample, want <= 0.5", bps)
		}
	})

	t.Run("integer-valued gauge", func(t *testing.T) {
		// The typical case in practice. Most real metrics are counts -
		// bytes, requests, open file descriptors - and an integer stored as
		// a float64 has a long run of trailing mantissa zeros. Consecutive
		// values therefore XOR to something narrow, and the encoder can hold
		// one leading/trailing window across many samples.
		//
		// Note that this depends on the values being integers, not on them
		// being "low precision": rounding to two decimals would not help at
		// all, because 1.23 is not representable in binary and its mantissa
		// is as dense as any other.
		rng := rand.New(rand.NewSource(7))
		var samples []sample
		ts := int64(1_754_700_000_000)
		v := int64(4_000_000)
		for i := 0; i < 1000; i++ {
			samples = append(samples, sample{ts, float64(v)})
			ts += 15_000 + int64(rng.Intn(200)) - 100
			v += int64(rng.Intn(2048)) - 1024
		}
		c := appendAll(t, samples)

		bps := float64(len(c.Bytes())) / float64(len(samples))
		t.Logf("integer-valued gauge: %d bytes for %d samples (%.3f bytes/sample, %.1fx vs raw)",
			len(c.Bytes()), len(samples), bps, 16/bps)
		if bps > 5 {
			t.Errorf("%.3f bytes/sample, want <= 5", bps)
		}
	})

	t.Run("full-entropy mantissa (worst case)", func(t *testing.T) {
		// The adversarial case for XOR encoding: every value has an unrelated
		// mantissa, so almost the whole 64 bits is significant and there is
		// nothing to exploit. Worth pinning down, because it is the number
		// that bounds how bad things can get - roughly 2x better than raw,
		// entirely from the timestamp side.
		rng := rand.New(rand.NewSource(7))
		var samples []sample
		ts := int64(1_754_700_000_000)
		v := 100.0
		for i := 0; i < 1000; i++ {
			samples = append(samples, sample{ts, v})
			ts += 15_000 + int64(rng.Intn(200)) - 100
			v += rng.NormFloat64()
		}
		c := appendAll(t, samples)

		bps := float64(len(c.Bytes())) / float64(len(samples))
		t.Logf("full-entropy mantissa: %d bytes for %d samples (%.3f bytes/sample, %.1fx vs raw)",
			len(c.Bytes()), len(samples), bps, 16/bps)
		if bps > 10 {
			t.Errorf("%.3f bytes/sample, want <= 10", bps)
		}
	})
}

func TestXORRejectsOutOfOrder(t *testing.T) {
	c := NewXORChunk()
	app, err := c.Appender()
	if err != nil {
		t.Fatal(err)
	}
	if err := app.Append(1000, 1); err != nil {
		t.Fatal(err)
	}
	for _, ts := range []int64{1000, 999, -5} {
		if err := app.Append(ts, 2); !errors.Is(err, ErrOutOfOrder) {
			t.Errorf("Append(%d) = %v, want ErrOutOfOrder", ts, err)
		}
	}
	if got := c.NumSamples(); got != 1 {
		t.Errorf("NumSamples() = %d, want 1: a rejected append must not count", got)
	}
}

func TestXORSealedChunkRejectsAppend(t *testing.T) {
	c := appendAll(t, []sample{{1, 1}, {2, 2}})

	sealed, err := FromData(EncXOR, c.Bytes())
	if err != nil {
		t.Fatalf("FromData: %v", err)
	}
	if _, err := sealed.Appender(); !errors.Is(err, ErrChunkSealed) {
		t.Errorf("Appender() on sealed chunk = %v, want ErrChunkSealed", err)
	}
	// It must still read correctly.
	if got := collect(t, sealed); len(got) != 2 {
		t.Errorf("sealed chunk yielded %d samples, want 2", len(got))
	}
}

func TestXORAppenderResumesAfterReopen(t *testing.T) {
	// Getting a second Appender must recover the encoder state rather than
	// restarting the deltas, or the chunk decodes to nonsense.
	first := []sample{{1000, 1}, {2000, 2}, {3000, 3}}
	c := appendAll(t, first)

	app, err := c.Appender()
	if err != nil {
		t.Fatalf("second Appender: %v", err)
	}
	rest := []sample{{4000, 4}, {5000, 5}}
	for _, s := range rest {
		if err := app.Append(s.t, s.v); err != nil {
			t.Fatalf("Append: %v", err)
		}
	}

	want := append(append([]sample{}, first...), rest...)
	if i, ok := equalSamples(want, collect(t, c)); !ok {
		t.Fatalf("mismatch at index %d", i)
	}
}

func TestXORAppenderOnEmptyChunk(t *testing.T) {
	c := NewXORChunk()
	if _, err := c.Appender(); err != nil {
		t.Fatalf("Appender on empty chunk: %v", err)
	}
	if got := c.NumSamples(); got != 0 {
		t.Errorf("NumSamples() = %d, want 0", got)
	}
	if got := collect(t, c); len(got) != 0 {
		t.Errorf("empty chunk yielded %d samples", len(got))
	}
}

func TestXORChunkFull(t *testing.T) {
	if testing.Short() {
		t.Skip("fills a 65535-sample chunk")
	}
	c := NewXORChunk()
	app, _ := c.Appender()
	for i := 0; i < MaxSamplesPerChunk; i++ {
		if err := app.Append(int64(i)*1000, float64(i)); err != nil {
			t.Fatalf("Append(%d): %v", i, err)
		}
	}
	if err := app.Append(1<<40, 1); !errors.Is(err, ErrChunkFull) {
		t.Errorf("Append past capacity = %v, want ErrChunkFull", err)
	}
	if got := c.NumSamples(); got != MaxSamplesPerChunk {
		t.Errorf("NumSamples() = %d, want %d", got, MaxSamplesPerChunk)
	}
}

func TestXORSeekTo(t *testing.T) {
	var samples []sample
	for i := 0; i < 100; i++ {
		samples = append(samples, sample{int64(i) * 1000, float64(i)})
	}
	c := appendAll(t, samples)

	t.Run("exact hit", func(t *testing.T) {
		it := c.Iterator(nil)
		if !it.SeekTo(50_000) {
			t.Fatal("SeekTo(50000) = false")
		}
		if ts, v := it.At(); ts != 50_000 || v != 50 {
			t.Errorf("At() = (%d, %v), want (50000, 50)", ts, v)
		}
	})

	t.Run("lands on next sample", func(t *testing.T) {
		it := c.Iterator(nil)
		if !it.SeekTo(50_001) {
			t.Fatal("SeekTo(50001) = false")
		}
		if ts, _ := it.At(); ts != 51_000 {
			t.Errorf("At() timestamp = %d, want 51000", ts)
		}
	})

	t.Run("before start", func(t *testing.T) {
		it := c.Iterator(nil)
		if !it.SeekTo(math.MinInt64) {
			t.Fatal("SeekTo(MinInt64) = false")
		}
		if ts, _ := it.At(); ts != 0 {
			t.Errorf("At() timestamp = %d, want 0", ts)
		}
	})

	t.Run("past end", func(t *testing.T) {
		it := c.Iterator(nil)
		if it.SeekTo(1 << 40) {
			t.Error("SeekTo past the last sample = true, want false")
		}
		if err := it.Err(); err != nil {
			t.Errorf("exhausted iterator reported %v, want nil", err)
		}
	})

	t.Run("does not rewind", func(t *testing.T) {
		it := c.Iterator(nil)
		if !it.SeekTo(50_000) {
			t.Fatal("initial SeekTo failed")
		}
		if !it.SeekTo(10_000) {
			t.Fatal("backwards SeekTo = false")
		}
		if ts, _ := it.At(); ts != 50_000 {
			t.Errorf("backwards SeekTo moved to %d, want to stay at 50000", ts)
		}
	})

	t.Run("continues with Next", func(t *testing.T) {
		it := c.Iterator(nil)
		it.SeekTo(50_000)
		if !it.Next() {
			t.Fatal("Next after SeekTo = false")
		}
		if ts, _ := it.At(); ts != 51_000 {
			t.Errorf("At() = %d, want 51000", ts)
		}
	})
}

func TestXORIteratorIsReused(t *testing.T) {
	c := appendAll(t, []sample{{1, 1}, {2, 2}})

	it := c.Iterator(nil)
	for it.Next() {
	}
	// Passing the exhausted iterator back must reset it in place rather than
	// allocate, and must clear the read position.
	it2 := c.Iterator(it)
	if it2 != it {
		t.Error("Iterator did not reuse the supplied iterator")
	}
	n := 0
	for it2.Next() {
		n++
	}
	if n != 2 {
		t.Errorf("reused iterator yielded %d samples, want 2", n)
	}
}

func TestXORTruncatedDataIsAnError(t *testing.T) {
	c := appendAll(t, []sample{{1000, 1}, {2000, 2}, {3000, 3}, {4000, 4}})
	full := c.Bytes()

	// Chop the body but leave the header claiming four samples. Every prefix
	// must fail cleanly rather than panic or invent data.
	for n := chunkHeaderSize; n < len(full); n++ {
		truncated := append([]byte(nil), full[:n]...)
		ch, err := FromData(EncXOR, truncated)
		if err != nil {
			continue
		}
		it := ch.Iterator(nil)
		count := 0
		for it.Next() {
			count++
		}
		if count == 4 && it.Err() == nil {
			t.Errorf("truncated to %d bytes still decoded all 4 samples", n)
		}
	}
}

func TestFromDataRejectsShortAndUnknown(t *testing.T) {
	if _, err := FromData(EncXOR, []byte{0x00}); err == nil {
		t.Error("FromData with a 1-byte buffer succeeded, want error")
	}
	if _, err := FromData(EncNone, []byte{0, 0}); !errors.Is(err, ErrUnknownEncoding) {
		t.Errorf("FromData(EncNone) = %v, want ErrUnknownEncoding", err)
	}
}

func TestEncodingString(t *testing.T) {
	for _, tc := range []struct {
		enc  Encoding
		want string
	}{{EncNone, "none"}, {EncXOR, "xor"}, {Encoding(9), "unknown(9)"}} {
		if got := tc.enc.String(); got != tc.want {
			t.Errorf("Encoding(%d).String() = %q, want %q", tc.enc, got, tc.want)
		}
	}
	if !EncXOR.Valid() || EncNone.Valid() {
		t.Error("Valid() disagrees with the supported encoding set")
	}
}

func TestPoolRecyclesChunks(t *testing.T) {
	p := NewPool()

	c, err := p.Get(EncXOR)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	app, _ := c.Appender()
	if err := app.Append(1000, 1); err != nil {
		t.Fatal(err)
	}
	p.Put(c)

	// A recycled chunk must come back empty, or samples leak between series.
	c2, err := p.Get(EncXOR)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got := c2.NumSamples(); got != 0 {
		t.Errorf("recycled chunk has %d samples, want 0", got)
	}

	if _, err := p.Get(EncNone); !errors.Is(err, ErrUnknownEncoding) {
		t.Errorf("Get(EncNone) = %v, want ErrUnknownEncoding", err)
	}

	// Sealed chunks alias memory the pool does not own; putting one back must
	// not poison the pool.
	sealed, _ := FromData(EncXOR, c.Bytes())
	p.Put(sealed)
	c3, _ := p.Get(EncXOR)
	if _, err := c3.Appender(); err != nil {
		t.Errorf("pool returned an unusable chunk after Put(sealed): %v", err)
	}
}

// samplesFromFuzz interprets the fuzzer's bytes as a run of strictly
// increasing timestamps paired with arbitrary float bit patterns.
func samplesFromFuzz(data []byte) []sample {
	const recordSize = 12 // 4 bytes of timestamp gap + 8 bytes of value
	var (
		out []sample
		ts  int64
	)
	for len(data) >= recordSize && len(out) < 4096 {
		gap := binary.LittleEndian.Uint32(data[:4])
		bits := binary.LittleEndian.Uint64(data[4:12])
		data = data[recordSize:]

		// Strictly increasing, but with gaps wide enough to reach every
		// delta-of-delta bucket.
		ts += 1 + int64(gap)
		out = append(out, sample{ts, math.Float64frombits(bits)})
	}
	return out
}

// FuzzChunkRoundTrip asserts the encoder is lossless for any value the
// fuzzer can produce, including every NaN payload and denormal.
func FuzzChunkRoundTrip(f *testing.F) {
	seed := func(samples ...sample) []byte {
		var buf []byte
		var prev int64
		for _, s := range samples {
			buf = binary.LittleEndian.AppendUint32(buf, uint32(s.t-prev-1))
			buf = binary.LittleEndian.AppendUint64(buf, math.Float64bits(s.v))
			prev = s.t
		}
		return buf
	}
	f.Add(seed(sample{1, 1}, sample{2, 1}, sample{3, 1}))
	f.Add(seed(sample{1, 0}, sample{15001, math.NaN()}, sample{30001, math.Inf(-1)}))
	f.Add(seed(sample{1, math.MaxFloat64}, sample{1 << 20, math.SmallestNonzeroFloat64}))
	f.Add([]byte{})

	f.Fuzz(func(t *testing.T, data []byte) {
		want := samplesFromFuzz(data)
		if len(want) == 0 {
			return
		}

		c := NewXORChunk()
		app, err := c.Appender()
		if err != nil {
			t.Fatalf("Appender: %v", err)
		}
		for i, s := range want {
			if err := app.Append(s.t, s.v); err != nil {
				t.Fatalf("Append(%d) of %d: %v", i, len(want), err)
			}
		}

		var got []sample
		it := c.Iterator(nil)
		for it.Next() {
			ts, v := it.At()
			got = append(got, sample{ts, v})
		}
		if err := it.Err(); err != nil {
			t.Fatalf("iterate: %v", err)
		}
		if i, ok := equalSamples(want, got); !ok {
			if i < 0 {
				t.Fatalf("got %d samples, want %d", len(got), len(want))
			}
			t.Fatalf("sample %d: got (%d, %#x), want (%d, %#x)",
				i, got[i].t, math.Float64bits(got[i].v), want[i].t, math.Float64bits(want[i].v))
		}
	})
}

// FuzzChunkDecode feeds arbitrary bytes to the decoder. Chunks are
// checksummed before they reach this path, but a checksum only proves the
// bytes are the ones that were written - a bug in the writer, or a
// format-version mistake, still lands here. The decoder must fail, not panic
// and not loop forever.
func FuzzChunkDecode(f *testing.F) {
	c := NewXORChunk()
	app, _ := c.Appender()
	for i := 0; i < 20; i++ {
		_ = app.Append(int64(i)*1000, float64(i)*1.5)
	}
	f.Add(c.Bytes())
	f.Add([]byte{0x00, 0x05})
	f.Add([]byte{0xFF, 0xFF, 0xFF})

	f.Fuzz(func(t *testing.T, data []byte) {
		ch, err := FromData(EncXOR, data)
		if err != nil {
			return
		}
		it := ch.Iterator(nil)
		// The header caps the loop, so a decoder that always reports progress
		// still terminates; anything worse shows up as a timeout.
		for it.Next() {
			_, _ = it.At()
		}
		_ = it.Err()
	})
}

func BenchmarkXORAppend(b *testing.B) {
	c := NewXORChunk()
	app, _ := c.Appender()
	ts := int64(1_754_700_000_000)
	v := 1.0

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if c.NumSamples() >= MaxSamplesPerChunk {
			c.Reset()
			app, _ = c.Appender()
			ts = 1_754_700_000_000
		}
		ts += 15_000
		v += 1
		if err := app.Append(ts, v); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkXORIterate(b *testing.B) {
	rng := rand.New(rand.NewSource(3))
	c := NewXORChunk()
	app, _ := c.Appender()
	ts := int64(1_754_700_000_000)
	v := 100.0
	const n = 120
	for i := 0; i < n; i++ {
		ts += 15_000 + int64(rng.Intn(100))
		v += rng.NormFloat64()
		_ = app.Append(ts, v)
	}
	b.SetBytes(int64(len(c.Bytes())))

	var it Iterator
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		it = c.Iterator(it)
		for it.Next() {
			_, _ = it.At()
		}
	}
}
