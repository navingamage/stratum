package encoding

import (
	"errors"
	"io"
	"math/rand"
	"testing"
)

func TestBitWriterReaderSingleBits(t *testing.T) {
	// A pattern that is deliberately not byte-aligned (13 bits) so the final
	// partial byte is exercised.
	want := []Bit{One, Zero, One, One, Zero, Zero, Zero, One, One, Zero, One, Zero, One}

	var w BitWriter
	for _, b := range want {
		w.WriteBit(b)
	}
	if got := w.Len(); got != 2 {
		t.Fatalf("Len() = %d, want 2 bytes for 13 bits", got)
	}

	r := NewBitReader(w.Bytes())
	for i, wantBit := range want {
		got, err := r.ReadBit()
		if err != nil {
			t.Fatalf("ReadBit(%d): %v", i, err)
		}
		if got != wantBit {
			t.Errorf("bit %d = %v, want %v", i, got, wantBit)
		}
	}
	// The three padding bits read back as zero, then the stream ends.
	for i := 0; i < 3; i++ {
		if _, err := r.ReadBit(); err != nil {
			t.Fatalf("padding bit %d: unexpected error %v", i, err)
		}
	}
	if _, err := r.ReadBit(); !errors.Is(err, io.EOF) {
		t.Errorf("ReadBit() past end = %v, want io.EOF", err)
	}
}

func TestBitWriterUnalignedBytes(t *testing.T) {
	// Write a 3-bit prefix so every subsequent byte straddles a boundary.
	var w BitWriter
	w.WriteBits(0b101, 3)
	payload := []byte{0x00, 0xFF, 0xA5, 0x5A, 0x01, 0x80}
	for _, b := range payload {
		if err := w.WriteByte(b); err != nil {
			t.Fatalf("WriteByte: %v", err)
		}
	}

	r := NewBitReader(w.Bytes())
	if got, err := r.ReadBits(3); err != nil || got != 0b101 {
		t.Fatalf("ReadBits(3) = %d, %v; want 5, nil", got, err)
	}
	for i, want := range payload {
		got, err := r.ReadByte()
		if err != nil {
			t.Fatalf("ReadByte(%d): %v", i, err)
		}
		if got != want {
			t.Errorf("byte %d = %#02x, want %#02x", i, got, want)
		}
	}
}

func TestBitWriterBitsRoundTrip(t *testing.T) {
	// Every width from 1 to 64, with a value that has bits set at both ends
	// of the range so truncation bugs surface.
	for nbits := 1; nbits <= 64; nbits++ {
		var w BitWriter
		var want uint64 = 1
		if nbits > 1 {
			want = 1<<(nbits-1) | 1
		}
		w.WriteBits(want, nbits)

		r := NewBitReader(w.Bytes())
		got, err := r.ReadBits(nbits)
		if err != nil {
			t.Fatalf("nbits=%d: ReadBits: %v", nbits, err)
		}
		if got != want {
			t.Errorf("nbits=%d: got %#x, want %#x", nbits, got, want)
		}
	}
}

func TestBitWriterZeroWidthIsNoOp(t *testing.T) {
	var w BitWriter
	w.WriteBits(0xFFFF, 0)
	if w.Len() != 0 {
		t.Errorf("WriteBits(_, 0) wrote %d bytes, want 0", w.Len())
	}
}

func TestBitWriterPanicsOnBadWidth(t *testing.T) {
	for _, nbits := range []int{-1, 65} {
		t.Run("", func(t *testing.T) {
			defer func() {
				if recover() == nil {
					t.Errorf("WriteBits(_, %d) did not panic", nbits)
				}
			}()
			var w BitWriter
			w.WriteBits(0, nbits)
		})
	}
}

func TestBitReaderRejectsBadWidth(t *testing.T) {
	r := NewBitReader([]byte{0xFF})
	if _, err := r.ReadBits(65); err == nil {
		t.Error("ReadBits(65) succeeded, want error")
	}
}

func TestBitReaderTruncatedByte(t *testing.T) {
	// One bit written, then a byte read: the reader runs out mid-byte.
	var w BitWriter
	w.WriteBit(One)

	r := NewBitReader(w.Bytes())
	if _, err := r.ReadBit(); err != nil {
		t.Fatalf("ReadBit: %v", err)
	}
	// Seven padding bits remain, so a full byte read must straddle into
	// nothing and report an unexpected EOF rather than silently zero-filling.
	if _, err := r.ReadByte(); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Errorf("ReadByte() = %v, want io.ErrUnexpectedEOF", err)
	}
}

func TestBitReaderReset(t *testing.T) {
	var w BitWriter
	w.WriteBits(0xDEAD, 16)

	r := NewBitReader(nil)
	r.Reset(w.Bytes())
	got, err := r.ReadBits(16)
	if err != nil || got != 0xDEAD {
		t.Fatalf("after Reset: got %#x, %v; want 0xdead, nil", got, err)
	}
}

func TestBitWriterReset(t *testing.T) {
	var w BitWriter
	w.WriteBits(0b1, 1) // leave the buffer mid-byte
	w.Reset()
	w.WriteByte(0xAB)

	if w.Len() != 1 {
		t.Fatalf("Len() after Reset = %d, want 1 (alignment not cleared)", w.Len())
	}
	r := NewBitReader(w.Bytes())
	if got, _ := r.ReadByte(); got != 0xAB {
		t.Errorf("got %#02x, want 0xab", got)
	}
}

// TestBitStreamRandomOperations interleaves the three write primitives at
// random widths and replays them, which is the case most likely to expose an
// alignment bug in the straddling paths.
func TestBitStreamRandomOperations(t *testing.T) {
	type op struct {
		kind  int // 0=bit, 1=byte, 2=bits
		val   uint64
		nbits int
	}

	rng := rand.New(rand.NewSource(20260809))
	for iter := 0; iter < 200; iter++ {
		ops := make([]op, rng.Intn(120)+1)
		var w BitWriter
		for i := range ops {
			o := op{kind: rng.Intn(3)}
			switch o.kind {
			case 0:
				o.val = uint64(rng.Intn(2))
				w.WriteBit(o.val == 1)
			case 1:
				o.val = uint64(rng.Intn(256))
				_ = w.WriteByte(byte(o.val))
			case 2:
				o.nbits = rng.Intn(64) + 1
				o.val = rng.Uint64() >> uint(64-o.nbits)
				w.WriteBits(o.val, o.nbits)
			}
			ops[i] = o
		}

		r := NewBitReader(w.Bytes())
		for i, o := range ops {
			var got uint64
			var err error
			switch o.kind {
			case 0:
				var b Bit
				b, err = r.ReadBit()
				if b {
					got = 1
				}
			case 1:
				var b byte
				b, err = r.ReadByte()
				got = uint64(b)
			case 2:
				got, err = r.ReadBits(o.nbits)
			}
			if err != nil {
				t.Fatalf("iter %d op %d (kind %d): %v", iter, i, o.kind, err)
			}
			if got != o.val {
				t.Fatalf("iter %d op %d (kind %d, nbits %d): got %#x, want %#x",
					iter, i, o.kind, o.nbits, got, o.val)
			}
		}
	}
}

func BenchmarkBitWriterWriteBits(b *testing.B) {
	var w BitWriter
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if w.Len() > 1<<20 {
			w.Reset()
		}
		w.WriteBits(uint64(i), 37)
	}
}

func BenchmarkBitReaderReadBits(b *testing.B) {
	var w BitWriter
	for i := 0; i < 1<<16; i++ {
		w.WriteBits(uint64(i), 37)
	}
	buf := w.Bytes()

	r := NewBitReader(buf)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := r.ReadBits(37); err != nil {
			r.Reset(buf)
		}
	}
}
