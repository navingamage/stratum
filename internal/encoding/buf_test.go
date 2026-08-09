package encoding

import (
	"errors"
	"math"
	"testing"
)

func TestEncbufDecbufRoundTrip(t *testing.T) {
	var e Encbuf
	e.PutByte(0x2A)
	e.PutBE32(0xDEADBEEF)
	e.PutBE64(0x0102030405060708)
	e.PutBE64Float(math.Pi)
	e.PutUvarint64(math.MaxUint64)
	e.PutVarint64(-999999)
	e.PutUvarintStr("cpu.usage")
	e.PutUvarintBytes([]byte{0x00, 0xFF})
	e.PutUvarintStr("") // empty strings are common for absent labels

	d := NewDecbuf(e.Get())
	if got := d.Byte(); got != 0x2A {
		t.Errorf("Byte() = %#x, want 0x2a", got)
	}
	if got := d.Be32(); got != 0xDEADBEEF {
		t.Errorf("Be32() = %#x, want 0xdeadbeef", got)
	}
	if got := d.Be64(); got != 0x0102030405060708 {
		t.Errorf("Be64() = %#x, want 0x0102030405060708", got)
	}
	if got := d.Be64Float(); got != math.Pi {
		t.Errorf("Be64Float() = %v, want %v", got, math.Pi)
	}
	if got := d.Uvarint64(); got != math.MaxUint64 {
		t.Errorf("Uvarint64() = %d, want MaxUint64", got)
	}
	if got := d.Varint64(); got != -999999 {
		t.Errorf("Varint64() = %d, want -999999", got)
	}
	if got := d.UvarintStr(); got != "cpu.usage" {
		t.Errorf("UvarintStr() = %q, want %q", got, "cpu.usage")
	}
	if got := d.UvarintBytes(); len(got) != 2 || got[0] != 0x00 || got[1] != 0xFF {
		t.Errorf("UvarintBytes() = %v, want [0 255]", got)
	}
	if got := d.UvarintStr(); got != "" {
		t.Errorf("UvarintStr() = %q, want empty", got)
	}
	if err := d.Err(); err != nil {
		t.Fatalf("Err() = %v", err)
	}
	if d.Len() != 0 {
		t.Errorf("%d bytes left unread", d.Len())
	}
}

func TestDecbufErrorIsSticky(t *testing.T) {
	// Two bytes where eight are needed: the first read fails, and every read
	// after it must be a no-op so a parser can check Err once at the end.
	d := NewDecbuf([]byte{0x01, 0x02})
	_ = d.Be64()
	if !errors.Is(d.Err(), ErrInvalidSize) {
		t.Fatalf("Err() = %v, want ErrInvalidSize", d.Err())
	}

	before := d.Len()
	if got := d.Be32(); got != 0 {
		t.Errorf("Be32() after error = %d, want 0", got)
	}
	if got := d.Uvarint64(); got != 0 {
		t.Errorf("Uvarint64() after error = %d, want 0", got)
	}
	if got := d.UvarintStr(); got != "" {
		t.Errorf("UvarintStr() after error = %q, want empty", got)
	}
	d.Skip(1)
	if d.Len() != before {
		t.Errorf("buffer advanced past a sticky error: %d -> %d", before, d.Len())
	}
	if !errors.Is(d.Err(), ErrInvalidSize) {
		t.Errorf("Err() changed to %v", d.Err())
	}
}

func TestDecbufTruncatedVarint(t *testing.T) {
	// 0x80 sets the continuation bit with no following byte.
	d := NewDecbuf([]byte{0x80})
	d.Uvarint64()
	if !errors.Is(d.Err(), ErrInvalidSize) {
		t.Errorf("Err() = %v, want ErrInvalidSize", d.Err())
	}
}

// TestDecbufRejectsOversizedLength covers the bug a fuzzer found in the WAL
// record decoder, pinned here at the layer it actually lived in.
//
// A uvarint too large for an int converts to a negative number. The obvious
// bounds check, `len(buf) < n`, is false for a negative n, so the length passes
// validation and the slice expression underneath panics - a corrupt record
// taking the process down instead of being rejected.
func TestDecbufRejectsOversizedLength(t *testing.T) {
	// A uvarint whose value exceeds MaxInt.
	huge := []byte{0xC6, 0xC6, 0xC6, 0xC6, 0xC6, 0xC6, 0xC6, 0xC6, 0xD3, 0x01}

	t.Run("Uvarint", func(t *testing.T) {
		d := NewDecbuf(huge)
		if got := d.Uvarint(); got != 0 {
			t.Errorf("Uvarint() = %d, want 0", got)
		}
		if !errors.Is(d.Err(), ErrInvalidSize) {
			t.Errorf("Err() = %v, want ErrInvalidSize", d.Err())
		}
	})

	t.Run("UvarintBytes", func(t *testing.T) {
		d := NewDecbuf(append(append([]byte(nil), huge...), 'p', 'a', 'y'))
		if got := d.UvarintBytes(); got != nil {
			t.Errorf("UvarintBytes() = %v, want nil", got)
		}
		if !errors.Is(d.Err(), ErrInvalidSize) {
			t.Errorf("Err() = %v, want ErrInvalidSize", d.Err())
		}
	})

	t.Run("UvarintStr", func(t *testing.T) {
		d := NewDecbuf(append(append([]byte(nil), huge...), 'p', 'a', 'y'))
		if got := d.UvarintStr(); got != "" {
			t.Errorf("UvarintStr() = %q, want empty", got)
		}
		if !errors.Is(d.Err(), ErrInvalidSize) {
			t.Errorf("Err() = %v, want ErrInvalidSize", d.Err())
		}
	})

	// Uvarint64 still reports the raw value: only the int-sized accessor,
	// whose result feeds slice bounds, needs the range check.
	t.Run("Uvarint64 is unaffected", func(t *testing.T) {
		d := NewDecbuf(huge)
		if got := d.Uvarint64(); got == 0 {
			t.Error("Uvarint64() = 0; the raw value should still decode")
		}
		if d.Err() != nil {
			t.Errorf("Err() = %v, want nil", d.Err())
		}
	})
}

func TestDecbufUvarintBytesLengthOverrun(t *testing.T) {
	// Claims 10 bytes of payload but only supplies 2.
	d := NewDecbuf([]byte{10, 0x01, 0x02})
	if got := d.UvarintBytes(); got != nil {
		t.Errorf("UvarintBytes() = %v, want nil", got)
	}
	if !errors.Is(d.Err(), ErrInvalidSize) {
		t.Errorf("Err() = %v, want ErrInvalidSize", d.Err())
	}
}

func TestCheckCrc32(t *testing.T) {
	var e Encbuf
	e.PutUvarintStr("payload")
	e.PutBE32(7)
	e.PutHash(NewCRC32())

	body := len(e.Get()) - 4

	d := NewDecbuf(e.Get())
	d.CheckCrc32(Castagnoli)
	if err := d.Err(); err != nil {
		t.Fatalf("CheckCrc32 on valid data: %v", err)
	}
	if d.Len() != body {
		t.Errorf("after CheckCrc32, Len() = %d, want %d (checksum not trimmed)", d.Len(), body)
	}
	if got := d.UvarintStr(); got != "payload" {
		t.Errorf("payload = %q", got)
	}
}

func TestCheckCrc32DetectsCorruption(t *testing.T) {
	var e Encbuf
	e.PutUvarintStr("payload")
	e.PutHash(NewCRC32())

	// Flip one bit in the body. This is the failure mode that actually shows
	// up in practice: a torn or bit-rotted page, not a wholesale truncation.
	corrupt := append([]byte(nil), e.Get()...)
	corrupt[1] ^= 0x01

	d := NewDecbuf(corrupt)
	d.CheckCrc32(Castagnoli)
	if !errors.Is(d.Err(), ErrInvalidChecksum) {
		t.Errorf("Err() = %v, want ErrInvalidChecksum", d.Err())
	}
}

func TestCheckCrc32TooShort(t *testing.T) {
	d := NewDecbuf([]byte{0x01, 0x02})
	d.CheckCrc32(Castagnoli)
	if !errors.Is(d.Err(), ErrInvalidSize) {
		t.Errorf("Err() = %v, want ErrInvalidSize", d.Err())
	}
}

func TestEncbufResetKeepsCapacity(t *testing.T) {
	var e Encbuf
	e.PutBytes(make([]byte, 128))
	capBefore := cap(e.B)

	e.Reset()
	if e.Len() != 0 {
		t.Errorf("Len() after Reset = %d, want 0", e.Len())
	}
	if cap(e.B) != capBefore {
		t.Errorf("capacity dropped from %d to %d", capBefore, cap(e.B))
	}
}

func BenchmarkEncbufPutUvarintStr(b *testing.B) {
	var e Encbuf
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if e.Len() > 1<<20 {
			e.Reset()
		}
		e.PutUvarintStr("node_cpu_seconds_total")
	}
}

func BenchmarkDecbufUvarintStr(b *testing.B) {
	var e Encbuf
	for i := 0; i < 1024; i++ {
		e.PutUvarintStr("node_cpu_seconds_total")
	}
	buf := e.Get()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		d := NewDecbuf(buf)
		for d.Len() > 0 {
			_ = d.UvarintStr()
		}
	}
}
