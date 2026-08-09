// Package encoding provides the low-level bit and byte serialisation
// primitives shared by the chunk, index and block formats.
package encoding

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

// Bit is a single bit value.
type Bit bool

// Bit constants, named so call sites read as data rather than booleans.
const (
	Zero Bit = false
	One  Bit = true
)

// BitWriter appends individual bits to a growable byte slice, most
// significant bit first. The zero value is ready to use.
//
// Bit-level packing matters here because the Gorilla encoding in
// internal/chunk spends most of its budget on control bits that are one or
// two bits wide; rounding those up to whole bytes would roughly double the
// size of a typical chunk.
type BitWriter struct {
	buf []byte
	// free is the number of unused low-order bits in the final byte of buf.
	// It is kept in [0, 7]; zero means the buffer is byte-aligned, which is
	// also the state of an empty buffer. That invariant is what lets the
	// zero value work without a constructor.
	free uint8
}

// NewBitWriter returns a writer that appends to buf. The buffer is assumed to
// be byte-aligned, which is true for a fresh or previously finished stream.
func NewBitWriter(buf []byte) *BitWriter {
	return &BitWriter{buf: buf}
}

// Bytes returns the underlying buffer. Bits written after the last byte
// boundary are present, with the trailing padding bits set to zero.
func (w *BitWriter) Bytes() []byte { return w.buf }

// Len reports the size of the stream in bytes, including a partial final byte.
func (w *BitWriter) Len() int { return len(w.buf) }

// Reset discards any accumulated bits and reuses the backing array.
func (w *BitWriter) Reset() {
	w.buf = w.buf[:0]
	w.free = 0
}

// WriteBit appends a single bit.
func (w *BitWriter) WriteBit(bit Bit) {
	if w.free == 0 {
		w.buf = append(w.buf, 0)
		w.free = 8
	}
	if bit {
		w.buf[len(w.buf)-1] |= 1 << (w.free - 1)
	}
	w.free--
}

// WriteByte appends eight bits. The byte may straddle a byte boundary in the
// underlying buffer, in which case it is split across two bytes and the
// alignment is preserved.
//
// It never returns an error; the signature matches io.ByteWriter so a
// BitWriter can be dropped into code that expects one.
func (w *BitWriter) WriteByte(b byte) error {
	if w.free == 0 {
		w.buf = append(w.buf, b)
		return nil
	}
	// Split b: the high `free` bits complete the current byte, the rest open
	// a new one. `free` is unchanged because we consumed exactly 8 bits.
	w.buf[len(w.buf)-1] |= b >> (8 - w.free)
	w.buf = append(w.buf, b<<w.free)
	return nil
}

// WriteBits appends the low nbits of u, most significant bit first.
// It panics if nbits is outside [0, 64]; that is a programming error rather
// than a data error, and every call site passes a compile-time constant or a
// value already clamped to a bit count.
func (w *BitWriter) WriteBits(u uint64, nbits int) {
	if nbits < 0 || nbits > 64 {
		panic(fmt.Sprintf("encoding: WriteBits with nbits=%d out of range", nbits))
	}
	if nbits == 0 {
		return
	}
	// Left-align so the bits we care about sit at the top of the word, which
	// makes the byte fast path a simple shift.
	u <<= uint(64 - nbits)
	for nbits >= 8 {
		_ = w.WriteByte(byte(u >> 56))
		u <<= 8
		nbits -= 8
	}
	for nbits > 0 {
		w.WriteBit(u>>63 == 1)
		u <<= 1
		nbits--
	}
}

// WriteUvarint writes x in LEB128 form. The bytes are emitted through
// WriteByte, so this works at any bit alignment - which is the point, since
// the chunk format interleaves varints with one- and two-bit control flags.
func (w *BitWriter) WriteUvarint(x uint64) {
	for x >= 0x80 {
		_ = w.WriteByte(byte(x) | 0x80)
		x >>= 7
	}
	_ = w.WriteByte(byte(x))
}

// WriteVarint writes x zig-zag encoded, so small magnitudes of either sign
// stay in one byte.
func (w *BitWriter) WriteVarint(x int64) {
	ux := uint64(x) << 1
	if x < 0 {
		ux = ^ux
	}
	w.WriteUvarint(ux)
}

// BitReader consumes a stream produced by BitWriter.
type BitReader struct {
	buf []byte
	pos int // index of the next byte to load from buf

	cur   byte  // byte currently being consumed
	valid uint8 // number of unconsumed low-order bits in cur
}

// NewBitReader returns a reader over buf.
func NewBitReader(buf []byte) *BitReader {
	return &BitReader{buf: buf}
}

// Reset points the reader at a new buffer, reusing the receiver. Iterators are
// pooled and re-seeded per query, so avoiding an allocation here is worth the
// extra method.
func (r *BitReader) Reset(buf []byte) {
	r.buf = buf
	r.pos = 0
	r.cur = 0
	r.valid = 0
}

// ReadBit returns the next bit, or io.EOF once the stream is exhausted.
func (r *BitReader) ReadBit() (Bit, error) {
	if r.valid == 0 {
		if r.pos >= len(r.buf) {
			return Zero, io.EOF
		}
		r.cur = r.buf[r.pos]
		r.pos++
		r.valid = 8
	}
	r.valid--
	return Bit((r.cur>>r.valid)&1 == 1), nil
}

// ReadByte returns the next eight bits, which may straddle a byte boundary.
func (r *BitReader) ReadByte() (byte, error) {
	if r.valid == 0 {
		if r.pos >= len(r.buf) {
			return 0, io.EOF
		}
		b := r.buf[r.pos]
		r.pos++
		return b, nil
	}
	// Take the `valid` remaining bits of cur as the high part, then refill
	// from the next byte for the low part. Alignment is unchanged.
	hi := r.cur << (8 - r.valid)
	if r.pos >= len(r.buf) {
		return 0, io.ErrUnexpectedEOF
	}
	r.cur = r.buf[r.pos]
	r.pos++
	return hi | (r.cur >> r.valid), nil
}

// ReadBits returns the next nbits bits, right-aligned in the result.
func (r *BitReader) ReadBits(nbits int) (uint64, error) {
	if nbits < 0 || nbits > 64 {
		return 0, fmt.Errorf("encoding: ReadBits with nbits=%d out of range", nbits)
	}
	var v uint64
	for nbits >= 8 {
		b, err := r.ReadByte()
		if err != nil {
			return 0, err
		}
		v = v<<8 | uint64(b)
		nbits -= 8
	}
	for nbits > 0 {
		bit, err := r.ReadBit()
		if err != nil {
			return 0, err
		}
		v <<= 1
		if bit {
			v |= 1
		}
		nbits--
	}
	return v, nil
}

// ErrVarintOverflow reports a varint whose continuation bits run past 64 bits.
// A well-formed stream never produces one, so in practice this means the
// buffer is corrupt or misaligned.
var ErrVarintOverflow = errors.New("encoding: varint overflows 64 bits")

// ReadUvarint reads a value written by WriteUvarint.
func (r *BitReader) ReadUvarint() (uint64, error) {
	var (
		x uint64
		s uint
	)
	for i := 0; i < binary.MaxVarintLen64; i++ {
		b, err := r.ReadByte()
		if err != nil {
			return 0, err
		}
		if b < 0x80 {
			// The tenth byte contributes only one usable bit.
			if i == binary.MaxVarintLen64-1 && b > 1 {
				return 0, ErrVarintOverflow
			}
			return x | uint64(b)<<s, nil
		}
		x |= uint64(b&0x7f) << s
		s += 7
	}
	return 0, ErrVarintOverflow
}

// ReadVarint reads a value written by WriteVarint.
func (r *BitReader) ReadVarint() (int64, error) {
	ux, err := r.ReadUvarint()
	if err != nil {
		return 0, err
	}
	x := int64(ux >> 1)
	if ux&1 != 0 {
		x = ^x
	}
	return x, nil
}
