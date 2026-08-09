package encoding

import (
	"encoding/binary"
	"errors"
	"hash"
	"hash/crc32"
	"math"
	"unsafe"
)

// Castagnoli is the CRC-32C polynomial table. It is used for every on-disk
// checksum in stratum because CRC-32C has hardware support on both amd64 and
// arm64, which keeps checksum verification off the critical path when a block
// is opened.
var Castagnoli = crc32.MakeTable(crc32.Castagnoli)

// Errors returned by Decbuf. They are sticky: once a Decbuf holds an error,
// every subsequent read is a no-op returning a zero value, so a parser can run
// a whole sequence of reads and check Err() once at the end.
var (
	ErrInvalidSize     = errors.New("encoding: invalid size")
	ErrInvalidChecksum = errors.New("encoding: invalid checksum")
)

// NewCRC32 returns a fresh CRC-32C hasher.
func NewCRC32() hash.Hash32 { return crc32.New(Castagnoli) }

// Encbuf is an append-only byte buffer with big-endian and varint helpers.
// Big-endian is deliberate for fixed-width fields: it makes the byte order of
// an index entry the same as its sort order, so a binary search over the
// serialised form does not have to decode.
type Encbuf struct {
	B []byte

	// scratch avoids an allocation per varint write.
	scratch [binary.MaxVarintLen64]byte
}

// Reset truncates the buffer, retaining its capacity.
func (e *Encbuf) Reset() { e.B = e.B[:0] }

// Len reports the number of bytes written.
func (e *Encbuf) Len() int { return len(e.B) }

// Get returns the accumulated bytes.
func (e *Encbuf) Get() []byte { return e.B }

func (e *Encbuf) PutByte(b byte) { e.B = append(e.B, b) }

func (e *Encbuf) PutBytes(b []byte) { e.B = append(e.B, b...) }

func (e *Encbuf) PutString(s string) { e.B = append(e.B, s...) }

func (e *Encbuf) PutBE32(x uint32) { e.B = binary.BigEndian.AppendUint32(e.B, x) }

func (e *Encbuf) PutBE64(x uint64) { e.B = binary.BigEndian.AppendUint64(e.B, x) }

func (e *Encbuf) PutBE64Float(x float64) { e.PutBE64(math.Float64bits(x)) }

func (e *Encbuf) PutUvarint64(x uint64) {
	n := binary.PutUvarint(e.scratch[:], x)
	e.B = append(e.B, e.scratch[:n]...)
}

func (e *Encbuf) PutVarint64(x int64) {
	n := binary.PutVarint(e.scratch[:], x)
	e.B = append(e.B, e.scratch[:n]...)
}

func (e *Encbuf) PutUvarint(x int) { e.PutUvarint64(uint64(x)) }

// PutUvarintBytes writes a length-prefixed byte slice.
func (e *Encbuf) PutUvarintBytes(b []byte) {
	e.PutUvarint(len(b))
	e.PutBytes(b)
}

// PutUvarintStr writes a length-prefixed string.
func (e *Encbuf) PutUvarintStr(s string) {
	e.PutUvarint(len(s))
	e.PutString(s)
}

// PutHash appends the checksum of everything written so far and resets h.
func (e *Encbuf) PutHash(h hash.Hash) {
	h.Reset()
	// hash.Hash writers never return an error.
	_, _ = h.Write(e.B)
	e.B = h.Sum(e.B)
}

// Decbuf reads from a byte slice with bounds checking and a sticky error.
type Decbuf struct {
	B []byte
	E error
}

// NewDecbuf returns a decoder over b.
func NewDecbuf(b []byte) *Decbuf { return &Decbuf{B: b} }

// Err returns the first error encountered, if any.
func (d *Decbuf) Err() error { return d.E }

// Len reports the number of unread bytes.
func (d *Decbuf) Len() int { return len(d.B) }

// Get returns the unread bytes.
func (d *Decbuf) Get() []byte { return d.B }

// Skip advances past n bytes.
func (d *Decbuf) Skip(n int) {
	if d.E != nil {
		return
	}
	if len(d.B) < n {
		d.E = ErrInvalidSize
		return
	}
	d.B = d.B[n:]
}

func (d *Decbuf) Byte() byte {
	if d.E != nil {
		return 0
	}
	if len(d.B) < 1 {
		d.E = ErrInvalidSize
		return 0
	}
	x := d.B[0]
	d.B = d.B[1:]
	return x
}

func (d *Decbuf) Be32() uint32 {
	if d.E != nil {
		return 0
	}
	if len(d.B) < 4 {
		d.E = ErrInvalidSize
		return 0
	}
	x := binary.BigEndian.Uint32(d.B)
	d.B = d.B[4:]
	return x
}

func (d *Decbuf) Be64() uint64 {
	if d.E != nil {
		return 0
	}
	if len(d.B) < 8 {
		d.E = ErrInvalidSize
		return 0
	}
	x := binary.BigEndian.Uint64(d.B)
	d.B = d.B[8:]
	return x
}

func (d *Decbuf) Be64Float() float64 { return math.Float64frombits(d.Be64()) }

func (d *Decbuf) Uvarint64() uint64 {
	if d.E != nil {
		return 0
	}
	x, n := binary.Uvarint(d.B)
	if n < 1 {
		d.E = ErrInvalidSize
		return 0
	}
	d.B = d.B[n:]
	return x
}

func (d *Decbuf) Varint64() int64 {
	if d.E != nil {
		return 0
	}
	x, n := binary.Varint(d.B)
	if n < 1 {
		d.E = ErrInvalidSize
		return 0
	}
	d.B = d.B[n:]
	return x
}

func (d *Decbuf) Uvarint() int { return int(d.Uvarint64()) }

// UvarintBytes returns a length-prefixed slice that aliases the underlying
// buffer. Callers must not retain it beyond the lifetime of that buffer; for
// mmap'd blocks that means the slice is only valid while the block is open.
func (d *Decbuf) UvarintBytes() []byte {
	n := d.Uvarint()
	if d.E != nil {
		return nil
	}
	if len(d.B) < n {
		d.E = ErrInvalidSize
		return nil
	}
	b := d.B[:n]
	d.B = d.B[n:]
	return b
}

// UvarintStr returns a length-prefixed string without copying.
//
// The result aliases the decoder's buffer, so it carries the same lifetime
// caveat as UvarintBytes. Index lookups hit this on every label read, and
// copying each one showed up clearly in profiles of large-cardinality queries.
func (d *Decbuf) UvarintStr() string {
	b := d.UvarintBytes()
	if len(b) == 0 {
		return ""
	}
	return unsafe.String(&b[0], len(b))
}

// CheckCrc32 verifies that the buffer ends with a CRC-32C over its own
// contents, and consumes the whole buffer on success.
func (d *Decbuf) CheckCrc32(table *crc32.Table) {
	if d.E != nil {
		return
	}
	if len(d.B) < 4 {
		d.E = ErrInvalidSize
		return
	}
	body, want := d.B[:len(d.B)-4], binary.BigEndian.Uint32(d.B[len(d.B)-4:])
	if crc32.Checksum(body, table) != want {
		d.E = ErrInvalidChecksum
		return
	}
	d.B = body
}
