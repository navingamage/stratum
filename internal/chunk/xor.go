package chunk

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"math/bits"

	"github.com/navingamage/stratum/internal/encoding"
)

// chunkHeaderSize is the two-byte big-endian sample count that prefixes every
// XOR chunk. It is kept outside the bit stream so NumSamples is a plain load
// rather than a decode, which matters because compaction planning calls it
// for every chunk in a block.
const chunkHeaderSize = 2

// noWindow marks "no previous leading-zero window established". 0xff is safe
// as a sentinel because a real leading count is clamped to 31 by the 5-bit
// field that stores it.
const noWindow uint8 = 0xff

// XORChunk stores samples using Gorilla delta-of-delta timestamps and XOR'd
// float values.
//
// Layout:
//
//	uint16be              sample count
//	varint                first timestamp
//	64 bits               first value, raw
//	uvarint               second timestamp, as a delta from the first
//	<value>               second value
//	(<dod> <value>)*      remaining samples
//
// Everything after the header is bit-packed, so the stream is only
// byte-aligned by coincidence.
type XORChunk struct {
	w *encoding.BitWriter

	// sealed marks a chunk loaded from persisted bytes. See ErrChunkSealed.
	sealed bool
}

// NewXORChunk returns an empty, appendable chunk.
func NewXORChunk() *XORChunk {
	c := &XORChunk{w: encoding.NewBitWriter(make([]byte, 0, 128))}
	c.writeHeader()
	return c
}

// xorChunkFromData wraps persisted bytes for reading.
func xorChunkFromData(data []byte) (*XORChunk, error) {
	if len(data) < chunkHeaderSize {
		return nil, fmt.Errorf("chunk: xor chunk of %d bytes is shorter than its header", len(data))
	}
	return &XORChunk{w: encoding.NewBitWriter(data), sealed: true}, nil
}

func (c *XORChunk) writeHeader() {
	c.w.WriteBits(0, 8*chunkHeaderSize)
}

// Reset empties the chunk for reuse, keeping its backing array.
func (c *XORChunk) Reset() {
	c.w.Reset()
	c.sealed = false
	c.writeHeader()
}

// Bytes returns the serialised chunk, aliasing internal storage.
func (c *XORChunk) Bytes() []byte { return c.w.Bytes() }

// Encoding reports the chunk format.
func (c *XORChunk) Encoding() Encoding { return EncXOR }

// NumSamples reports the number of samples in the chunk.
func (c *XORChunk) NumSamples() int {
	b := c.w.Bytes()
	if len(b) < chunkHeaderSize {
		return 0
	}
	return int(binary.BigEndian.Uint16(b))
}

func (c *XORChunk) setNumSamples(n int) {
	binary.BigEndian.PutUint16(c.w.Bytes()[:chunkHeaderSize], uint16(n))
}

// Appender returns an appender positioned at the end of the chunk.
//
// Reopening a partially written chunk requires the encoder state, which is
// not serialised, so it is recovered by decoding the chunk once. That is O(n)
// in the chunk's sample count but happens at most once per chunk per process
// lifetime, against an Append path that runs per sample.
func (c *XORChunk) Appender() (Appender, error) {
	if c.sealed {
		return nil, ErrChunkSealed
	}

	it := c.iterator(nil)
	for it.Next() {
	}
	if err := it.Err(); err != nil {
		return nil, fmt.Errorf("chunk: recovering appender state: %w", err)
	}

	a := &xorAppender{
		c:        c,
		t:        it.t,
		v:        it.v,
		tDelta:   it.tDelta,
		leading:  it.leading,
		trailing: it.trailing,
	}
	if c.NumSamples() == 0 {
		a.leading = noWindow
	}
	return a, nil
}

// Iterator returns an iterator over the chunk, reusing it when possible.
func (c *XORChunk) Iterator(it Iterator) Iterator { return c.iterator(it) }

func (c *XORChunk) iterator(it Iterator) *xorIterator {
	if xit, ok := it.(*xorIterator); ok && xit != nil {
		xit.reset(c.w.Bytes())
		return xit
	}
	xit := &xorIterator{}
	xit.reset(c.w.Bytes())
	return xit
}

type xorAppender struct {
	c *XORChunk

	t      int64
	v      float64
	tDelta uint64

	// leading and trailing describe the zero window of the previous XOR, so a
	// value that stays in the same window can skip re-sending its bounds.
	leading  uint8
	trailing uint8
}

// bitRange reports whether x fits in nbits of two's complement, excluding the
// most negative value so that the range stays symmetric. Giving up one
// encodable value keeps the bucket boundaries easy to reason about and costs
// nothing measurable.
func bitRange(x int64, nbits uint8) bool {
	limit := int64(1)<<(nbits-1) - 1
	return -limit <= x && x <= limit
}

func (a *xorAppender) Append(t int64, v float64) error {
	num := a.c.NumSamples()
	if num >= MaxSamplesPerChunk {
		return ErrChunkFull
	}
	if num > 0 && t <= a.t {
		return fmt.Errorf("%w: %d follows %d", ErrOutOfOrder, t, a.t)
	}

	var tDelta uint64
	w := a.c.w

	switch num {
	case 0:
		// Nothing to delta against. Timestamps are epoch milliseconds, so a
		// zig-zag varint of the raw value is ~6 bytes and beats a fixed 8.
		w.WriteVarint(t)
		w.WriteBits(math.Float64bits(v), 64)

	case 1:
		tDelta = uint64(t - a.t)
		w.WriteUvarint(tDelta)
		a.writeVDelta(v)

	default:
		tDelta = uint64(t - a.t)
		// Unsigned subtraction then a signed reinterpretation: the wrap is
		// exactly the two's complement difference we want, and both deltas
		// are far below 2^63 for any real timestamp.
		dod := int64(tDelta - a.tDelta)

		// Bucket widths of 14/17/20/64 rather than the paper's 7/9/12/32.
		// Gorilla assumed a 4-minute-ish window of 64-second-resolution
		// points; stratum targets sub-second scrape intervals in
		// milliseconds, where a one-second jitter is already ±1000 and
		// overflows a 7-bit bucket on nearly every sample. 14 bits covers
		// ±8s of jitter, which absorbs essentially all real scrape drift in
		// two control bits.
		switch {
		case dod == 0:
			w.WriteBit(encoding.Zero)
		case bitRange(dod, 14):
			w.WriteBits(0b10, 2)
			w.WriteBits(uint64(dod), 14)
		case bitRange(dod, 17):
			w.WriteBits(0b110, 3)
			w.WriteBits(uint64(dod), 17)
		case bitRange(dod, 20):
			w.WriteBits(0b1110, 4)
			w.WriteBits(uint64(dod), 20)
		default:
			w.WriteBits(0b1111, 4)
			w.WriteBits(uint64(dod), 64)
		}
		a.writeVDelta(v)
	}

	a.t, a.v, a.tDelta = t, v, tDelta
	a.c.setNumSamples(num + 1)
	return nil
}

func (a *xorAppender) writeVDelta(v float64) {
	w := a.c.w
	xor := math.Float64bits(v) ^ math.Float64bits(a.v)

	// The common case for a gauge that has not moved, and for any counter
	// sampled faster than it increments.
	if xor == 0 {
		w.WriteBit(encoding.Zero)
		return
	}
	w.WriteBit(encoding.One)

	leading := uint8(bits.LeadingZeros64(xor))
	trailing := uint8(bits.TrailingZeros64(xor))

	// The leading-zero count is stored in 5 bits, so it cannot exceed 31.
	// Clamping loses at most a bit of compression and never correctness,
	// because the significand width is derived from the clamped value.
	if leading >= 32 {
		leading = 31
	}

	if a.leading != noWindow && leading >= a.leading && trailing >= a.trailing {
		// The new XOR fits inside the window we already sent. Re-use it and
		// pay one control bit instead of eleven.
		w.WriteBit(encoding.Zero)
		w.WriteBits(xor>>a.trailing, 64-int(a.leading)-int(a.trailing))
		return
	}

	a.leading, a.trailing = leading, trailing
	w.WriteBit(encoding.One)
	w.WriteBits(uint64(leading), 5)

	// Significand width is in [1, 64] because xor != 0, so 64 is encoded as 0
	// and the 6-bit field stays wide enough.
	sigbits := 64 - leading - trailing
	w.WriteBits(uint64(sigbits), 6)
	w.WriteBits(xor>>trailing, int(sigbits))
}

type xorIterator struct {
	br encoding.BitReader

	numTotal uint16
	numRead  uint16

	t int64
	v float64

	tDelta   uint64
	leading  uint8
	trailing uint8

	err error
}

func (it *xorIterator) reset(b []byte) {
	if len(b) < chunkHeaderSize {
		*it = xorIterator{err: fmt.Errorf("chunk: truncated header of %d bytes", len(b))}
		return
	}
	total := binary.BigEndian.Uint16(b)
	*it = xorIterator{numTotal: total}
	it.br.Reset(b[chunkHeaderSize:])
}

func (it *xorIterator) At() (int64, float64) { return it.t, it.v }

func (it *xorIterator) Err() error { return it.err }

// fail records err and stops the iterator. A clean EOF part-way through is
// upgraded to ErrUnexpectedEOF: the header said more samples were coming, so
// running out of bits means the chunk is truncated, not finished.
func (it *xorIterator) fail(err error) bool {
	if errors.Is(err, io.EOF) {
		err = io.ErrUnexpectedEOF
	}
	it.err = err
	return false
}

func (it *xorIterator) Next() bool {
	if it.err != nil || it.numRead == it.numTotal {
		return false
	}

	switch it.numRead {
	case 0:
		t, err := it.br.ReadVarint()
		if err != nil {
			return it.fail(err)
		}
		v, err := it.br.ReadBits(64)
		if err != nil {
			return it.fail(err)
		}
		it.t, it.v = t, math.Float64frombits(v)
		it.leading = noWindow
		it.numRead++
		return true

	case 1:
		tDelta, err := it.br.ReadUvarint()
		if err != nil {
			return it.fail(err)
		}
		it.tDelta = tDelta
		it.t += int64(it.tDelta)
		return it.readValue()
	}

	// Read the unary-ish control prefix: up to four bits, terminated by the
	// first zero, with 0b1111 meaning "full 64-bit delta-of-delta".
	var prefix byte
	for i := 0; i < 4; i++ {
		prefix <<= 1
		bit, err := it.br.ReadBit()
		if err != nil {
			return it.fail(err)
		}
		if bit == encoding.Zero {
			break
		}
		prefix |= 1
	}

	var (
		width uint8
		dod   int64
	)
	switch prefix {
	case 0b0:
		// dod == 0; nothing further on the wire.
	case 0b10:
		width = 14
	case 0b110:
		width = 17
	case 0b1110:
		width = 20
	case 0b1111:
		raw, err := it.br.ReadBits(64)
		if err != nil {
			return it.fail(err)
		}
		dod = int64(raw)
	default:
		return it.fail(fmt.Errorf("chunk: invalid delta-of-delta prefix %04b", prefix))
	}

	if width > 0 {
		raw, err := it.br.ReadBits(int(width))
		if err != nil {
			return it.fail(err)
		}
		// Sign-extend from width bits.
		if raw >= 1<<(width-1) {
			raw -= 1 << width
		}
		dod = int64(raw)
	}

	it.tDelta += uint64(dod)
	it.t += int64(it.tDelta)
	return it.readValue()
}

func (it *xorIterator) readValue() bool {
	changed, err := it.br.ReadBit()
	if err != nil {
		return it.fail(err)
	}

	if changed == encoding.One {
		newWindow, err := it.br.ReadBit()
		if err != nil {
			return it.fail(err)
		}
		if newWindow == encoding.One {
			lead, err := it.br.ReadBits(5)
			if err != nil {
				return it.fail(err)
			}
			sig, err := it.br.ReadBits(6)
			if err != nil {
				return it.fail(err)
			}
			// 0 encodes a full 64-bit significand; see writeVDelta.
			if sig == 0 {
				sig = 64
			}
			if uint64(lead)+sig > 64 {
				return it.fail(fmt.Errorf("chunk: invalid xor window lead=%d sig=%d", lead, sig))
			}
			it.leading = uint8(lead)
			it.trailing = 64 - it.leading - uint8(sig)
		} else if it.leading == noWindow {
			// A window re-use before any window was established. Only
			// reachable from corrupt data, but it would otherwise decode
			// silently into garbage.
			return it.fail(errors.New("chunk: xor window re-use with no prior window"))
		}

		sigbits := 64 - int(it.leading) - int(it.trailing)
		raw, err := it.br.ReadBits(sigbits)
		if err != nil {
			return it.fail(err)
		}
		it.v = math.Float64frombits(math.Float64bits(it.v) ^ (raw << it.trailing))
	}

	it.numRead++
	return true
}

// SeekTo scans forward to the first sample at or after t.
//
// The scan is linear. Chunks hold at most a few hundred samples, so a binary
// search would need a per-chunk offset index that costs more space than the
// scan costs time; the block-level index already narrows queries to the
// chunks that can possibly match.
func (it *xorIterator) SeekTo(t int64) bool {
	if it.err != nil {
		return false
	}
	// Already at or past t: Seek must not rewind.
	if it.numRead > 0 && it.t >= t {
		return true
	}
	for it.Next() {
		if it.t >= t {
			return true
		}
	}
	return false
}
