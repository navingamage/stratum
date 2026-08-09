// Package chunk implements the compressed sample containers that hold all
// time-series data in stratum, both in the head block and on disk.
//
// The encoding is the one from Facebook's Gorilla paper[1]: timestamps are
// stored as delta-of-delta and float values as an XOR against the previous
// value, with variable-width control prefixes so that the overwhelmingly
// common cases (a regular scrape interval, a value that barely moves) cost a
// bit or two rather than sixteen bytes.
//
// [1] Pelkonen et al., "Gorilla: A Fast, Scalable, In-Memory Time Series
// Database", VLDB 2015.
package chunk

import (
	"errors"
	"fmt"
	"sync"
)

// Encoding identifies a chunk's on-disk format. It is persisted, so existing
// values must never be renumbered.
type Encoding uint8

// Supported encodings.
const (
	EncNone Encoding = 0
	EncXOR  Encoding = 1
)

func (e Encoding) String() string {
	switch e {
	case EncNone:
		return "none"
	case EncXOR:
		return "xor"
	}
	return fmt.Sprintf("unknown(%d)", uint8(e))
}

// Valid reports whether e is an encoding this build can decode.
func (e Encoding) Valid() bool { return e == EncXOR }

// Errors returned by this package.
var (
	// ErrChunkSealed is returned when appending to a chunk that was loaded
	// from disk. Persisted chunks are immutable: the encoder's state (the
	// running delta and the leading/trailing zero window) is not part of the
	// serialised form, and reconstructing it would mean trusting bytes we
	// have only just checksummed. Head chunks are rebuilt from the WAL
	// instead.
	ErrChunkSealed = errors.New("chunk: chunk is sealed and cannot be appended to")

	// ErrChunkFull is returned once a chunk holds MaxSamplesPerChunk samples.
	ErrChunkFull = errors.New("chunk: chunk is full")

	// ErrOutOfOrder is returned when a sample's timestamp is not strictly
	// greater than the previous one. Chunks are strictly ordered; the head
	// block is responsible for routing late samples elsewhere.
	ErrOutOfOrder = errors.New("chunk: sample timestamp is not monotonic")

	// ErrUnknownEncoding is returned by FromData for an unrecognised format.
	ErrUnknownEncoding = errors.New("chunk: unknown encoding")
)

// MaxSamplesPerChunk bounds a single chunk. The limit exists because the
// sample count is stored in a two-byte header, but the practical ceiling is
// much lower: the head block cuts a chunk at 120 samples, which at a 15s
// scrape interval is 30 minutes of data and keeps the linear scan inside
// Iterator.SeekTo cheap enough not to need an index.
//
// The method is SeekTo rather than Seek only because go vet's stdmethods
// check reserves the latter name for the io.Seeker signature.
const MaxSamplesPerChunk = 1<<16 - 1

// Chunk is a compressed, ordered run of samples for a single series.
type Chunk interface {
	// Bytes returns the serialised chunk. The slice aliases the chunk's own
	// storage and is only valid until the next Append.
	Bytes() []byte

	// Encoding reports the chunk's format.
	Encoding() Encoding

	// Appender returns an appender positioned at the end of the chunk.
	Appender() (Appender, error)

	// Iterator returns an iterator over the chunk. If it is non-nil and of a
	// compatible concrete type it is reset and returned, avoiding an
	// allocation per series per query.
	Iterator(it Iterator) Iterator

	// NumSamples reports how many samples the chunk holds.
	NumSamples() int
}

// Appender adds samples to the end of a chunk.
type Appender interface {
	// Append adds a sample. Timestamps must be strictly increasing.
	Append(t int64, v float64) error
}

// Iterator walks the samples of a chunk in timestamp order.
type Iterator interface {
	// Next advances to the following sample, reporting whether one exists.
	Next() bool

	// SeekTo advances to the first sample at or after t. It reports false if
	// no such sample exists. Seeking backwards is a no-op that keeps the
	// current position.
	SeekTo(t int64) bool

	// At returns the sample at the current position. It is only valid after
	// a call to Next or SeekTo that returned true.
	At() (int64, float64)

	// Err returns any decoding error encountered. An exhausted iterator
	// returns nil; a corrupt one returns the failure.
	Err() error
}

// FromData wraps an already-encoded chunk body for reading. The returned
// chunk is sealed. The data is not copied, so the caller must keep it alive -
// for chunks read out of an mmap'd block that means the block stays open.
func FromData(enc Encoding, data []byte) (Chunk, error) {
	switch enc {
	case EncXOR:
		return xorChunkFromData(data)
	default:
		return nil, fmt.Errorf("%w: %s", ErrUnknownEncoding, enc)
	}
}

// Pool recycles chunks and iterators across compactions and queries. A single
// compaction of a large block allocates one chunk per series otherwise, and
// those are exactly the medium-lived allocations the GC handles worst.
type Pool struct {
	xorChunks sync.Pool
	xorIters  sync.Pool
}

// NewPool returns a ready-to-use pool.
func NewPool() *Pool {
	return &Pool{
		xorChunks: sync.Pool{New: func() any { return NewXORChunk() }},
		xorIters:  sync.Pool{New: func() any { return &xorIterator{} }},
	}
}

// Get returns a reset chunk of the requested encoding.
func (p *Pool) Get(enc Encoding) (Chunk, error) {
	switch enc {
	case EncXOR:
		c := p.xorChunks.Get().(*XORChunk)
		c.Reset()
		return c, nil
	default:
		return nil, fmt.Errorf("%w: %s", ErrUnknownEncoding, enc)
	}
}

// Put returns a chunk to the pool. Sealed chunks are dropped rather than
// pooled: they alias memory the pool does not own.
func (p *Pool) Put(c Chunk) {
	xc, ok := c.(*XORChunk)
	if !ok || xc.sealed {
		return
	}
	p.xorChunks.Put(xc)
}
