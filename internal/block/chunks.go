package block

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"

	"github.com/navingamage/stratum/internal/chunk"
	"github.com/navingamage/stratum/internal/encoding"
)

// ChunksFilename is the name of the chunk data file inside a block.
const ChunksFilename = "chunks"

const (
	// chunksMagic tags the chunk file. Reading a file whose first four bytes
	// are wrong fails immediately with a clear message rather than producing
	// nonsense offsets.
	chunksMagic uint32 = 0x53545243 // "STRC"

	chunksFormatV1 byte = 1

	// chunksHeaderSize is magic(4) + version(1) + padding(3). The padding
	// exists so that the first chunk starts on an 8-byte boundary.
	chunksHeaderSize = 8
)

// ErrCorruptBlock reports damage detected while reading a block.
var ErrCorruptBlock = errors.New("block: corrupt data")

// ChunkRef locates a chunk within a block's chunk file. It is the byte offset
// of the chunk's length prefix.
type ChunkRef uint64

// ChunkWriter appends chunks to a block's chunk file.
//
// The layout of each entry is:
//
//	uvarint  payload length
//	byte     encoding
//	bytes    payload
//	uint32be CRC-32C over the encoding byte and the payload
//
// The checksum is per chunk rather than per file so that a query verifies
// only what it reads. Verifying a whole 512MiB chunk file to answer a query
// touching one series would make checksums cost more than they are worth,
// and operators would turn them off.
type ChunkWriter struct {
	f   *os.File
	w   *bufio.Writer
	pos uint64
	buf [binary.MaxVarintLen64]byte
}

// NewChunkWriter creates the chunk file in dir.
func NewChunkWriter(dir string) (*ChunkWriter, error) {
	f, err := os.OpenFile(filepath.Join(dir, ChunksFilename), os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o666)
	if err != nil {
		return nil, fmt.Errorf("block: creating the chunk file: %w", err)
	}

	cw := &ChunkWriter{f: f, w: bufio.NewWriterSize(f, 1<<20)}

	var hdr [chunksHeaderSize]byte
	binary.BigEndian.PutUint32(hdr[:4], chunksMagic)
	hdr[4] = chunksFormatV1
	if _, err := cw.w.Write(hdr[:]); err != nil {
		f.Close()
		return nil, err
	}
	cw.pos = chunksHeaderSize
	return cw, nil
}

// WriteChunk appends a chunk and returns the reference to read it back.
func (w *ChunkWriter) WriteChunk(c chunk.Chunk) (ChunkRef, error) {
	data := c.Bytes()
	ref := ChunkRef(w.pos)

	n := binary.PutUvarint(w.buf[:], uint64(len(data)))
	if _, err := w.w.Write(w.buf[:n]); err != nil {
		return 0, err
	}
	w.pos += uint64(n)

	encByte := [1]byte{byte(c.Encoding())}
	if _, err := w.w.Write(encByte[:]); err != nil {
		return 0, err
	}
	if _, err := w.w.Write(data); err != nil {
		return 0, err
	}
	w.pos += 1 + uint64(len(data))

	crc := crc32.Checksum(encByte[:], encoding.Castagnoli)
	crc = crc32.Update(crc, encoding.Castagnoli, data)
	var crcBuf [4]byte
	binary.BigEndian.PutUint32(crcBuf[:], crc)
	if _, err := w.w.Write(crcBuf[:]); err != nil {
		return 0, err
	}
	w.pos += 4

	return ref, nil
}

// Size reports the bytes written so far.
func (w *ChunkWriter) Size() uint64 { return w.pos }

// Close flushes and syncs the chunk file.
func (w *ChunkWriter) Close() error {
	if err := w.w.Flush(); err != nil {
		w.f.Close()
		return err
	}
	if err := w.f.Sync(); err != nil {
		w.f.Close()
		return err
	}
	return w.f.Close()
}

// ChunkReader reads chunks out of a mapped chunk file.
type ChunkReader struct {
	m *mappedFile
	b []byte
}

// OpenChunkReader maps the chunk file in dir.
func OpenChunkReader(dir string) (*ChunkReader, error) {
	m, err := openMapped(filepath.Join(dir, ChunksFilename))
	if err != nil {
		return nil, fmt.Errorf("block: opening the chunk file: %w", err)
	}
	b := m.Bytes()

	fail := func(format string, args ...any) (*ChunkReader, error) {
		m.Close()
		return nil, fmt.Errorf(format, args...)
	}
	if len(b) < chunksHeaderSize {
		return fail("%w: chunk file is %d bytes, shorter than its header", ErrCorruptBlock, len(b))
	}
	if got := binary.BigEndian.Uint32(b[:4]); got != chunksMagic {
		return fail("%w: chunk file magic is %#08x, want %#08x", ErrCorruptBlock, got, chunksMagic)
	}
	if b[4] != chunksFormatV1 {
		return fail("block: chunk file format %d, this build understands %d", b[4], chunksFormatV1)
	}

	return &ChunkReader{m: m, b: b}, nil
}

// Chunk returns the chunk at ref. The returned chunk aliases the mapping, so
// it is only valid while the reader is open.
func (r *ChunkReader) Chunk(ref ChunkRef) (chunk.Chunk, error) {
	off := uint64(ref)
	if off >= uint64(len(r.b)) {
		return nil, fmt.Errorf("%w: chunk reference %d is past the end of a %d byte file",
			ErrCorruptBlock, off, len(r.b))
	}

	rest := r.b[off:]
	length, n := binary.Uvarint(rest)
	if n <= 0 {
		return nil, fmt.Errorf("%w: unreadable chunk length at offset %d", ErrCorruptBlock, off)
	}
	rest = rest[n:]

	// 1 encoding byte + payload + 4 checksum bytes.
	if uint64(len(rest)) < length+5 {
		return nil, fmt.Errorf("%w: chunk at offset %d claims %d bytes with only %d available",
			ErrCorruptBlock, off, length, len(rest))
	}

	enc := chunk.Encoding(rest[0])
	payload := rest[1 : 1+length]
	want := binary.BigEndian.Uint32(rest[1+length : 5+length])

	crc := crc32.Checksum(rest[:1], encoding.Castagnoli)
	crc = crc32.Update(crc, encoding.Castagnoli, payload)
	if crc != want {
		return nil, fmt.Errorf("%w: chunk at offset %d fails its checksum (got %08x, want %08x)",
			ErrCorruptBlock, off, crc, want)
	}

	return chunk.FromData(enc, payload)
}

// Size reports the mapped file size.
func (r *ChunkReader) Size() int { return len(r.b) }

// Close releases the mapping.
func (r *ChunkReader) Close() error {
	r.b = nil
	return r.m.Close()
}
