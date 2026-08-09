package wal

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
)

// Reader replays records from a log stream.
//
// The central judgement this type makes is whether a stream that ends
// unexpectedly is a torn write or corruption. A record that runs off the end
// of the data is a torn write: the process died mid-write and the record was
// never acknowledged, so dropping it is correct and expected. A record whose
// checksum fails, or a fragment sequence that cannot occur (a middle without
// a first), means bytes changed after they were written - that is data loss,
// and Err reports it as ErrCorrupt rather than quietly ending the replay.
type Reader struct {
	rdr io.Reader

	rec []byte         // record assembled so far
	buf [PageSize]byte // current page
	tot int64          // bytes consumed, for error messages

	err error
}

// NewReader returns a Reader over r.
func NewReader(r io.Reader) *Reader {
	return &Reader{rdr: r}
}

// Record returns the record read by the last call to Next. The slice is
// invalidated by the next call.
func (r *Reader) Record() []byte { return r.rec }

// Err returns the error that stopped the replay, or nil if it ended cleanly.
// A clean end includes a torn final record.
func (r *Reader) Err() error { return r.err }

// Offset reports how many bytes have been consumed.
func (r *Reader) Offset() int64 { return r.tot }

// Next assembles the following record, reporting whether one was available.
func (r *Reader) Next() bool {
	if r.err != nil {
		return false
	}
	r.rec = r.rec[:0]

	for i := 0; ; i++ {
		typ, part, err := r.readFragment()
		if err != nil {
			// A fragment that runs out of data is the torn tail. It is only
			// benign if we were not part-way through assembling a record; a
			// truncated continuation means the record is unrecoverable, but
			// it was never acknowledged either, so it is still a clean end.
			if err == io.EOF {
				return false
			}
			r.err = err
			return false
		}

		switch typ {
		case recPageTerm:
			// Padding to the end of a page. Skip the rest of the page and
			// carry on with the next one.
			if err := r.skipPageRemainder(); err != nil {
				if err == io.EOF {
					return false
				}
				r.err = err
				return false
			}
			// Padding cannot appear in the middle of a fragmented record.
			if i > 0 && len(r.rec) > 0 {
				r.err = fmt.Errorf("%w: page padding inside a fragmented record at offset %d", ErrCorrupt, r.tot)
				return false
			}
			i--
			continue

		case recFull:
			if i != 0 {
				r.err = fmt.Errorf("%w: unexpected full fragment at offset %d, mid-record", ErrCorrupt, r.tot)
				return false
			}
			r.rec = append(r.rec, part...)
			return true

		case recFirst:
			if i != 0 {
				r.err = fmt.Errorf("%w: unexpected first fragment at offset %d, mid-record", ErrCorrupt, r.tot)
				return false
			}
			r.rec = append(r.rec, part...)

		case recMiddle:
			if i == 0 {
				r.err = fmt.Errorf("%w: middle fragment at offset %d with no preceding first", ErrCorrupt, r.tot)
				return false
			}
			r.rec = append(r.rec, part...)

		case recLast:
			if i == 0 {
				r.err = fmt.Errorf("%w: last fragment at offset %d with no preceding first", ErrCorrupt, r.tot)
				return false
			}
			r.rec = append(r.rec, part...)
			return true

		default:
			r.err = fmt.Errorf("%w: invalid fragment type %d at offset %d", ErrCorrupt, typ, r.tot)
			return false
		}
	}
}

// pageOffset returns the position within the current page.
func (r *Reader) pageOffset() int { return int(r.tot % PageSize) }

// readFragment reads one page fragment. It returns io.EOF when the stream is
// exhausted, including when it ends part-way through a fragment.
func (r *Reader) readFragment() (recType, []byte, error) {
	// A header cannot straddle a page boundary; the writer starts a new page
	// rather than split one.
	if PageSize-r.pageOffset() < recordHeaderSize {
		if err := r.skipPageRemainder(); err != nil {
			return 0, nil, err
		}
	}

	hdr := r.buf[:recordHeaderSize]
	if err := r.readFull(hdr); err != nil {
		return 0, nil, err
	}

	typ := recType(hdr[0])
	if typ == recPageTerm {
		return typ, nil, nil
	}

	length := int(binary.BigEndian.Uint16(hdr[1:3]))
	want := binary.BigEndian.Uint32(hdr[3:7])

	if length > PageSize-recordHeaderSize {
		return 0, nil, fmt.Errorf("%w: fragment length %d exceeds a page at offset %d",
			ErrCorrupt, length, r.tot)
	}
	// A fragment must fit in the remainder of its page.
	if length > PageSize-r.pageOffset() {
		return 0, nil, fmt.Errorf("%w: fragment of %d bytes overruns its page at offset %d",
			ErrCorrupt, length, r.tot)
	}

	part := r.buf[recordHeaderSize : recordHeaderSize+length]
	if err := r.readFull(part); err != nil {
		return 0, nil, err
	}

	crc := crc32.Checksum(hdr[:3], castagnoli)
	crc = crc32.Update(crc, castagnoli, part)
	if crc != want {
		// A zeroed header reads as recPageTerm and is handled above, so
		// reaching a checksum failure means real damage rather than an
		// unwritten tail.
		return 0, nil, fmt.Errorf("%w: checksum mismatch at offset %d (got %08x, want %08x)",
			ErrCorrupt, r.tot, crc, want)
	}
	return typ, part, nil
}

// skipPageRemainder advances to the start of the next page.
func (r *Reader) skipPageRemainder() error {
	n := PageSize - r.pageOffset()
	if n == PageSize {
		return nil
	}
	return r.readFull(r.buf[:n])
}

// readFull fills b, translating a partial read into io.EOF. A partial read is
// precisely the torn write this format exists to survive.
func (r *Reader) readFull(b []byte) error {
	n, err := io.ReadFull(r.rdr, b)
	r.tot += int64(n)
	switch err {
	case nil:
		return nil
	case io.EOF, io.ErrUnexpectedEOF:
		return io.EOF
	default:
		return err
	}
}
