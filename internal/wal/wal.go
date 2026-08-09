// Package wal implements the write-ahead log that makes ingested samples
// durable before they reach the head block.
//
// The log is a sequence of segment files, each a sequence of fixed-size
// pages. Records are packed into pages and fragmented across page boundaries
// when they do not fit, in the style of the LevelDB log.
//
// The page structure exists for exactly one reason: recovering from a torn
// write. A process killed mid-write leaves a partial record at the end of the
// log. Because every record carries its own length and checksum inside a page
// of known size, replay can tell "the log ends here, incompletely" from "the
// log is corrupt in the middle" - the first is expected after any crash and
// is recovered by truncation, the second is data loss and must be reported.
package wal

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"sync"
	"time"
)

const (
	// PageSize is the unit of writing and of torn-write recovery. 32KiB is
	// large enough that the per-page header overhead is negligible and small
	// enough that a partial page loses little.
	PageSize = 32 * 1024

	// recordHeaderSize is type(1) + length(2) + checksum(4).
	recordHeaderSize = 7

	// DefaultSegmentSize caps a single segment file. Segments are the unit of
	// truncation: once the head has been flushed to a block, whole segments
	// are deleted rather than rewritten.
	DefaultSegmentSize = 128 * 1024 * 1024

	pagesPerSegment = DefaultSegmentSize / PageSize

	// MaxRecordSize is the largest single record the log will accept.
	MaxRecordSize = 8 * 1024 * 1024
)

// recType tags a page fragment.
type recType byte

const (
	recPageTerm recType = 0 // zero padding to the end of a page
	recFull     recType = 1 // a complete record
	recFirst    recType = 2 // the first fragment of a record
	recMiddle   recType = 3 // a middle fragment
	recLast     recType = 4 // the final fragment
)

func (t recType) String() string {
	switch t {
	case recPageTerm:
		return "zero"
	case recFull:
		return "full"
	case recFirst:
		return "first"
	case recMiddle:
		return "middle"
	case recLast:
		return "last"
	}
	return fmt.Sprintf("unknown(%d)", byte(t))
}

// castagnoli is CRC-32C, chosen for its hardware support.
var castagnoli = crc32.MakeTable(crc32.Castagnoli)

// Errors reported by the log.
var (
	ErrClosed       = errors.New("wal: log is closed")
	ErrRecordTooBig = errors.New("wal: record exceeds the maximum size")

	// ErrCorrupt marks damage that truncation cannot explain: a bad checksum
	// or an impossible fragment sequence somewhere other than the tail. It is
	// deliberately distinct from a clean truncation, because the two demand
	// different responses - one is routine crash recovery, the other means
	// something has eaten the data and a human should know.
	ErrCorrupt = errors.New("wal: corrupt record")
)

// SyncPolicy controls when the log is forced to stable storage.
type SyncPolicy int

const (
	// SyncAlways fsyncs before every Log call returns. No acknowledged write
	// is ever lost, at the cost of a device round-trip per batch.
	SyncAlways SyncPolicy = iota

	// SyncInterval fsyncs on a timer. A crash can lose up to one interval of
	// acknowledged writes. This is the default because time-series ingest is
	// a firehose of individually near-worthless samples: losing 200ms of
	// scrapes is a gap in a graph, and paying a device round-trip per batch
	// to prevent it costs more throughput than the data is worth.
	SyncInterval

	// SyncNever leaves durability entirely to the operating system. Useful
	// for tests and for rebuildable data.
	SyncNever
)

// Options configures a log.
type Options struct {
	// SegmentSize caps each segment file. Zero selects DefaultSegmentSize.
	SegmentSize int

	// Sync selects the durability policy.
	Sync SyncPolicy

	// SyncInterval is the flush period for SyncInterval. Zero selects 200ms.
	SyncInterval time.Duration

	// Logger receives operational events. Zero uses the default logger.
	Logger *slog.Logger
}

func (o *Options) withDefaults() Options {
	out := *o
	if out.SegmentSize <= 0 {
		out.SegmentSize = DefaultSegmentSize
	}
	if out.SyncInterval <= 0 {
		out.SyncInterval = 200 * time.Millisecond
	}
	if out.Logger == nil {
		out.Logger = slog.Default()
	}
	return out
}

// page is the staging buffer for the segment currently being written.
type page struct {
	alloc   int // bytes filled
	flushed int // bytes already written to the file
	buf     [PageSize]byte
}

func (p *page) remaining() int { return PageSize - p.alloc }
func (p *page) full() bool     { return PageSize-p.alloc < recordHeaderSize }

func (p *page) reset() {
	// Zeroing matters: unwritten tail bytes must read back as recPageTerm so
	// replay recognises padding rather than decoding stale content.
	for i := range p.buf {
		p.buf[i] = 0
	}
	p.alloc = 0
	p.flushed = 0
}

// WAL is a segmented write-ahead log.
type WAL struct {
	dir  string
	opts Options

	mtx       sync.Mutex
	segment   *Segment
	page      *page
	donePages int
	closed    bool

	// dirtySinceSync tracks whether anything needs an fsync, so the background
	// ticker does not issue syscalls against an idle log.
	dirtySinceSync bool

	stopc chan struct{}
	donec chan struct{}
}

// Open opens or creates a log in dir, positioned to append after the last
// existing segment.
func Open(dir string, opts Options) (*WAL, error) {
	o := opts.withDefaults()
	if err := os.MkdirAll(dir, 0o777); err != nil {
		return nil, fmt.Errorf("wal: creating %s: %w", dir, err)
	}

	w := &WAL{
		dir:   dir,
		opts:  o,
		page:  &page{},
		stopc: make(chan struct{}),
		donec: make(chan struct{}),
	}

	_, last, err := Segments(dir)
	if err != nil {
		return nil, err
	}
	// A fresh log starts at segment 0; an existing one opens a new segment
	// rather than appending to the last. Appending would mean writing into a
	// page whose tail state we would have to reconstruct, and a new segment
	// costs one file.
	if _, err := w.nextSegment(last + 1); err != nil {
		return nil, err
	}

	if o.Sync == SyncInterval {
		go w.syncLoop()
	} else {
		close(w.donec)
	}
	return w, nil
}

func (w *WAL) syncLoop() {
	defer close(w.donec)
	t := time.NewTicker(w.opts.SyncInterval)
	defer t.Stop()
	for {
		select {
		case <-w.stopc:
			return
		case <-t.C:
			if err := w.Sync(); err != nil {
				w.opts.Logger.Error("wal: periodic sync failed", "err", err)
			}
		}
	}
}

// Dir returns the log's directory.
func (w *WAL) Dir() string { return w.dir }

// nextSegment closes the current segment and opens the one numbered n.
func (w *WAL) nextSegment(n int) (*Segment, error) {
	if w.segment != nil {
		if err := w.flushPage(true); err != nil {
			return nil, err
		}
		if err := w.segment.Sync(); err != nil {
			return nil, err
		}
		if err := w.segment.Close(); err != nil {
			return nil, err
		}
	}
	s, err := CreateSegment(w.dir, n)
	if err != nil {
		return nil, err
	}
	w.segment = s
	w.donePages = 0
	w.page.reset()
	return s, nil
}

// flushPage writes the buffered page to the segment. If complete is true the
// remainder of the page is abandoned as padding and a fresh page is started;
// otherwise only the newly filled bytes are written and the page stays open
// for more records.
func (w *WAL) flushPage(complete bool) error {
	p := w.page
	if p.alloc == p.flushed && !complete {
		return nil
	}

	end := p.alloc
	if complete {
		// Everything past alloc is already zero, which decodes as recPageTerm.
		end = PageSize
	}

	if _, err := w.segment.Write(p.buf[p.flushed:end]); err != nil {
		return fmt.Errorf("wal: writing page to %s: %w", w.segment.Name(), err)
	}
	p.flushed = end
	w.dirtySinceSync = true

	if complete {
		p.reset()
		w.donePages++
	}
	return nil
}

// Log appends records to the log atomically with respect to other Log calls.
// Either all of the supplied records are written or an error is returned.
func (w *WAL) Log(recs ...[]byte) error {
	if len(recs) == 0 {
		return nil
	}
	w.mtx.Lock()
	defer w.mtx.Unlock()

	if w.closed {
		return ErrClosed
	}
	for _, r := range recs {
		if len(r) > MaxRecordSize {
			return fmt.Errorf("%w: %d bytes exceeds %d", ErrRecordTooBig, len(r), MaxRecordSize)
		}
	}
	for i, r := range recs {
		// Only the last record forces the partial page out, so a batch costs
		// one write rather than one per record.
		if err := w.log(r, i == len(recs)-1); err != nil {
			return err
		}
	}
	if w.opts.Sync == SyncAlways {
		return w.sync()
	}
	return nil
}

func (w *WAL) log(rec []byte, final bool) error {
	if w.page.full() {
		if err := w.flushPage(true); err != nil {
			return err
		}
	}

	// Space left in this segment, accounting for the header each page
	// fragment will need.
	pagesLeft := w.opts.SegmentSize/PageSize - w.donePages - 1
	left := w.page.remaining() - recordHeaderSize
	if pagesLeft > 0 {
		left += pagesLeft * (PageSize - recordHeaderSize)
	}
	if len(rec) > left {
		if _, err := w.nextSegment(w.segment.Index() + 1); err != nil {
			return err
		}
	}

	// Fragment the record across pages. The loop runs at least once so that a
	// zero-length record still produces a recFull fragment - the head block
	// uses those as markers and dropping them silently would be worse than
	// the byte they cost.
	for i := 0; i == 0 || len(rec) > 0; i++ {
		p := w.page

		n := min(len(rec), p.remaining()-recordHeaderSize)
		part := rec[:n]

		var typ recType
		switch {
		case i == 0 && n == len(rec):
			typ = recFull
		case n == len(rec):
			typ = recLast
		case i == 0:
			typ = recFirst
		default:
			typ = recMiddle
		}

		buf := p.buf[p.alloc:]
		buf[0] = byte(typ)
		binary.BigEndian.PutUint16(buf[1:], uint16(n))
		// The checksum covers the type and length as well as the payload, so
		// a flipped bit in the header is caught rather than steering the
		// decoder into the wrong number of bytes.
		crc := crc32.Checksum(buf[:3], castagnoli)
		crc = crc32.Update(crc, castagnoli, part)
		binary.BigEndian.PutUint32(buf[3:], crc)
		copy(buf[recordHeaderSize:], part)

		p.alloc += n + recordHeaderSize
		rec = rec[n:]

		if p.full() {
			if err := w.flushPage(true); err != nil {
				return err
			}
		}
	}

	if final && w.page.alloc > w.page.flushed {
		if err := w.flushPage(false); err != nil {
			return err
		}
	}
	return nil
}

// Sync flushes buffered data and forces it to stable storage.
func (w *WAL) Sync() error {
	w.mtx.Lock()
	defer w.mtx.Unlock()
	if w.closed {
		return ErrClosed
	}
	return w.sync()
}

func (w *WAL) sync() error {
	if err := w.flushPage(false); err != nil {
		return err
	}
	if !w.dirtySinceSync {
		return nil
	}
	if err := w.segment.Sync(); err != nil {
		return err
	}
	w.dirtySinceSync = false
	return nil
}

// NextSegment closes the active segment and starts a new one. The head block
// calls this before a flush so that everything belonging to the flushed range
// lives in segments that can then be deleted whole.
func (w *WAL) NextSegment() error {
	w.mtx.Lock()
	defer w.mtx.Unlock()
	if w.closed {
		return ErrClosed
	}
	_, err := w.nextSegment(w.segment.Index() + 1)
	return err
}

// Truncate deletes every segment numbered below n.
func (w *WAL) Truncate(n int) error {
	w.mtx.Lock()
	defer w.mtx.Unlock()
	if w.closed {
		return ErrClosed
	}

	first, _, err := Segments(w.dir)
	if err != nil {
		return err
	}
	if first < 0 {
		return nil
	}
	for i := first; i < n; i++ {
		if i == w.segment.Index() {
			// Never delete the segment being written to.
			break
		}
		path := SegmentName(w.dir, i)
		if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("wal: removing segment %s: %w", path, err)
		}
	}
	return nil
}

// Close flushes, syncs and closes the log.
func (w *WAL) Close() error {
	w.mtx.Lock()
	if w.closed {
		w.mtx.Unlock()
		return ErrClosed
	}
	w.closed = true
	w.mtx.Unlock()

	// Stop the sync loop before the final flush, so it cannot race with Close
	// on an already-closed file.
	close(w.stopc)
	<-w.donec

	w.mtx.Lock()
	defer w.mtx.Unlock()

	if err := w.flushPage(false); err != nil {
		return err
	}
	if err := w.segment.Sync(); err != nil {
		return err
	}
	return w.segment.Close()
}

// Segment is one file of the log.
type Segment struct {
	*os.File
	dir string
	i   int
}

// Index returns the segment's sequence number.
func (s *Segment) Index() int { return s.i }

// Dir returns the directory containing the segment.
func (s *Segment) Dir() string { return s.dir }

// SegmentName builds the path of segment i. Segments are named with a
// zero-padded fixed width so that lexical order matches numeric order, which
// keeps directory listings usable and replay ordering trivial.
func SegmentName(dir string, i int) string {
	return filepath.Join(dir, fmt.Sprintf("%08d", i))
}

// CreateSegment creates segment i in dir.
func CreateSegment(dir string, i int) (*Segment, error) {
	f, err := os.OpenFile(SegmentName(dir, i), os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0o666)
	if err != nil {
		return nil, fmt.Errorf("wal: creating segment %d: %w", i, err)
	}
	return &Segment{File: f, dir: dir, i: i}, nil
}

// OpenReadSegment opens an existing segment for reading.
func OpenReadSegment(name string) (*Segment, error) {
	i, err := strconv.Atoi(filepath.Base(name))
	if err != nil {
		return nil, fmt.Errorf("wal: segment name %q is not a sequence number", filepath.Base(name))
	}
	f, err := os.Open(name)
	if err != nil {
		return nil, err
	}
	return &Segment{File: f, dir: filepath.Dir(name), i: i}, nil
}

// Segments returns the lowest and highest segment numbers present in dir, or
// (-1, -1) when the directory holds none.
func Segments(dir string) (first, last int, err error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return -1, -1, nil
		}
		return 0, 0, err
	}

	var nums []int
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		n, err := strconv.Atoi(e.Name())
		if err != nil {
			// Not a segment. Checkpoint directories and stray files live
			// alongside segments, so this is expected rather than an error.
			continue
		}
		nums = append(nums, n)
	}
	if len(nums) == 0 {
		return -1, -1, nil
	}
	sort.Ints(nums)

	// A gap means a segment was lost, which replay cannot paper over: the
	// records in it are simply gone and the resulting head would be missing
	// samples with no indication.
	for i := 1; i < len(nums); i++ {
		if nums[i] != nums[i-1]+1 {
			return 0, 0, fmt.Errorf("%w: segments jump from %d to %d", ErrCorrupt, nums[i-1], nums[i])
		}
	}
	return nums[0], nums[len(nums)-1], nil
}

// Replayer reads every record in a log directory, in order.
//
// Each segment gets its own Reader rather than being concatenated into one
// stream. That is not a stylistic choice: page offsets are relative to the
// start of a segment, and a segment left short by a crash would throw the
// alignment of every segment after it. Concatenating and then padding to
// restore alignment is worse still - the padding bytes satisfy the length of
// a half-written record, turning a torn write into a checksum failure and so
// a spurious corruption report.
//
// This works because a record never spans segments: the writer starts a new
// segment before writing a record that would not fit in the current one.
type Replayer struct {
	segs []*Segment
	cur  int
	rdr  *Reader
	err  error
}

// NewReplayer opens every segment in dir for replay.
func NewReplayer(dir string) (*Replayer, error) {
	first, last, err := Segments(dir)
	if err != nil {
		return nil, err
	}
	r := &Replayer{}
	if first < 0 {
		return r, nil
	}
	for i := first; i <= last; i++ {
		s, err := OpenReadSegment(SegmentName(dir, i))
		if err != nil {
			r.Close()
			return nil, err
		}
		r.segs = append(r.segs, s)
	}
	return r, nil
}

// Next advances to the next record across all segments.
func (r *Replayer) Next() bool {
	if r.err != nil {
		return false
	}
	for {
		if r.rdr == nil {
			if r.cur >= len(r.segs) {
				return false
			}
			r.rdr = NewReader(r.segs[r.cur])
			r.cur++
		}
		if r.rdr.Next() {
			return true
		}
		if err := r.rdr.Err(); err != nil {
			// Corruption is attributed to its segment, since that is what an
			// operator needs in order to do anything about it.
			r.err = fmt.Errorf("segment %s: %w", r.segs[r.cur-1].Name(), err)
			return false
		}
		// Clean end of this segment; move on.
		r.rdr = nil
	}
}

// Record returns the record read by the last Next.
func (r *Replayer) Record() []byte {
	if r.rdr == nil {
		return nil
	}
	return r.rdr.Record()
}

// Err returns the error that stopped the replay, if any.
func (r *Replayer) Err() error { return r.err }

// Segment returns the index of the segment the last record came from, or -1.
// The head block records this so it knows which segments a flush made
// redundant.
func (r *Replayer) Segment() int {
	if r.cur == 0 {
		return -1
	}
	return r.segs[r.cur-1].Index()
}

// Close releases every open segment.
func (r *Replayer) Close() error {
	var first error
	for _, s := range r.segs {
		if err := s.Close(); err != nil && first == nil {
			first = err
		}
	}
	r.segs = nil
	return first
}
