package block

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"sort"

	"github.com/navingamage/stratum/internal/encoding"
	"github.com/navingamage/stratum/internal/index"
	"github.com/navingamage/stratum/internal/model"
)

// IndexFilename is the name of the index file inside a block.
const IndexFilename = "index"

const (
	indexMagic uint32 = 0x53545249 // "STRI"

	indexFormatV1 byte = 1

	indexHeaderSize = 8

	// tocSize is six section offsets plus a checksum.
	tocSize = 6*8 + 4
)

// ChunkMeta describes one chunk of a series within a block.
type ChunkMeta struct {
	Ref              ChunkRef
	MinTime, MaxTime int64
}

// IndexWriter builds a block's index file.
//
// It is used in two phases: every symbol first, then every series in label
// order. The order is not a convenience for the writer, it is what the format
// requires - the symbol table is written before the series that reference it,
// and series are stored sorted so that the reader can binary search them.
//
// Postings are accumulated in memory. They are the same size as the head's
// in-memory index for the same data, which the process was already holding,
// so the peak is unchanged; everything else streams straight to disk.
type IndexWriter struct {
	f   *os.File
	w   *bufio.Writer
	pos uint64

	symbols   map[string]uint32
	numSeries int

	// seriesOffsets maps dense series index to file offset.
	seriesOffsets []uint64

	// postings maps a label pair to the dense series indexes carrying it.
	postings map[labelPair][]uint32
	// labelValues maps a label name to the values seen for it.
	labelValues map[string]map[string]struct{}

	// toc records where each section begins.
	toc tableOfContents

	closed bool
}

type labelPair struct{ Name, Value string }

type tableOfContents struct {
	Symbols       uint64
	Series        uint64
	SeriesTable   uint64
	Postings      uint64
	PostingsTable uint64
	LabelIndex    uint64
}

// NewIndexWriter creates the index file in dir.
func NewIndexWriter(dir string) (*IndexWriter, error) {
	f, err := os.OpenFile(filepath.Join(dir, IndexFilename), os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o666)
	if err != nil {
		return nil, fmt.Errorf("block: creating the index file: %w", err)
	}

	w := &IndexWriter{
		f:           f,
		w:           bufio.NewWriterSize(f, 1<<20),
		symbols:     make(map[string]uint32),
		postings:    make(map[labelPair][]uint32),
		labelValues: make(map[string]map[string]struct{}),
	}

	var hdr [indexHeaderSize]byte
	binary.BigEndian.PutUint32(hdr[:4], indexMagic)
	hdr[4] = indexFormatV1
	if err := w.write(hdr[:]); err != nil {
		f.Close()
		return nil, err
	}
	return w, nil
}

func (w *IndexWriter) write(b []byte) error {
	n, err := w.w.Write(b)
	w.pos += uint64(n)
	return err
}

// AddSymbols writes the symbol table. It must be called once, before any
// series, with every label name and value the block will use.
func (w *IndexWriter) AddSymbols(symbols []string) error {
	if w.toc.Symbols != 0 {
		return fmt.Errorf("block: AddSymbols called twice")
	}
	w.toc.Symbols = w.pos

	sorted := append([]string(nil), symbols...)
	sort.Strings(sorted)

	var enc encoding.Encbuf
	enc.PutUvarint(len(sorted))
	for i, s := range sorted {
		w.symbols[s] = uint32(i)
		enc.PutUvarintStr(s)
	}
	enc.PutHash(encoding.NewCRC32())

	return w.write(enc.Get())
}

// AddSeries appends one series. Series must arrive in ascending label order,
// which the reader relies on for binary search.
func (w *IndexWriter) AddSeries(ls model.Labels, chunks []ChunkMeta) error {
	if w.toc.Series == 0 {
		w.toc.Series = w.pos
	}
	idx := uint32(w.numSeries)
	w.seriesOffsets = append(w.seriesOffsets, w.pos)
	w.numSeries++

	var enc encoding.Encbuf
	enc.PutUvarint(len(ls))
	for _, l := range ls {
		ni, ok := w.symbols[l.Name]
		if !ok {
			return fmt.Errorf("block: label name %q was not declared in the symbol table", l.Name)
		}
		vi, ok := w.symbols[l.Value]
		if !ok {
			return fmt.Errorf("block: label value %q was not declared in the symbol table", l.Value)
		}
		enc.PutUvarint64(uint64(ni))
		enc.PutUvarint64(uint64(vi))

		p := labelPair{l.Name, l.Value}
		w.postings[p] = append(w.postings[p], idx)
		if w.labelValues[l.Name] == nil {
			w.labelValues[l.Name] = make(map[string]struct{})
		}
		w.labelValues[l.Name][l.Value] = struct{}{}
	}

	// Chunk metadata is delta-encoded against the previous chunk of the same
	// series. Chunks of one series are contiguous in the chunk file and cover
	// consecutive time ranges, so both deltas are small.
	enc.PutUvarint(len(chunks))
	var prevRef uint64
	var prevMax int64
	for i, c := range chunks {
		if i == 0 {
			enc.PutVarint64(c.MinTime)
			enc.PutUvarint64(uint64(c.Ref))
		} else {
			enc.PutVarint64(c.MinTime - prevMax)
			enc.PutUvarint64(uint64(c.Ref) - prevRef)
		}
		enc.PutUvarint64(uint64(c.MaxTime - c.MinTime))
		prevRef, prevMax = uint64(c.Ref), c.MaxTime
	}

	// Every series also joins the reserved all-postings list, so that a query
	// built only from negations has something to subtract from.
	allName, allValue := index.AllPostingsKey()
	ap := labelPair{allName, allValue}
	w.postings[ap] = append(w.postings[ap], idx)

	enc.PutHash(encoding.NewCRC32())
	return w.write(enc.Get())
}

// Close writes the remaining sections and the table of contents.
func (w *IndexWriter) Close() error {
	if w.closed {
		return nil
	}
	w.closed = true

	if err := w.writeSeriesTable(); err != nil {
		return err
	}
	if err := w.writePostings(); err != nil {
		return err
	}
	if err := w.writeLabelIndex(); err != nil {
		return err
	}
	if err := w.writeTOC(); err != nil {
		return err
	}

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

func (w *IndexWriter) writeSeriesTable() error {
	w.toc.SeriesTable = w.pos

	var enc encoding.Encbuf
	enc.PutUvarint(len(w.seriesOffsets))
	// Offsets ascend, so a delta costs one or two bytes instead of eight.
	var prev uint64
	for _, off := range w.seriesOffsets {
		enc.PutUvarint64(off - prev)
		prev = off
	}
	enc.PutHash(encoding.NewCRC32())
	return w.write(enc.Get())
}

func (w *IndexWriter) writePostings() error {
	w.toc.Postings = w.pos

	// Sorted so that the file is byte-identical for identical input, which
	// makes a compaction reproducible and a diff meaningful.
	pairs := make([]labelPair, 0, len(w.postings))
	for p := range w.postings {
		pairs = append(pairs, p)
	}
	sort.Slice(pairs, func(i, j int) bool {
		if pairs[i].Name != pairs[j].Name {
			return pairs[i].Name < pairs[j].Name
		}
		return pairs[i].Value < pairs[j].Value
	})

	offsets := make([]uint64, len(pairs))
	for i, p := range pairs {
		offsets[i] = w.pos

		refs := w.postings[p]
		sort.Slice(refs, func(a, b int) bool { return refs[a] < refs[b] })

		// Fixed-width big-endian, not varints. The reader binary searches
		// these lists during a Seek, which needs constant-time indexing;
		// varints would force a linear decode and turn every leapfrog step
		// into a scan.
		var enc encoding.Encbuf
		enc.PutBE32(uint32(len(refs)))
		for _, r := range refs {
			enc.PutBE32(r)
		}
		enc.PutHash(encoding.NewCRC32())
		if err := w.write(enc.Get()); err != nil {
			return err
		}
	}

	w.toc.PostingsTable = w.pos
	var enc encoding.Encbuf
	enc.PutUvarint(len(pairs))
	for i, p := range pairs {
		enc.PutUvarintStr(p.Name)
		enc.PutUvarintStr(p.Value)
		enc.PutUvarint64(offsets[i])
	}
	enc.PutHash(encoding.NewCRC32())
	return w.write(enc.Get())
}

func (w *IndexWriter) writeLabelIndex() error {
	w.toc.LabelIndex = w.pos

	names := make([]string, 0, len(w.labelValues))
	for n := range w.labelValues {
		names = append(names, n)
	}
	sort.Strings(names)

	var enc encoding.Encbuf
	enc.PutUvarint(len(names))
	for _, n := range names {
		enc.PutUvarintStr(n)
		vals := make([]string, 0, len(w.labelValues[n]))
		for v := range w.labelValues[n] {
			vals = append(vals, v)
		}
		sort.Strings(vals)
		enc.PutUvarint(len(vals))
		for _, v := range vals {
			enc.PutUvarintStr(v)
		}
	}
	enc.PutHash(encoding.NewCRC32())
	return w.write(enc.Get())
}

func (w *IndexWriter) writeTOC() error {
	var enc encoding.Encbuf
	enc.PutBE64(w.toc.Symbols)
	enc.PutBE64(w.toc.Series)
	enc.PutBE64(w.toc.SeriesTable)
	enc.PutBE64(w.toc.Postings)
	enc.PutBE64(w.toc.PostingsTable)
	enc.PutBE64(w.toc.LabelIndex)
	enc.PutHash(encoding.NewCRC32())
	return w.write(enc.Get())
}

// IndexReader reads a block's index out of a mapping.
//
// The symbol table, series offset table, postings offset table and label
// index are decoded eagerly at open time - they are small, and paying for
// them once beats paying on every query. Series records and postings lists
// stay in the mapping and are decoded on demand.
type IndexReader struct {
	m *mappedFile
	b []byte

	symbols       []string
	seriesOffsets []uint64
	postingsOff   map[labelPair]uint64
	labelIndex    map[string][]string
	labelNamesAll []string
}

// OpenIndexReader maps and parses the index file in dir.
func OpenIndexReader(dir string) (*IndexReader, error) {
	m, err := openMapped(filepath.Join(dir, IndexFilename))
	if err != nil {
		return nil, fmt.Errorf("block: opening the index: %w", err)
	}

	r := &IndexReader{m: m, b: m.Bytes()}
	if err := r.init(); err != nil {
		r.Close()
		return nil, err
	}
	return r, nil
}

func (r *IndexReader) init() error {
	if len(r.b) < indexHeaderSize+tocSize {
		return fmt.Errorf("%w: index is %d bytes, too short to hold a header and a table of contents",
			ErrCorruptBlock, len(r.b))
	}
	if got := binary.BigEndian.Uint32(r.b[:4]); got != indexMagic {
		return fmt.Errorf("%w: index magic is %#08x, want %#08x", ErrCorruptBlock, got, indexMagic)
	}
	if r.b[4] != indexFormatV1 {
		return fmt.Errorf("block: index format %d, this build understands %d", r.b[4], indexFormatV1)
	}

	toc, err := r.readTOC()
	if err != nil {
		return err
	}
	if err := r.readSymbols(toc.Symbols); err != nil {
		return err
	}
	if err := r.readSeriesTable(toc.SeriesTable); err != nil {
		return err
	}
	if err := r.readPostingsTable(toc.PostingsTable); err != nil {
		return err
	}
	return r.readLabelIndex(toc.LabelIndex)
}

func (r *IndexReader) readTOC() (tableOfContents, error) {
	var toc tableOfContents

	d := encoding.NewDecbuf(r.b[len(r.b)-tocSize:])
	d.CheckCrc32(encoding.Castagnoli)
	toc.Symbols = d.Be64()
	toc.Series = d.Be64()
	toc.SeriesTable = d.Be64()
	toc.Postings = d.Be64()
	toc.PostingsTable = d.Be64()
	toc.LabelIndex = d.Be64()

	if err := d.Err(); err != nil {
		return toc, fmt.Errorf("%w: table of contents: %v", ErrCorruptBlock, err)
	}
	for _, off := range []uint64{toc.Symbols, toc.SeriesTable, toc.PostingsTable, toc.LabelIndex} {
		if off >= uint64(len(r.b)) {
			return toc, fmt.Errorf("%w: table of contents points to offset %d in a %d byte file",
				ErrCorruptBlock, off, len(r.b))
		}
	}
	return toc, nil
}

// section returns the bytes from off to the end of the file, for a decoder
// that will stop at its own length prefix.
func (r *IndexReader) section(off uint64) []byte { return r.b[off:] }

func (r *IndexReader) readSymbols(off uint64) error {
	d := encoding.NewDecbuf(r.section(off))
	n := d.Uvarint()
	if err := d.Err(); err != nil {
		return fmt.Errorf("%w: symbol count: %v", ErrCorruptBlock, err)
	}
	if n < 0 || n > d.Len() {
		return fmt.Errorf("%w: symbol table claims %d entries with %d bytes available",
			ErrCorruptBlock, n, d.Len())
	}

	r.symbols = make([]string, 0, n)
	for i := 0; i < n; i++ {
		// The strings alias the mapping, which stays valid for the reader's
		// lifetime. Copying a million symbols per open would be the single
		// largest cost of opening a block.
		r.symbols = append(r.symbols, d.UvarintStr())
	}
	if err := d.Err(); err != nil {
		return fmt.Errorf("%w: symbol table: %v", ErrCorruptBlock, err)
	}
	return nil
}

func (r *IndexReader) readSeriesTable(off uint64) error {
	d := encoding.NewDecbuf(r.section(off))
	n := d.Uvarint()
	if err := d.Err(); err != nil {
		return fmt.Errorf("%w: series count: %v", ErrCorruptBlock, err)
	}
	if n < 0 || n > d.Len() {
		return fmt.Errorf("%w: series table claims %d entries with %d bytes available",
			ErrCorruptBlock, n, d.Len())
	}

	r.seriesOffsets = make([]uint64, 0, n)
	var prev uint64
	for i := 0; i < n; i++ {
		prev += d.Uvarint64()
		if prev >= uint64(len(r.b)) {
			return fmt.Errorf("%w: series %d is at offset %d in a %d byte file",
				ErrCorruptBlock, i, prev, len(r.b))
		}
		r.seriesOffsets = append(r.seriesOffsets, prev)
	}
	if err := d.Err(); err != nil {
		return fmt.Errorf("%w: series table: %v", ErrCorruptBlock, err)
	}
	return nil
}

func (r *IndexReader) readPostingsTable(off uint64) error {
	d := encoding.NewDecbuf(r.section(off))
	n := d.Uvarint()
	if err := d.Err(); err != nil {
		return fmt.Errorf("%w: postings table count: %v", ErrCorruptBlock, err)
	}
	if n < 0 || n > d.Len() {
		return fmt.Errorf("%w: postings table claims %d entries with %d bytes available",
			ErrCorruptBlock, n, d.Len())
	}

	r.postingsOff = make(map[labelPair]uint64, n)
	for i := 0; i < n; i++ {
		name := d.UvarintStr()
		value := d.UvarintStr()
		o := d.Uvarint64()
		if d.Err() != nil {
			break
		}
		if o >= uint64(len(r.b)) {
			return fmt.Errorf("%w: postings for %s=%q are at offset %d in a %d byte file",
				ErrCorruptBlock, name, value, o, len(r.b))
		}
		r.postingsOff[labelPair{name, value}] = o
	}
	if err := d.Err(); err != nil {
		return fmt.Errorf("%w: postings table: %v", ErrCorruptBlock, err)
	}
	return nil
}

func (r *IndexReader) readLabelIndex(off uint64) error {
	d := encoding.NewDecbuf(r.section(off))
	n := d.Uvarint()
	if err := d.Err(); err != nil {
		return fmt.Errorf("%w: label index count: %v", ErrCorruptBlock, err)
	}
	if n < 0 || n > d.Len() {
		return fmt.Errorf("%w: label index claims %d names with %d bytes available",
			ErrCorruptBlock, n, d.Len())
	}

	r.labelIndex = make(map[string][]string, n)
	r.labelNamesAll = make([]string, 0, n)
	for i := 0; i < n; i++ {
		name := d.UvarintStr()
		vn := d.Uvarint()
		if d.Err() != nil {
			break
		}
		if vn < 0 || vn > d.Len() {
			return fmt.Errorf("%w: label %q claims %d values with %d bytes available",
				ErrCorruptBlock, name, vn, d.Len())
		}
		vals := make([]string, 0, vn)
		for j := 0; j < vn; j++ {
			vals = append(vals, d.UvarintStr())
		}
		r.labelIndex[name] = vals
		r.labelNamesAll = append(r.labelNamesAll, name)
	}
	if err := d.Err(); err != nil {
		return fmt.Errorf("%w: label index: %v", ErrCorruptBlock, err)
	}
	return nil
}

// NumSeries reports how many series the block holds.
func (r *IndexReader) NumSeries() int { return len(r.seriesOffsets) }

// Series decodes the labels and chunk metadata of one series.
func (r *IndexReader) Series(id model.SeriesRef) (model.Labels, []ChunkMeta, error) {
	if int(id) >= len(r.seriesOffsets) {
		return nil, nil, fmt.Errorf("%w: series %d of %d", ErrCorruptBlock, id, len(r.seriesOffsets))
	}

	d := encoding.NewDecbuf(r.section(r.seriesOffsets[id]))

	n := d.Uvarint()
	if d.Err() != nil || n < 0 || n > d.Len() {
		return nil, nil, fmt.Errorf("%w: series %d claims %d labels", ErrCorruptBlock, id, n)
	}
	ls := make(model.Labels, 0, n)
	for i := 0; i < n; i++ {
		ni, vi := d.Uvarint64(), d.Uvarint64()
		if d.Err() != nil {
			return nil, nil, fmt.Errorf("%w: series %d labels: %v", ErrCorruptBlock, id, d.Err())
		}
		if ni >= uint64(len(r.symbols)) || vi >= uint64(len(r.symbols)) {
			return nil, nil, fmt.Errorf("%w: series %d references symbol %d/%d of %d",
				ErrCorruptBlock, id, ni, vi, len(r.symbols))
		}
		ls = append(ls, model.Label{Name: r.symbols[ni], Value: r.symbols[vi]})
	}

	cn := d.Uvarint()
	if d.Err() != nil || cn < 0 || cn > d.Len() {
		return nil, nil, fmt.Errorf("%w: series %d claims %d chunks", ErrCorruptBlock, id, cn)
	}
	chunks := make([]ChunkMeta, 0, cn)
	var (
		prevRef uint64
		prevMax int64
	)
	for i := 0; i < cn; i++ {
		var (
			mint int64
			ref  uint64
		)
		if i == 0 {
			mint = d.Varint64()
			ref = d.Uvarint64()
		} else {
			mint = prevMax + d.Varint64()
			ref = prevRef + d.Uvarint64()
		}
		maxt := mint + int64(d.Uvarint64())
		if d.Err() != nil {
			return nil, nil, fmt.Errorf("%w: series %d chunks: %v", ErrCorruptBlock, id, d.Err())
		}
		chunks = append(chunks, ChunkMeta{Ref: ChunkRef(ref), MinTime: mint, MaxTime: maxt})
		prevRef, prevMax = ref, maxt
	}

	return ls, chunks, nil
}

// Postings returns the union of the postings for one label name across values.
func (r *IndexReader) Postings(name string, values ...string) index.Postings {
	switch len(values) {
	case 0:
		return index.EmptyPostings()
	case 1:
		return r.postingsFor(name, values[0])
	}
	its := make([]index.Postings, 0, len(values))
	for _, v := range values {
		p := r.postingsFor(name, v)
		if !index.IsEmpty(p) {
			its = append(its, p)
		}
	}
	return index.Merge(its...)
}

func (r *IndexReader) postingsFor(name, value string) index.Postings {
	off, ok := r.postingsOff[labelPair{name, value}]
	if !ok {
		return index.EmptyPostings()
	}

	rest := r.b[off:]
	if len(rest) < 4 {
		return index.ErrPostings(fmt.Errorf("%w: postings for %s=%q are truncated",
			ErrCorruptBlock, name, value))
	}
	n := binary.BigEndian.Uint32(rest[:4])
	need := int(n)*4 + 4 // entries plus the trailing checksum
	if len(rest) < 4+need {
		return index.ErrPostings(fmt.Errorf("%w: postings for %s=%q claim %d entries with %d bytes available",
			ErrCorruptBlock, name, value, n, len(rest)))
	}

	// The list is handed out as a view over the mapping rather than decoded.
	// A selective intersection reads only the entries it seeks to, so
	// materialising a million-entry list to answer a one-series query would
	// be the dominant cost of the query.
	return newBigEndianPostings(rest[4 : 4+int(n)*4])
}

// All returns the postings for every series in the block.
func (r *IndexReader) All() index.Postings {
	name, value := index.AllPostingsKey()
	return r.postingsFor(name, value)
}

// LabelValues returns the values seen for a label name, sorted.
func (r *IndexReader) LabelValues(name string) []string { return r.labelIndex[name] }

// LabelNames returns every label name in the block, sorted.
func (r *IndexReader) LabelNames() []string { return r.labelNamesAll }

// Symbols returns the block's symbol table.
func (r *IndexReader) Symbols() []string { return r.symbols }

// Close releases the mapping.
func (r *IndexReader) Close() error {
	r.b = nil
	return r.m.Close()
}

// bigEndianPostings iterates fixed-width big-endian series ids straight out of
// the mapping.
type bigEndianPostings struct {
	list []byte
	pos  int // byte index just past cur
	cur  model.SeriesRef
}

func newBigEndianPostings(list []byte) index.Postings {
	if len(list) == 0 {
		return index.EmptyPostings()
	}
	return &bigEndianPostings{list: list}
}

func (p *bigEndianPostings) At() model.SeriesRef { return p.cur }
func (p *bigEndianPostings) Err() error          { return nil }
func (p *bigEndianPostings) Len() int            { return len(p.list) / 4 }

func (p *bigEndianPostings) Next() bool {
	if p.pos >= len(p.list) {
		return false
	}
	p.cur = model.SeriesRef(binary.BigEndian.Uint32(p.list[p.pos:]))
	p.pos += 4
	return true
}

// Seek binary searches the remaining entries. Fixed-width encoding is what
// makes this possible: with varints the same operation would be a linear
// decode, and an intersection against a large list would lose its advantage.
func (p *bigEndianPostings) Seek(v model.SeriesRef) bool {
	if p.pos > 0 && p.cur >= v {
		return true
	}
	rest := p.list[p.pos:]
	n := len(rest) / 4
	i := sort.Search(n, func(i int) bool {
		return model.SeriesRef(binary.BigEndian.Uint32(rest[i*4:])) >= v
	})
	if i >= n {
		p.pos = len(p.list)
		return false
	}
	p.pos += i * 4
	p.cur = model.SeriesRef(binary.BigEndian.Uint32(p.list[p.pos:]))
	p.pos += 4
	return true
}
