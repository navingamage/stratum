package block

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/navingamage/stratum/internal/chunk"
	"github.com/navingamage/stratum/internal/index"
	"github.com/navingamage/stratum/internal/model"
)

// TmpSuffix marks a block directory that is still being written. A directory
// carrying it is always safe to delete on startup: the block it would have
// become was never referenced by anything.
const TmpSuffix = ".tmp"

// Block is an open, immutable block.
type Block struct {
	dir  string
	meta Meta

	chunks *ChunkReader
	idx    *IndexReader
}

// Open opens the block in dir.
func Open(dir string) (*Block, error) {
	meta, err := ReadMeta(dir)
	if err != nil {
		return nil, err
	}

	idx, err := OpenIndexReader(dir)
	if err != nil {
		return nil, err
	}
	chunks, err := OpenChunkReader(dir)
	if err != nil {
		idx.Close()
		return nil, err
	}

	return &Block{dir: dir, meta: *meta, chunks: chunks, idx: idx}, nil
}

// Dir returns the block's directory.
func (b *Block) Dir() string { return b.dir }

// Meta returns the block's metadata.
func (b *Block) Meta() Meta { return b.meta }

// ID returns the block's identifier.
func (b *Block) ID() ID { return b.meta.ID }

// MinTime and MaxTime return the block's inclusive bounds.
func (b *Block) MinTime() int64 { return b.meta.MinTime }
func (b *Block) MaxTime() int64 { return b.meta.MaxTime }

// Overlaps reports whether the block can contribute to a query range.
func (b *Block) Overlaps(mint, maxt int64) bool { return b.meta.Overlaps(mint, maxt) }

// Index returns the block's index reader, which satisfies index.Reader.
func (b *Block) Index() *IndexReader { return b.idx }

// Chunks returns the block's chunk reader.
func (b *Block) Chunks() *ChunkReader { return b.chunks }

// Series returns the labels and chunk metadata for a series in this block.
func (b *Block) Series(id model.SeriesRef) (model.Labels, []ChunkMeta, error) {
	return b.idx.Series(id)
}

// SeriesChunks loads the chunks of a series overlapping [mint, maxt].
func (b *Block) SeriesChunks(id model.SeriesRef, mint, maxt int64) (model.Labels, []chunk.Chunk, error) {
	ls, metas, err := b.idx.Series(id)
	if err != nil {
		return nil, nil, err
	}

	var out []chunk.Chunk
	for _, m := range metas {
		// Chunk bounds come from the index, so an out-of-range chunk is
		// skipped without touching the chunk file at all.
		if m.MinTime > maxt || mint > m.MaxTime {
			continue
		}
		c, err := b.chunks.Chunk(m.Ref)
		if err != nil {
			return nil, nil, err
		}
		out = append(out, c)
	}
	return ls, out, nil
}

// Close releases the block's mappings.
func (b *Block) Close() error {
	var first error
	if err := b.chunks.Close(); err != nil {
		first = err
	}
	if err := b.idx.Close(); err != nil && first == nil {
		first = err
	}
	return first
}

// SeriesSource is what Write needs from whatever is producing a block: a way
// to enumerate series in label order, each with its chunks.
//
// It is an interface so that flushing the head and compacting several blocks
// go through exactly the same writer. A block produced by compaction is
// byte-identical in structure to one flushed from memory, which means there
// is only one format to test and only one reader path.
type SeriesSource interface {
	// Symbols returns every label name and value that will appear.
	Symbols() []string

	// Next advances to the next series, in ascending label order.
	Next() bool

	// At returns the current series.
	At() (model.Labels, []chunk.Chunk)

	// Err returns any error encountered.
	Err() error
}

// Write builds a block in parentDir from src and returns its metadata.
//
// The block is assembled in a directory with a .tmp suffix and renamed into
// place only once every file is written and synced. A crash therefore leaves
// either a complete block or a .tmp directory that startup deletes - never a
// block that looks valid and is missing half its chunks.
func Write(parentDir string, src SeriesSource, compaction Compaction) (*Meta, error) {
	id, err := NewID()
	if err != nil {
		return nil, err
	}

	finalDir := filepath.Join(parentDir, id.String())
	tmpDir := finalDir + TmpSuffix

	if err := os.RemoveAll(tmpDir); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(tmpDir, 0o777); err != nil {
		return nil, fmt.Errorf("block: creating %s: %w", tmpDir, err)
	}

	meta, err := writeInto(tmpDir, src, compaction, id)
	if err != nil {
		os.RemoveAll(tmpDir)
		return nil, err
	}

	// An empty block is not written at all. Leaving one behind would mean
	// compaction planning had to keep skipping it forever.
	if meta.Stats.NumSeries == 0 {
		os.RemoveAll(tmpDir)
		return nil, nil
	}

	if err := os.Rename(tmpDir, finalDir); err != nil {
		os.RemoveAll(tmpDir)
		return nil, fmt.Errorf("block: publishing %s: %w", finalDir, err)
	}
	if err := syncDir(parentDir); err != nil {
		return nil, err
	}
	return meta, nil
}

func writeInto(dir string, src SeriesSource, compaction Compaction, id ID) (*Meta, error) {
	cw, err := NewChunkWriter(dir)
	if err != nil {
		return nil, err
	}
	iw, err := NewIndexWriter(dir)
	if err != nil {
		cw.Close()
		return nil, err
	}

	if err := iw.AddSymbols(src.Symbols()); err != nil {
		cw.Close()
		iw.Close()
		return nil, err
	}

	meta := &Meta{
		ID:         id,
		MinTime:    model.MaxTime,
		MaxTime:    model.MinTime,
		Compaction: compaction,
		CreatedAt:  time.Now().UTC(),
	}

	var prev model.Labels
	for src.Next() {
		ls, chunks := src.At()

		// The writer's ordering contract is checked rather than trusted: the
		// reader binary searches series, so an unsorted block would fail as a
		// wrong answer rather than an error.
		if prev != nil && model.Compare(prev, ls) >= 0 {
			cw.Close()
			iw.Close()
			return nil, fmt.Errorf("block: series arrived out of order: %s then %s", prev, ls)
		}
		prev = ls

		metas := make([]ChunkMeta, 0, len(chunks))
		for _, c := range chunks {
			if c.NumSamples() == 0 {
				continue
			}
			mint, maxt, err := chunkBounds(c)
			if err != nil {
				cw.Close()
				iw.Close()
				return nil, err
			}
			ref, err := cw.WriteChunk(c)
			if err != nil {
				cw.Close()
				iw.Close()
				return nil, err
			}
			metas = append(metas, ChunkMeta{Ref: ref, MinTime: mint, MaxTime: maxt})

			meta.Stats.NumChunks++
			meta.Stats.NumSamples += uint64(c.NumSamples())
			if mint < meta.MinTime {
				meta.MinTime = mint
			}
			if maxt > meta.MaxTime {
				meta.MaxTime = maxt
			}
		}
		if len(metas) == 0 {
			continue
		}

		if err := iw.AddSeries(ls, metas); err != nil {
			cw.Close()
			iw.Close()
			return nil, err
		}
		meta.Stats.NumSeries++
	}
	if err := src.Err(); err != nil {
		cw.Close()
		iw.Close()
		return nil, fmt.Errorf("block: reading the series source: %w", err)
	}

	if err := cw.Close(); err != nil {
		iw.Close()
		return nil, err
	}
	if err := iw.Close(); err != nil {
		return nil, err
	}

	if meta.Stats.NumSeries == 0 {
		return meta, nil
	}

	// meta.json goes last. Its presence is what makes the directory a block,
	// so writing it earlier would create a window where a partial block looks
	// complete.
	if err := WriteMeta(dir, meta); err != nil {
		return nil, err
	}
	return meta, nil
}

// chunkBounds reads the first and last timestamps of a chunk.
func chunkBounds(c chunk.Chunk) (mint, maxt int64, err error) {
	it := c.Iterator(nil)
	if !it.Next() {
		if err := it.Err(); err != nil {
			return 0, 0, err
		}
		return 0, 0, errors.New("block: chunk reports samples but yields none")
	}
	mint, _ = it.At()
	maxt = mint
	for it.Next() {
		maxt, _ = it.At()
	}
	if err := it.Err(); err != nil {
		return 0, 0, err
	}
	return mint, maxt, nil
}

// List returns the blocks in dir, oldest first, skipping anything that is not
// a complete block.
//
// IDs sort in creation order, so the directory listing is already in the right
// order and no metadata has to be read to establish it.
func List(dir string) ([]string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}

	var out []string
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		name := e.Name()
		if filepath.Ext(name) == TmpSuffix {
			continue
		}
		if _, err := ParseID(name); err != nil {
			continue
		}
		// A directory without meta.json is a block whose write did not finish.
		if _, err := os.Stat(filepath.Join(dir, name, MetaFilename)); err != nil {
			continue
		}
		out = append(out, filepath.Join(dir, name))
	}
	sort.Strings(out)
	return out, nil
}

// CleanTmpDirs removes leftover partial blocks. Startup calls this before
// opening anything.
func CleanTmpDirs(dir string) error {
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	for _, e := range entries {
		if e.IsDir() && filepath.Ext(e.Name()) == TmpSuffix {
			if err := os.RemoveAll(filepath.Join(dir, e.Name())); err != nil {
				return err
			}
		}
	}
	return nil
}

// Delete removes a block directory.
func Delete(dir string) error {
	// Renaming to .tmp first makes the removal atomic from a reader's point of
	// view: the block either exists or does not, and a crash part-way through
	// leaves a directory that CleanTmpDirs will finish off.
	tmp := dir + TmpSuffix
	if err := os.Rename(dir, tmp); err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	if err := os.RemoveAll(tmp); err != nil {
		return err
	}
	return syncDir(filepath.Dir(dir))
}

// Ensure the index reader satisfies the shared index interface, so that
// matcher resolution against a block and against the head is the same code.
var _ index.Reader = (*IndexReader)(nil)
