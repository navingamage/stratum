// Package block implements the immutable on-disk blocks that hold everything
// older than the head.
//
// A block is a directory:
//
//	01J8ZQ.../
//	  meta.json   block bounds, statistics and compaction lineage
//	  chunks      concatenated compressed chunks
//	  index       symbol table, series, postings
//
// Blocks are written once and never modified. Compaction produces new blocks
// and deletes old ones; nothing is ever updated in place. That is what lets a
// query hold a block open with no locking at all, and what makes a crash
// mid-compaction recoverable by deleting whatever partial directory is left.
package block

import (
	"crypto/rand"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// MetaFilename is the name of the metadata file inside a block directory.
const MetaFilename = "meta.json"

// MetaVersion is the current metadata schema version.
const MetaVersion = 1

// Stats summarises a block's contents.
type Stats struct {
	NumSamples uint64 `json:"numSamples"`
	NumSeries  uint64 `json:"numSeries"`
	NumChunks  uint64 `json:"numChunks"`
}

// Compaction records how a block came to exist.
//
// The lineage is not decoration. When a compaction crashes half-way, the
// sources let the next startup tell "these inputs were already absorbed into
// that output" from "this output is partial and should be discarded" - the
// difference between silently losing data and cleanly retrying.
type Compaction struct {
	// Level is 1 for a block flushed straight from the head, and one more
	// than the highest input level for a compacted block.
	Level int `json:"level"`

	// Sources lists the level-1 blocks whose data ultimately reached here.
	Sources []ID `json:"sources,omitempty"`

	// Parents lists the blocks this one was compacted from directly.
	Parents []ID `json:"parents,omitempty"`
}

// Meta is the content of meta.json.
type Meta struct {
	Version int `json:"version"`

	ID      ID    `json:"id"`
	MinTime int64 `json:"minTime"`
	MaxTime int64 `json:"maxTime"`

	Stats      Stats      `json:"stats"`
	Compaction Compaction `json:"compaction"`

	// CreatedAt is when the block was written, for operator sanity rather
	// than for any decision the code makes.
	CreatedAt time.Time `json:"createdAt"`
}

// TimeRange returns the block's inclusive bounds.
func (m *Meta) TimeRange() (mint, maxt int64) { return m.MinTime, m.MaxTime }

// Overlaps reports whether the block can contribute to a query range.
func (m *Meta) Overlaps(mint, maxt int64) bool {
	return m.MinTime <= maxt && mint <= m.MaxTime
}

// ReadMeta loads the metadata from a block directory.
func ReadMeta(dir string) (*Meta, error) {
	b, err := os.ReadFile(filepath.Join(dir, MetaFilename))
	if err != nil {
		return nil, fmt.Errorf("block: reading metadata for %s: %w", dir, err)
	}
	var m Meta
	if err := json.Unmarshal(b, &m); err != nil {
		return nil, fmt.Errorf("block: parsing metadata for %s: %w", dir, err)
	}
	if m.Version != MetaVersion {
		return nil, fmt.Errorf("block: %s has metadata version %d, this build understands %d",
			dir, m.Version, MetaVersion)
	}
	return &m, nil
}

// WriteMeta writes the metadata into a block directory.
//
// The file is written to a temporary name and renamed into place, so a crash
// leaves either the previous contents or the new ones and never a half-parsed
// file. meta.json is what makes a directory a block, so it is written last
// during block creation and is the marker of a complete block.
func WriteMeta(dir string, m *Meta) error {
	m.Version = MetaVersion
	b, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return fmt.Errorf("block: encoding metadata: %w", err)
	}
	b = append(b, '\n')

	path := filepath.Join(dir, MetaFilename)
	tmp := path + ".tmp"

	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o666)
	if err != nil {
		return err
	}
	if _, err := f.Write(b); err != nil {
		f.Close()
		os.Remove(tmp)
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		os.Remove(tmp)
		return err
	}
	if err := f.Close(); err != nil {
		os.Remove(tmp)
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		return err
	}
	// Rename is only durable once the directory entry itself is synced.
	return syncDir(dir)
}

// syncDir fsyncs a directory so that entries created or renamed in it survive
// a crash. Skipping this is the classic way to end up with a file that exists
// but is not linked into its directory after power loss.
func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	if err := d.Sync(); err != nil {
		// Directory fsync is not supported everywhere; on those systems the
		// rename is already durable.
		if isNotSupported(err) {
			return nil
		}
		return err
	}
	return nil
}

// ID identifies a block. It is a 16-byte value rendered as 26 characters of
// Crockford base32: a 48-bit millisecond timestamp followed by 80 bits of
// randomness, in the manner of a ULID.
//
// The encoding is chosen so that lexical order is creation order. Listing a
// data directory therefore returns blocks oldest-first with no sorting and no
// metadata reads, which matters because compaction planning starts by doing
// exactly that.
type ID [16]byte

// crockford is the base32 alphabet, excluding I, L, O and U so that a block
// name read off a terminal cannot be mistranscribed.
const crockford = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"

var (
	idMu       sync.Mutex
	lastIDMs   uint64
	lastIDRand [10]byte
)

// NewID returns a fresh block ID.
//
// IDs generated within the same millisecond increment the random component
// rather than redrawing it, which keeps them strictly ordered. Two blocks
// created in the same millisecond with independent randomness would sort
// arbitrarily, and compaction would then process them out of order.
func NewID() (ID, error) {
	ms := uint64(time.Now().UnixMilli())

	idMu.Lock()
	defer idMu.Unlock()

	if ms == lastIDMs {
		// Increment the 80-bit random field as a big-endian integer.
		for i := len(lastIDRand) - 1; i >= 0; i-- {
			lastIDRand[i]++
			if lastIDRand[i] != 0 {
				break
			}
		}
	} else {
		if _, err := rand.Read(lastIDRand[:]); err != nil {
			return ID{}, fmt.Errorf("block: generating a block id: %w", err)
		}
		lastIDMs = ms
	}

	var id ID
	// 48-bit big-endian timestamp.
	binary.BigEndian.PutUint64(id[:8], ms<<16)
	copy(id[6:], lastIDRand[:])
	return id, nil
}

// String renders the ID in Crockford base32.
func (id ID) String() string {
	var out [26]byte
	// 26 characters of 5 bits covers 130 bits; the first character carries
	// only the top 3 bits of the 128.
	out[0] = crockford[(id[0]&224)>>5]
	out[1] = crockford[id[0]&31]

	// The remaining 15 bytes are read as a bit stream, five bits at a time.
	bitPos := 8
	for i := 2; i < 26; i++ {
		var v byte
		for b := 0; b < 5; b++ {
			byteIdx := bitPos >> 3
			shift := 7 - uint(bitPos&7)
			var bit byte
			if byteIdx < len(id) {
				bit = (id[byteIdx] >> shift) & 1
			}
			v = v<<1 | bit
			bitPos++
		}
		out[i] = crockford[v]
	}
	return string(out[:])
}

// MarshalText renders the ID for JSON.
func (id ID) MarshalText() ([]byte, error) { return []byte(id.String()), nil }

// UnmarshalText parses an ID from JSON.
func (id *ID) UnmarshalText(b []byte) error {
	parsed, err := ParseID(string(b))
	if err != nil {
		return err
	}
	*id = parsed
	return nil
}

// ParseID decodes an ID from its string form.
func ParseID(s string) (ID, error) {
	if len(s) != 26 {
		return ID{}, fmt.Errorf("block: id %q is %d characters, want 26", s, len(s))
	}
	var id ID

	dec := func(c byte) (byte, error) {
		i := strings.IndexByte(crockford, c)
		if i < 0 {
			return 0, fmt.Errorf("block: id contains %q, which is not a base32 character", c)
		}
		return byte(i), nil
	}

	hi0, err := dec(s[0])
	if err != nil {
		return ID{}, err
	}
	hi1, err := dec(s[1])
	if err != nil {
		return ID{}, err
	}
	id[0] = hi0<<5 | hi1

	bitPos := 8
	for i := 2; i < 26; i++ {
		v, err := dec(s[i])
		if err != nil {
			return ID{}, err
		}
		for b := 4; b >= 0; b-- {
			bit := (v >> uint(b)) & 1
			byteIdx := bitPos >> 3
			if byteIdx < len(id) {
				shift := 7 - uint(bitPos&7)
				id[byteIdx] |= bit << shift
			}
			bitPos++
		}
	}
	return id, nil
}

// Time returns the millisecond timestamp encoded in the ID.
func (id ID) Time() time.Time {
	ms := binary.BigEndian.Uint64(id[:8]) >> 16
	return time.UnixMilli(int64(ms)).UTC()
}

// Compare orders IDs, and therefore blocks, by creation time.
func (id ID) Compare(other ID) int {
	for i := range id {
		switch {
		case id[i] < other[i]:
			return -1
		case id[i] > other[i]:
			return 1
		}
	}
	return 0
}
