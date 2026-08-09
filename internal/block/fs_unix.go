//go:build unix

package block

import (
	"errors"
	"fmt"
	"os"
	"syscall"
)

// mappedFile is a read-only view of a file's contents.
//
// Blocks are read far more often than they are written and are never modified
// in place, which is exactly the case mapping is good at: the pages are shared
// with the page cache, so several open blocks cost no extra resident memory,
// and a query touching one series in a large block faults in only the pages it
// reads rather than copying the whole file.
type mappedFile struct {
	b []byte
}

// openMapped maps a file read-only into memory.
//
// The descriptor is closed before returning. A mapping does not depend on it -
// the kernel holds its own reference to the inode - and a database with
// thousands of blocks would otherwise hold two descriptors per block against
// RLIMIT_NOFILE for no benefit.
//
// Releasing the handle also keeps the storage lifecycle honest on platforms
// that do not share POSIX's willingness to unlink an open file: compaction
// deletes a block directory as soon as its output is published, and a stray
// open handle turns that into a failure rather than a no-op.
func openMapped(path string) (*mappedFile, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	info, err := f.Stat()
	if err != nil {
		return nil, err
	}
	size := info.Size()
	if size == 0 {
		return &mappedFile{}, nil
	}
	if size != int64(int(size)) {
		return nil, fmt.Errorf("block: %s is too large to map on this platform", path)
	}

	b, err := syscall.Mmap(int(f.Fd()), 0, int(size), syscall.PROT_READ, syscall.MAP_SHARED)
	if err != nil {
		return nil, fmt.Errorf("block: mapping %s: %w", path, err)
	}
	return &mappedFile{b: b}, nil
}

// Bytes returns the mapped contents. They stay valid until Close.
func (m *mappedFile) Bytes() []byte { return m.b }

// Close releases the mapping.
func (m *mappedFile) Close() error {
	b := m.b
	m.b = nil
	if len(b) == 0 {
		return nil
	}
	return syscall.Munmap(b)
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
		// Not every filesystem supports it; on those the rename is already
		// durable.
		if errors.Is(err, syscall.EINVAL) || errors.Is(err, syscall.ENOTSUP) {
			return nil
		}
		return err
	}
	return nil
}
