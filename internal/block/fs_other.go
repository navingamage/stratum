//go:build !unix

package block

import (
	"fmt"
	"io"
	"os"
)

// mappedFile holds a file's contents in memory.
//
// The fallback for platforms without the unix mmap syscalls. It is correct but
// costs resident memory proportional to the open blocks rather than sharing
// pages with the page cache, so a process holding many large blocks open will
// use noticeably more here than on unix.
type mappedFile struct {
	b []byte
}

// openMapped reads the whole file into memory and closes the handle.
//
// Closing it is not a tidiness detail on Windows, it is the difference between
// a working storage engine and a broken one. POSIX lets a file be unlinked
// while it is still open; Windows refuses to rename or delete a directory that
// contains an open file. Compaction deletes a block's directory as soon as its
// replacement is published, so any handle still held there fails the delete
// with "Access is denied" - which is exactly how the Windows CI job found this.
//
// Since the contents are already copied out, there is nothing the handle is
// still needed for.
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
	if info.Size() == 0 {
		return &mappedFile{}, nil
	}

	b := make([]byte, info.Size())
	if _, err := io.ReadFull(f, b); err != nil {
		return nil, fmt.Errorf("block: reading %s: %w", path, err)
	}
	return &mappedFile{b: b}, nil
}

// Bytes returns the file's contents.
func (m *mappedFile) Bytes() []byte { return m.b }

// Close drops the buffer. Garbage collection does the real work.
func (m *mappedFile) Close() error {
	m.b = nil
	return nil
}

// syncDir is a no-op away from unix.
//
// Windows has no way to fsync a directory: opening one and calling Sync on the
// handle fails outright with ERROR_ACCESS_DENIED. It also does not need one in
// the same way - MoveFileEx orders the metadata update itself, so a completed
// rename is already recorded.
//
// This was not a guess. The Windows CI job, which exists to exercise the
// read-into-memory fallback above, failed every block test with "Access is
// denied" until directory syncing was confined to the platforms that have it.
func syncDir(string) error { return nil }
