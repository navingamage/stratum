//go:build unix

package block

import (
	"errors"
	"fmt"
	"os"
	"syscall"
)

// mmapFile maps a file read-only into memory.
//
// Blocks are read far more often than they are written and are never modified
// in place, which is exactly the case mapping is good at: the pages are shared
// with the page cache, so several open blocks cost no extra resident memory,
// and a query that touches one series in a large block faults in only the
// pages it reads rather than copying the whole file.
func mmapFile(f *os.File) ([]byte, error) {
	info, err := f.Stat()
	if err != nil {
		return nil, err
	}
	size := info.Size()
	if size == 0 {
		return nil, nil
	}
	if size != int64(int(size)) {
		return nil, fmt.Errorf("block: %s is too large to map on this platform", f.Name())
	}

	b, err := syscall.Mmap(int(f.Fd()), 0, int(size), syscall.PROT_READ, syscall.MAP_SHARED)
	if err != nil {
		return nil, fmt.Errorf("block: mapping %s: %w", f.Name(), err)
	}
	return b, nil
}

// munmapFile releases a mapping.
func munmapFile(b []byte) error {
	if len(b) == 0 {
		return nil
	}
	return syscall.Munmap(b)
}

// syncDir fsyncs a directory so that entries created or renamed in it survive
// a crash.
//
// This is the step people forget. A rename is only durable once the directory
// entry recording it is, so skipping this is the classic way to end up, after
// power loss, with a block whose files all exist but which is not linked into
// its parent directory.
//
// Some filesystems do not support syncing a directory at all and say so with
// EINVAL or ENOTSUP. On those the rename is already durable, so the error is
// not one.
func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()

	if err := d.Sync(); err != nil {
		if errors.Is(err, syscall.EINVAL) || errors.Is(err, syscall.ENOTSUP) {
			return nil
		}
		return err
	}
	return nil
}
