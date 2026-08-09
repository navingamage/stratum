//go:build !unix

package block

import (
	"fmt"
	"io"
	"os"
)

// mmapFile reads the whole file into memory.
//
// The fallback for platforms without the unix mmap syscalls. It is correct
// but costs resident memory proportional to the open blocks rather than
// sharing pages with the page cache, so a process holding many large blocks
// open will use noticeably more here than on unix.
func mmapFile(f *os.File) ([]byte, error) {
	info, err := f.Stat()
	if err != nil {
		return nil, err
	}
	if info.Size() == 0 {
		return nil, nil
	}

	b := make([]byte, info.Size())
	if _, err := io.ReadFull(f, b); err != nil {
		return nil, fmt.Errorf("block: reading %s: %w", f.Name(), err)
	}
	return b, nil
}

// munmapFile releases the buffer. Garbage collection does the real work.
func munmapFile([]byte) error { return nil }

// isNotSupported reports whether an error means the operation is unavailable
// on this platform.
func isNotSupported(error) bool { return false }
