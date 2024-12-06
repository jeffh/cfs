package fs

import (
	"context"
	"io"

	ninep "github.com/jeffh/cfs/ninep"
)

// WithClose returns a wrapped file system that attaches the given closers to
// the Close() method of the filesystem. These methods are call prior to calling
// Close() on the underlying filesystem.
func WithClose(f ninep.FileSystem, closers ...context.CancelFunc) ninep.FileSystem {
	return &closerFs{f, closers}
}

type closerFs struct {
	ninep.FileSystem
	cancels []context.CancelFunc
}

func (fs *closerFs) Close() error {
	for _, closer := range fs.cancels {
		closer()
	}
	if closer, ok := fs.FileSystem.(io.Closer); ok {
		return closer.Close()
	}
	return nil
}
