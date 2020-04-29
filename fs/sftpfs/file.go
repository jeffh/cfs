package sftpfs

import (
	"io"
	"sync"

	"github.com/jeffh/cfs/ninep"
	"github.com/pkg/sftp"
)

type File struct {
	m sync.Mutex
	h *sftp.File
}

var _ ninep.FileHandle = (*File)(nil)

func (f *File) ReadAt(p []byte, off int64) (int, error) {
	f.m.Lock()
	defer f.m.Unlock()
	if _, err := f.h.Seek(off, io.SeekStart); err != nil {
		return 0, err
	}

	return f.h.Read(p)
}
func (f *File) WriteAt(p []byte, off int64) (int, error) {
	f.m.Lock()
	defer f.m.Unlock()
	if _, err := f.h.Seek(off, io.SeekStart); err != nil {
		return 0, err
	}

	return f.h.Write(p)
}
func (f *File) Close() error {
	f.m.Lock()
	defer f.m.Unlock()
	return f.h.Close()
}
func (f *File) Sync() error { return nil }
