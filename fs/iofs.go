package fs

import (
	"context"
	"io"
	"io/fs"

	ninep "github.com/jeffh/cfs/ninep"
)

// ReadOnlyFS wraps an fs.FS and returns a read-only version of it for use with ninep.
func ReadOnlyFS(f fs.FS) ninep.FileSystem {
	return readOnlyFS{f}
}

type readOnlyFS struct {
	Underlying fs.FS
}

type fsFileHandle struct {
	handle fs.File
	offset int64
}

func (h *fsFileHandle) ReadAt(p []byte, off int64) (int, error) {
	if r, ok := h.handle.(io.ReaderAt); ok {
		return r.ReadAt(p, off)
	} else if seeker, ok := h.handle.(io.Seeker); ok {
		n, err := seeker.Seek(off, io.SeekStart)
		if err != nil {
			return 0, err
		}
		h.offset = n
		return int(n), err
	} else {
		if off == h.offset {
			n, err := h.handle.Read(p)
			h.offset += int64(n)
			if err != nil {
				return n, err
			}
			return n, err
		} else {
			return 0, ninep.ErrSeekNotAllowed
		}
	}
}
func (h *fsFileHandle) WriteAt(p []byte, off int64) (int, error) {
	return 0, ninep.ErrWriteNotAllowed
}
func (h *fsFileHandle) Close() error {
	return h.handle.Close()
}
func (h *fsFileHandle) Sync() error { return nil }

func (r readOnlyFS) MakeDir(ctx context.Context, path string, mode ninep.Mode) error {
	return ninep.ErrWriteNotAllowed
}

func (r readOnlyFS) CreateFile(ctx context.Context, path string, flag ninep.OpenMode, mode ninep.Mode) (ninep.FileHandle, error) {
	return nil, ninep.ErrWriteNotAllowed
}

func (r readOnlyFS) OpenFile(ctx context.Context, path string, flag ninep.OpenMode) (ninep.FileHandle, error) {
	if flag.RequestsMutation() {
		return nil, ninep.ErrWriteNotAllowed
	}
	h, err := r.Underlying.Open(path)
	if err != nil {
		return nil, err
	}
	return &fsFileHandle{handle: h}, nil
}

func (r readOnlyFS) ListDir(ctx context.Context, path string) (ninep.FileInfoIterator, error) {
	entries, err := fs.ReadDir(r.Underlying, path)
	if err != nil {
		return nil, err
	}
	infos := make([]fs.FileInfo, 0, len(entries))
	for _, entry := range entries {
		info, err := entry.Info()
		if err != nil {
			return nil, err
		}
		infos = append(infos, info)
	}
	return ninep.FileInfoSliceIterator(infos), nil
}

func (r readOnlyFS) Stat(ctx context.Context, path string) (fs.FileInfo, error) {
	return fs.Stat(r.Underlying, path)
}

func (r readOnlyFS) WriteStat(ctx context.Context, path string, stat ninep.Stat) error {
	return ninep.ErrWriteNotAllowed
}
func (r readOnlyFS) Delete(ctx context.Context, path string) error { return ninep.ErrWriteNotAllowed }
