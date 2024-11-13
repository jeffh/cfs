package fs

import (
	"context"
	"io/fs"
	"iter"
	"os"

	ninep "github.com/jeffh/cfs/ninep"
)

// Returns a trace file system the wraps a given file system.
//
// The trace file system simply logs all file system operations are logged to
// the loggable.
//
// Supports also walkable file systems.
func TraceFs(fs ninep.FileSystem, l ninep.Loggable) ninep.FileSystem {
	if f, ok := fs.(ninep.WalkableFileSystem); ok {
		return &walkableTraceFileSystem{
			traceFileSystem{fs, l},
			f,
		}
	} else {
		return &traceFileSystem{fs, l}
	}
}

type traceFileHandle struct {
	H    ninep.FileHandle
	Path string
	ninep.Loggable
}

func (h *traceFileHandle) ReadAt(p []byte, offset int64) (int, error) {
	n, err := h.H.ReadAt(p, offset)
	if err != nil {
		h.Tracef("File(%v).ReadAt(_, %v) => (%d, %s)", h.Path, offset, n, err)
		h.Errorf("File(%v).ReadAt(_, %v) => (%d, %s)", h.Path, offset, n, err)
	} else {
		h.Tracef("File(%v).ReadAt(_, %v) => (%d, nil)", h.Path, offset, n)
	}
	return n, err
}

func (h *traceFileHandle) WriteAt(p []byte, offset int64) (int, error) {
	n, err := h.H.WriteAt(p, offset)
	if err != nil {
		h.Tracef("File(%v).WriteAt(len(%d), %v) => (%d, %s)", h.Path, len(p), offset, n, err)
		h.Errorf("File(%v).WriteAt(len(%d), %v) => (%d, %s)", h.Path, len(p), offset, n, err)
	} else {
		h.Tracef("File(%v).WriteAt(len(%d), %v) => (%d, nil)", h.Path, len(p), offset, n)
	}
	return n, err
}

func (h *traceFileHandle) Sync() error {
	err := h.H.Sync()
	if err != nil {
		h.Tracef("File(%v).Sync() => %s", h.Path, err)
		h.Errorf("File(%v).Sync() => %s", h.Path, err)
	} else {
		h.Tracef("File(%v).Sync() => nil", h.Path)
	}
	return err
}

func (h *traceFileHandle) Close() error {
	err := h.H.Close()
	if err != nil {
		h.Tracef("File(%v).Close() => %s", h.Path, err)
		h.Errorf("File(%v).Close() => %s", h.Path, err)
	} else {
		h.Tracef("File(%v).Close() => nil", h.Path)
	}
	return err
}

////////////////////

// A file system that wraps another file system, logging all the operations it receives.
type traceFileSystem struct {
	Fs ninep.FileSystem
	ninep.Loggable
}

func (f traceFileSystem) MakeDir(ctx context.Context, path string, mode ninep.Mode) error {
	err := f.Fs.MakeDir(ctx, path, mode)
	f.Tracef("FS.MakeDir(%v, %s) => %s", path, mode, err)
	if err != nil {
		f.Errorf("FS.MakeDir(%v, %s) => %s", path, mode, err)
	}
	return err
}

func (f traceFileSystem) CreateFile(ctx context.Context, path string, flag ninep.OpenMode, mode ninep.Mode) (ninep.FileHandle, error) {
	h, err := f.Fs.CreateFile(ctx, path, flag, mode)
	f.Tracef("FS.CreateFile(%v, %s, %s) => (%v, %s)", path, flag, mode, h, err)
	if err != nil || h == nil {
		f.Errorf("FS.CreateFile(%v, %s, %s) => (%v, %s)", path, flag, mode, h, err)
	}
	h = &traceFileHandle{
		H:        h,
		Path:     path,
		Loggable: f.Loggable,
	}
	return h, err
}

func (f traceFileSystem) OpenFile(ctx context.Context, path string, flag ninep.OpenMode) (ninep.FileHandle, error) {
	h, err := f.Fs.OpenFile(ctx, path, flag)
	f.Tracef("FS.OpenFile(%v, %s) => (%v, %s)", path, flag, h, err)
	if err != nil || h == nil {
		f.Errorf("FS.OpenFile(%v, %s) => (%v, %s)", path, flag, h, err)
	}
	h = &traceFileHandle{
		H:        h,
		Path:     path,
		Loggable: f.Loggable,
	}
	return h, err
}

func (f traceFileSystem) ListDir(ctx context.Context, path string) iter.Seq2[fs.FileInfo, error] {
	return func(yield func(fs.FileInfo, error) bool) {
		i := 0
		for info, err := range f.Fs.ListDir(ctx, path) {
			if err != nil {
				f.Errorf("FS.ListDir(%v)[%d] => (_, %s)", path, i, err)
			} else {
				f.Tracef("FS.ListDir(%v)[%d] => (%#v, %s)", path, i, info.Name(), err)
			}
			if !yield(info, err) {
				return
			}
			i++
		}
	}
}

func (f traceFileSystem) Stat(ctx context.Context, path string) (os.FileInfo, error) {
	info, err := f.Fs.Stat(ctx, path)
	if info != nil {
		f.Tracef("FS.Stat(%v) => (os.FileInfo{name: %#v, size: %d, isDir: %v...}, %s)", path, info.Name(), info.Size(), info.IsDir(), err)
	} else {
		if err != nil {
			f.Tracef("FS.Stat(%v) => (nil, %s)", path, err)
		} else {
			f.Tracef("FS.Stat(%v) => (nil, nil)", path)
		}
	}
	if err != nil {
		f.Errorf("FS.Stat(%v) => (_, %s)", path, err)
	}
	return info, err
}

func (f traceFileSystem) WriteStat(ctx context.Context, path string, s ninep.Stat) error {
	f.Tracef("FS.WriteStat(%v, %s)", path, s)
	err := f.Fs.WriteStat(ctx, path, s)
	if err != nil {
		f.Errorf("FS.WriteStat(%v, %s) => %s", path, s, err)
	}
	return err
}

func (f traceFileSystem) Delete(ctx context.Context, path string) error {
	f.Tracef("FS.Delete(%v)", path)
	err := f.Fs.Delete(ctx, path)
	if err != nil {
		f.Errorf("FS.Delete(%v) => %s", path, err)
	}
	return err
}

func (f traceFileSystem) DeleteWithMode(ctx context.Context, path string, mode ninep.Mode) error {
	if fs, ok := f.Fs.(ninep.DeleteWithModeFileSystem); ok {
		f.Tracef("FS.DeleteWithMode(%v, %s)", path, mode)
		err := fs.DeleteWithMode(ctx, path, mode)
		if err != nil {
			f.Errorf("FS.DeleteWithMode(%v, %s) => %s", path, mode, err)
		}
		return err
	} else {
		return f.Delete(ctx, path)
	}
}

////////////////////

type walkableTraceFileSystem struct {
	traceFileSystem
	Wfs ninep.WalkableFileSystem
}

var _ ninep.WalkableFileSystem = (*walkableTraceFileSystem)(nil)

func (f *walkableTraceFileSystem) Walk(ctx context.Context, parts []string) ([]os.FileInfo, error) {
	f.traceFileSystem.Tracef("FS.Walk(%v)", parts)
	infos, err := f.Wfs.Walk(ctx, parts)
	if err != nil {
		f.traceFileSystem.Errorf("FS.Walk(%v) => %s", parts, err)
	}
	return infos, err
}
