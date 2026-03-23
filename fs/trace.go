package fs

import (
	"context"
	"io/fs"
	"iter"
	"log/slog"
	"os"

	ninep "github.com/jeffh/cfs/ninep"
)

// Returns a trace file system the wraps a given file system.
//
// The trace file system simply logs all file system operations are logged to
// the loggable.
//
// Supports also walkable file systems.
func TraceFs(fs ninep.FileSystem, l *slog.Logger) ninep.FileSystem {
	if f, ok := fs.(ninep.WalkableFileSystem); ok {
		return &walkableTraceFileSystem{
			traceFileSystem{fs, l},
			f,
		}
	} else {
		return &traceFileSystem{fs, l}
	}
}

// traceLog is a helper that logs begin/result for an operation.
func traceLog(l *slog.Logger, op string, err error, attrs ...slog.Attr) {
	if l == nil {
		return
	}
	if err != nil {
		attrs = append(attrs, slog.String("err", err.Error()))
		l.LogAttrs(context.Background(), slog.LevelError, op, attrs...)
	} else {
		l.LogAttrs(context.Background(), slog.LevelInfo, op, attrs...)
	}
}

func traceBegin(l *slog.Logger, op string, attrs ...slog.Attr) {
	if l != nil {
		l.LogAttrs(context.Background(), slog.LevelDebug, op+".begin", attrs...)
	}
}

type traceFileHandle struct {
	H      ninep.FileHandle
	Path   string
	Logger *slog.Logger
}

func (h *traceFileHandle) ReadAt(p []byte, offset int64) (int, error) {
	attrs := []slog.Attr{slog.String("path", h.Path), slog.Int64("offset", offset)}
	traceBegin(h.Logger, "FileHandle.ReadAt", attrs...)
	n, err := h.H.ReadAt(p, offset)
	traceLog(h.Logger, "FileHandle.ReadAt", err, append(attrs, slog.Int("n", n))...)
	return n, err
}

func (h *traceFileHandle) WriteAt(p []byte, offset int64) (int, error) {
	attrs := []slog.Attr{slog.String("path", h.Path), slog.Int64("offset", offset)}
	traceBegin(h.Logger, "FileHandle.WriteAt", attrs...)
	n, err := h.H.WriteAt(p, offset)
	traceLog(h.Logger, "FileHandle.WriteAt", err, append(attrs, slog.Int("n", n))...)
	return n, err
}

func (h *traceFileHandle) Sync() error {
	traceBegin(h.Logger, "FileHandle.Sync", slog.String("path", h.Path))
	err := h.H.Sync()
	traceLog(h.Logger, "FileHandle.Sync", err, slog.String("path", h.Path))
	return err
}

func (h *traceFileHandle) Close() error {
	traceBegin(h.Logger, "FileHandle.Close", slog.String("path", h.Path))
	err := h.H.Close()
	traceLog(h.Logger, "FileHandle.Close", err, slog.String("path", h.Path))
	return err
}

////////////////////

// A file system that wraps another file system, logging all the operations it receives.
type traceFileSystem struct {
	Fs     ninep.FileSystem
	Logger *slog.Logger
}

func (f traceFileSystem) MakeDir(ctx context.Context, path string, mode ninep.Mode) error {
	attrs := []slog.Attr{slog.String("path", path), slog.String("mode", mode.String())}
	traceBegin(f.Logger, "FS.MakeDir", attrs...)
	err := f.Fs.MakeDir(ctx, path, mode)
	traceLog(f.Logger, "FS.MakeDir", err, attrs...)
	return err
}

func (f traceFileSystem) CreateFile(ctx context.Context, path string, flag ninep.OpenMode, mode ninep.Mode) (ninep.FileHandle, error) {
	attrs := []slog.Attr{slog.String("path", path), slog.String("flag", flag.String()), slog.String("mode", mode.String())}
	traceBegin(f.Logger, "FS.CreateFile", attrs...)
	h, err := f.Fs.CreateFile(ctx, path, flag, mode)
	if err != nil {
		traceLog(f.Logger, "FS.CreateFile", err, attrs...)
	} else if h == nil && f.Logger != nil {
		f.Logger.LogAttrs(ctx, slog.LevelError, "FS.CreateFile", append(attrs, slog.String("err", "returned handle is nil with no error"))...)
	}
	return &traceFileHandle{H: h, Path: path, Logger: f.Logger}, err
}

func (f traceFileSystem) OpenFile(ctx context.Context, path string, flag ninep.OpenMode) (ninep.FileHandle, error) {
	attrs := []slog.Attr{slog.String("path", path), slog.String("flag", flag.String())}
	traceBegin(f.Logger, "FS.OpenFile", attrs...)
	h, err := f.Fs.OpenFile(ctx, path, flag)
	if err != nil {
		traceLog(f.Logger, "FS.OpenFile", err, attrs...)
	} else if h == nil && f.Logger != nil {
		f.Logger.LogAttrs(ctx, slog.LevelError, "FS.OpenFile", append(attrs, slog.String("err", "returned handle is nil with no error"))...)
	}
	return &traceFileHandle{H: h, Path: path, Logger: f.Logger}, err
}

func (f traceFileSystem) ListDir(ctx context.Context, path string) iter.Seq2[fs.FileInfo, error] {
	traceBegin(f.Logger, "FS.ListDir", slog.String("path", path))
	return func(yield func(fs.FileInfo, error) bool) {
		i := 0
		for info, err := range f.Fs.ListDir(ctx, path) {
			if f.Logger != nil {
				if err != nil {
					f.Logger.Error("FS.ListDir.returnItem", slog.String("path", path), slog.Int("i", i), slog.String("err", err.Error()))
				} else if info != nil {
					f.Logger.Info("FS.ListDir.returnItem", slog.String("path", path), slog.Int("i", i), slog.String("name", info.Name()))
				} else {
					f.Logger.Error("FS.ListDir.returnItem", slog.String("path", path), slog.Int("i", i), slog.String("err", "returned info is nil with no error"))
				}
			}
			if !yield(info, err) {
				if f.Logger != nil {
					f.Logger.Info("FS.ListDir.end.break", slog.String("path", path))
				}
				return
			}
			i++
		}
		if f.Logger != nil {
			f.Logger.Info("FS.ListDir.end.finished", slog.String("path", path))
		}
	}
}

func (f traceFileSystem) Stat(ctx context.Context, path string) (os.FileInfo, error) {
	traceBegin(f.Logger, "FS.Stat", slog.String("path", path))
	info, err := f.Fs.Stat(ctx, path)
	if f.Logger != nil {
		if err != nil {
			f.Logger.Error("FS.Stat", slog.String("path", path), slog.String("err", err.Error()))
		} else if info == nil {
			f.Logger.Error("FS.Stat", slog.String("path", path), slog.String("err", "returned info is nil with no error"))
		} else {
			f.Logger.Info("FS.Stat", slog.String("path", path), slog.String("name", info.Name()), slog.Int64("size", info.Size()), slog.Bool("isDir", info.IsDir()))
		}
	}
	return info, err
}

func (f traceFileSystem) WriteStat(ctx context.Context, path string, s ninep.Stat) error {
	attrs := []slog.Attr{slog.String("path", path), slog.String("stat", s.String())}
	traceBegin(f.Logger, "FS.WriteStat", attrs...)
	err := f.Fs.WriteStat(ctx, path, s)
	if err != nil {
		traceLog(f.Logger, "FS.WriteStat", err, attrs...)
	}
	return err
}

func (f traceFileSystem) Delete(ctx context.Context, path string) error {
	traceBegin(f.Logger, "FS.Delete", slog.String("path", path))
	err := f.Fs.Delete(ctx, path)
	if err != nil {
		traceLog(f.Logger, "FS.Delete", err, slog.String("path", path))
	}
	return err
}

func (f traceFileSystem) DeleteWithMode(ctx context.Context, path string, mode ninep.Mode) error {
	attrs := []slog.Attr{slog.String("path", path), slog.String("mode", mode.String())}
	traceBegin(f.Logger, "FS.DeleteWithMode", attrs...)
	var err error
	if dfs, ok := f.Fs.(ninep.DeleteWithModeFileSystem); ok {
		err = dfs.DeleteWithMode(ctx, path, mode)
	} else {
		err = f.Delete(ctx, path)
	}
	traceLog(f.Logger, "FS.DeleteWithMode", err, attrs...)
	return err
}

////////////////////

type walkableTraceFileSystem struct {
	traceFileSystem
	Wfs ninep.WalkableFileSystem
}

var _ ninep.WalkableFileSystem = (*walkableTraceFileSystem)(nil)

func (f *walkableTraceFileSystem) Walk(ctx context.Context, parts []string) ([]os.FileInfo, error) {
	traceBegin(f.Logger, "FS.Walk", slog.Any("parts", parts))
	infos, err := f.Wfs.Walk(ctx, parts)
	if f.Logger != nil {
		if err != nil {
			f.Logger.Error("FS.Walk", slog.Any("parts", parts), slog.String("err", err.Error()))
		} else if len(infos) > 0 {
			for i, info := range infos {
				f.Logger.Info("FS.Walk.returnItem", slog.Any("parts", parts[:i+1]), slog.Int("i", i), slog.String("name", info.Name()))
			}
		} else {
			f.Logger.Info("FS.Walk.returnItem", slog.Any("parts", parts))
		}
	}
	return infos, err
}
