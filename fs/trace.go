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

type traceFileHandle struct {
	H      ninep.FileHandle
	Path   string
	Logger *slog.Logger
}

func (h *traceFileHandle) ReadAt(p []byte, offset int64) (int, error) {
	if h.Logger != nil {
		h.Logger.Debug(
			"FileHandle.ReadAt.begin",
			slog.String("path", h.Path),
			slog.Int64("offset", offset),
		)
	}
	n, err := h.H.ReadAt(p, offset)
	if err != nil {
		if h.Logger != nil {
			h.Logger.Error(
				"FileHandle.ReadAt",
				slog.String("path", h.Path),
				slog.Int64("offset", offset),
				slog.Int("n", n),
				slog.String("err", err.Error()),
			)
		}
	} else {
		if h.Logger != nil {
			h.Logger.Info(
				"FileHandle.ReadAt",
				slog.String("path", h.Path),
				slog.Int64("offset", offset),
				slog.Int("n", n),
			)
		}
	}
	return n, err
}

func (h *traceFileHandle) WriteAt(p []byte, offset int64) (int, error) {
	if h.Logger != nil {
		h.Logger.Debug(
			"FileHandle.WriteAt.begin",
			slog.String("path", h.Path),
			slog.Int64("offset", offset),
		)
	}
	n, err := h.H.WriteAt(p, offset)
	if err != nil {
		if h.Logger != nil {
			h.Logger.Error(
				"FileHandle.WriteAt",
				slog.String("path", h.Path),
				slog.Int64("offset", offset),
				slog.Int("n", n),
				slog.String("err", err.Error()),
			)
		}
	} else {
		if h.Logger != nil {
			h.Logger.Info(
				"FileHandle.WriteAt",
				slog.String("path", h.Path),
				slog.Int64("offset", offset),
				slog.Int("n", n),
			)
		}
	}
	return n, err
}

func (h *traceFileHandle) Sync() error {
	if h.Logger != nil {
		h.Logger.Debug("FileHandle.Sync.begin", slog.String("path", h.Path))
	}
	err := h.H.Sync()
	if err != nil {
		if h.Logger != nil {
			h.Logger.Error(
				"FileHandle.Sync",
				slog.String("path", h.Path),
				slog.String("err", err.Error()),
			)
		}
	} else {
		if h.Logger != nil {
			h.Logger.Info("FileHandle.Sync", slog.String("path", h.Path))
		}
	}
	return err
}

func (h *traceFileHandle) Close() error {
	if h.Logger != nil {
		h.Logger.Debug("FileHandle.Close.begin", slog.String("path", h.Path))
	}
	err := h.H.Close()
	if err != nil {
		if h.Logger != nil {
			h.Logger.Error(
				"FileHandle.Close",
				slog.String("path", h.Path),
				slog.String("err", err.Error()),
			)
		}
	} else {
		if h.Logger != nil {
			h.Logger.Info("FileHandle.Close", slog.String("path", h.Path))
		}
	}
	return err
}

////////////////////

// A file system that wraps another file system, logging all the operations it receives.
type traceFileSystem struct {
	Fs     ninep.FileSystem
	Logger *slog.Logger
}

func (f traceFileSystem) MakeDir(ctx context.Context, path string, mode ninep.Mode) error {
	if f.Logger != nil {
		f.Logger.Debug("FS.MakeDir.begin", slog.String("path", path), slog.String("mode", mode.String()))
	}
	err := f.Fs.MakeDir(ctx, path, mode)
	if err != nil {
		if f.Logger != nil {
			f.Logger.Error(
				"FS.MakeDir",
				slog.String("path", path),
				slog.String("mode", mode.String()),
				slog.String("err", err.Error()),
			)
		}
	} else {
		if f.Logger != nil {
			f.Logger.Info(
				"FS.MakeDir",
				slog.String("path", path),
				slog.String("mode", mode.String()),
			)
		}
	}
	return err
}

func (f traceFileSystem) CreateFile(ctx context.Context, path string, flag ninep.OpenMode, mode ninep.Mode) (ninep.FileHandle, error) {
	if f.Logger != nil {
		f.Logger.Debug("FS.CreateFile.begin", slog.String("path", path), slog.String("flag", flag.String()), slog.String("mode", mode.String()))
	}
	h, err := f.Fs.CreateFile(ctx, path, flag, mode)
	if err != nil {
		if f.Logger != nil {
			f.Logger.Error(
				"FS.CreateFile",
				slog.String("path", path),
				slog.String("flag", flag.String()),
				slog.String("mode", mode.String()),
				slog.String("err", err.Error()),
			)
		}
	} else if h == nil {
		if f.Logger != nil {
			f.Logger.Error(
				"FS.CreateFile",
				slog.String("path", path),
				slog.String("flag", flag.String()),
				slog.String("mode", mode.String()),
				slog.String("err", "returned handle is nil with no error"),
			)
		}
	}
	h = &traceFileHandle{
		H:      h,
		Path:   path,
		Logger: f.Logger,
	}
	return h, err
}

func (f traceFileSystem) OpenFile(ctx context.Context, path string, flag ninep.OpenMode) (ninep.FileHandle, error) {
	if f.Logger != nil {
		f.Logger.Debug("FS.OpenFile.begin", slog.String("path", path), slog.String("flag", flag.String()))
	}
	h, err := f.Fs.OpenFile(ctx, path, flag)
	if err != nil {
		if f.Logger != nil {
			f.Logger.Error(
				"FS.OpenFile",
				slog.String("path", path),
				slog.String("flag", flag.String()),
				slog.String("err", err.Error()),
			)
		}
	} else if h == nil {
		if f.Logger != nil {
			f.Logger.Error(
				"FS.OpenFile",
				slog.String("path", path),
				slog.String("flag", flag.String()),
				slog.String("err", "returned handle is nil with no error"),
			)
		}
	}
	h = &traceFileHandle{
		H:      h,
		Path:   path,
		Logger: f.Logger,
	}
	return h, err
}

func (f traceFileSystem) ListDir(ctx context.Context, path string) iter.Seq2[fs.FileInfo, error] {
	if f.Logger != nil {
		f.Logger.Debug("FS.ListDir.begin", slog.String("path", path))
	}
	return func(yield func(fs.FileInfo, error) bool) {
		i := 0
		for info, err := range f.Fs.ListDir(ctx, path) {
			if err != nil {
				if f.Logger != nil {
					f.Logger.Error(
						"FS.ListDir.returnItem",
						slog.String("path", path),
						slog.Int("i", i),
						slog.String("err", err.Error()),
					)
				}
			} else {
				if info != nil {
					if f.Logger != nil {
						f.Logger.Info(
							"FS.ListDir.returnItem",
							slog.String("path", path),
							slog.Int("i", i),
							slog.String("name", info.Name()),
						)
					}
				} else {
					if f.Logger != nil {
						f.Logger.Error(
							"FS.ListDir.returnItem",
							slog.String("path", path),
							slog.Int("i", i),
							slog.String("err", "returned info is nil with no error"),
						)
					}
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
	if f.Logger != nil {
		f.Logger.Debug("FS.Stat.begin", slog.String("path", path))
	}
	info, err := f.Fs.Stat(ctx, path)
	if f.Logger != nil {
		if err != nil {
			f.Logger.Error(
				"FS.Stat",
				slog.String("path", path),
				slog.String("err", err.Error()),
			)
		} else if info == nil {
			f.Logger.Error(
				"FS.Stat",
				slog.String("path", path),
				slog.String("err", "returned info is nil with no error"),
			)
		} else {
			f.Logger.Info(
				"FS.Stat",
				slog.String("path", path),
				slog.String("name", info.Name()),
				slog.Int64("size", info.Size()),
				slog.Bool("isDir", info.IsDir()),
			)
		}
	}
	return info, err
}

func (f traceFileSystem) WriteStat(ctx context.Context, path string, s ninep.Stat) error {
	if f.Logger != nil {
		f.Logger.Debug("FS.WriteStat.begin", slog.String("path", path), slog.String("stat", s.String()))
	}
	err := f.Fs.WriteStat(ctx, path, s)
	if f.Logger != nil {
		if err != nil {
			f.Logger.Error(
				"FS.WriteStat",
				slog.String("path", path),
				slog.String("stat", s.String()),
				slog.String("err", err.Error()),
			)
		}
	}
	return err
}

func (f traceFileSystem) Delete(ctx context.Context, path string) error {
	if f.Logger != nil {
		f.Logger.Debug("FS.Delete.begin", slog.String("path", path))
	}
	err := f.Fs.Delete(ctx, path)
	if err != nil {
		if f.Logger != nil {
			f.Logger.Error(
				"FS.Delete",
				slog.String("path", path),
				slog.String("err", err.Error()),
			)
		}
	}
	return err
}

func (f traceFileSystem) DeleteWithMode(ctx context.Context, path string, mode ninep.Mode) error {
	if f.Logger != nil {
		f.Logger.Debug("FS.DeleteWithMode.begin", slog.String("path", path), slog.String("mode", mode.String()))
	}
	if fs, ok := f.Fs.(ninep.DeleteWithModeFileSystem); ok {
		err := fs.DeleteWithMode(ctx, path, mode)
		if f.Logger != nil {
			if err != nil {
				f.Logger.Error(
					"FS.DeleteWithMode",
					slog.String("path", path),
					slog.String("mode", mode.String()),
					slog.String("err", err.Error()),
				)
			} else {
				f.Logger.Info(
					"FS.DeleteWithMode",
					slog.String("path", path),
					slog.String("mode", mode.String()),
				)
			}
		}
		return err
	} else {
		err := f.Delete(ctx, path)
		if f.Logger != nil {
			if err != nil {
				f.Logger.Error(
					"FS.DeleteWithMode",
					slog.String("path", path),
					slog.String("mode", mode.String()),
					slog.String("err", err.Error()),
				)
			} else {
				f.Logger.Info(
					"FS.DeleteWithMode",
					slog.String("path", path),
					slog.String("mode", mode.String()),
				)
			}
		}
		return err
	}
}

////////////////////

type walkableTraceFileSystem struct {
	traceFileSystem
	Wfs ninep.WalkableFileSystem
}

var _ ninep.WalkableFileSystem = (*walkableTraceFileSystem)(nil)

func (f *walkableTraceFileSystem) Walk(ctx context.Context, parts []string) ([]os.FileInfo, error) {
	if f.Logger != nil {
		f.Logger.Debug("FS.Walk.begin", slog.Any("parts", parts))
	}
	infos, err := f.Wfs.Walk(ctx, parts)
	if f.Logger != nil {
		if err != nil {
			f.Logger.Error(
				"FS.Walk",
				slog.Any("parts", parts),
				slog.String("err", err.Error()),
			)
		} else {
			if len(infos) > 0 {
				for i, info := range infos {
					f.Logger.Info(
						"FS.Walk.returnItem",
						slog.Any("parts", parts[:i+1]),
						slog.Int("i", i),
						slog.String("name", info.Name()),
					)
				}
			} else {
				f.Logger.Info("FS.Walk.returnItem", slog.Any("parts", parts))
			}
		}
	}
	return infos, err
}
