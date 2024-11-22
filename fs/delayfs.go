package fs

import (
	"context"
	"io/fs"
	"iter"
	"log/slog"
	"time"

	ninep "github.com/jeffh/cfs/ninep"
)

type delayFS struct {
	underlying ninep.FileSystem
	delay      time.Duration
}

func NewDelayFS(underlying ninep.FileSystem, delay time.Duration) ninep.FileSystem {
	return &delayFS{underlying, delay}
}

func (d *delayFS) MakeDir(ctx context.Context, path string, mode ninep.Mode) error {
	slog.Info("delayFS.MakeDir", "path", path, "delay", d.delay)
	time.Sleep(d.delay)
	return d.underlying.MakeDir(ctx, path, mode)
}

func (d *delayFS) CreateFile(ctx context.Context, path string, flag ninep.OpenMode, mode ninep.Mode) (ninep.FileHandle, error) {
	slog.Info("delayFS.CreateFile", "path", path, "delay", d.delay)
	time.Sleep(d.delay)
	h, err := d.underlying.CreateFile(ctx, path, flag, mode)
	if err != nil {
		return nil, err
	}
	return &delayFileHandle{h, path, d.delay}, nil
}

func (d *delayFS) OpenFile(ctx context.Context, path string, flag ninep.OpenMode) (ninep.FileHandle, error) {
	slog.Info("delayFS.OpenFile", "path", path, "delay", d.delay)
	time.Sleep(d.delay)
	h, err := d.underlying.OpenFile(ctx, path, flag)
	if err != nil {
		return nil, err
	}
	return &delayFileHandle{h, path, d.delay}, nil
}

func (d *delayFS) ListDir(ctx context.Context, path string) iter.Seq2[fs.FileInfo, error] {
	slog.Info("delayFS.ListDir", "path", path, "delay", d.delay)
	time.Sleep(d.delay)
	return d.underlying.ListDir(ctx, path)
}

func (d *delayFS) Stat(ctx context.Context, path string) (fs.FileInfo, error) {
	slog.Info("delayFS.Stat", "path", path, "delay", d.delay)
	time.Sleep(d.delay)
	return d.underlying.Stat(ctx, path)
}

func (d *delayFS) WriteStat(ctx context.Context, path string, stat ninep.Stat) error {
	slog.Info("delayFS.WriteStat", "path", path, "delay", d.delay)
	time.Sleep(d.delay)
	return d.underlying.WriteStat(ctx, path, stat)
}

func (d *delayFS) Delete(ctx context.Context, path string) error {
	slog.Info("delayFS.Delete", "path", path, "delay", d.delay)
	time.Sleep(d.delay)
	return d.underlying.Delete(ctx, path)
}

type delayFileHandle struct {
	h     ninep.FileHandle
	path  string
	delay time.Duration
}

func (h *delayFileHandle) ReadAt(p []byte, offset int64) (n int, err error) {
	slog.Info("delayFileHandle.ReadAt", "path", h.path, "offset", offset, "size", len(p), "delay", h.delay)
	time.Sleep(h.delay)
	return h.h.ReadAt(p, offset)
}

func (h *delayFileHandle) WriteAt(p []byte, offset int64) (n int, err error) {
	slog.Info("delayFileHandle.WriteAt", "path", h.path, "offset", offset, "size", len(p), "delay", h.delay)
	time.Sleep(h.delay)
	return h.h.WriteAt(p, offset)
}

func (h *delayFileHandle) Sync() error {
	slog.Info("delayFileHandle.Sync", "path", h.path, "delay", h.delay)
	time.Sleep(h.delay)
	return h.h.Sync()
}

func (h *delayFileHandle) Close() error {
	slog.Info("delayFileHandle.Close", "path", h.path, "delay", h.delay)
	time.Sleep(h.delay)
	return h.h.Close()
}
