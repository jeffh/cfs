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

func (d *delayFS) wait(op, path string) {
	slog.Info(op, "path", path, "delay", d.delay)
	time.Sleep(d.delay)
}

func (d *delayFS) MakeDir(ctx context.Context, path string, mode ninep.Mode) error {
	d.wait("delayFS.MakeDir", path)
	return d.underlying.MakeDir(ctx, path, mode)
}

func (d *delayFS) CreateFile(ctx context.Context, path string, flag ninep.OpenMode, mode ninep.Mode) (ninep.FileHandle, error) {
	d.wait("delayFS.CreateFile", path)
	h, err := d.underlying.CreateFile(ctx, path, flag, mode)
	if err != nil {
		return nil, err
	}
	return &delayFileHandle{h, path, d.delay}, nil
}

func (d *delayFS) OpenFile(ctx context.Context, path string, flag ninep.OpenMode) (ninep.FileHandle, error) {
	d.wait("delayFS.OpenFile", path)
	h, err := d.underlying.OpenFile(ctx, path, flag)
	if err != nil {
		return nil, err
	}
	return &delayFileHandle{h, path, d.delay}, nil
}

func (d *delayFS) ListDir(ctx context.Context, path string) iter.Seq2[fs.FileInfo, error] {
	d.wait("delayFS.ListDir", path)
	return d.underlying.ListDir(ctx, path)
}

func (d *delayFS) Stat(ctx context.Context, path string) (fs.FileInfo, error) {
	d.wait("delayFS.Stat", path)
	return d.underlying.Stat(ctx, path)
}

func (d *delayFS) WriteStat(ctx context.Context, path string, stat ninep.Stat) error {
	d.wait("delayFS.WriteStat", path)
	return d.underlying.WriteStat(ctx, path, stat)
}

func (d *delayFS) Delete(ctx context.Context, path string) error {
	d.wait("delayFS.Delete", path)
	return d.underlying.Delete(ctx, path)
}

type delayFileHandle struct {
	h     ninep.FileHandle
	path  string
	delay time.Duration
}

func (h *delayFileHandle) wait(op string, extra ...any) {
	args := append([]any{"path", h.path, "delay", h.delay}, extra...)
	slog.Info(op, args...)
	time.Sleep(h.delay)
}

func (h *delayFileHandle) ReadAt(p []byte, offset int64) (n int, err error) {
	h.wait("delayFileHandle.ReadAt", "offset", offset, "size", len(p))
	return h.h.ReadAt(p, offset)
}

func (h *delayFileHandle) WriteAt(p []byte, offset int64) (n int, err error) {
	h.wait("delayFileHandle.WriteAt", "offset", offset, "size", len(p))
	return h.h.WriteAt(p, offset)
}

func (h *delayFileHandle) Sync() error {
	h.wait("delayFileHandle.Sync")
	return h.h.Sync()
}

func (h *delayFileHandle) Close() error {
	h.wait("delayFileHandle.Close")
	return h.h.Close()
}
