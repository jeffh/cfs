package fs

import (
	"context"
	"io/fs"

	ninep "github.com/jeffh/cfs/ninep"
)

// ReadOnly wraps a FileSystem and returns a read-only version of it.
func ReadOnly(fsys ninep.FileSystem) ninep.FileSystem {
	return readOnly9FS{Underlying: fsys}
}

// TODO: support WalkableFileSystem if underlying supports it
type readOnly9FS struct {
	Underlying ninep.FileSystem
}

var _ ninep.FileSystem = readOnly9FS{}

func (r readOnly9FS) MakeDir(ctx context.Context, path string, mode ninep.Mode) error {
	return ninep.ErrWriteNotAllowed
}

func (r readOnly9FS) CreateFile(ctx context.Context, path string, flag ninep.OpenMode, mode ninep.Mode) (ninep.FileHandle, error) {
	return nil, ninep.ErrWriteNotAllowed
}

func (r readOnly9FS) OpenFile(ctx context.Context, path string, flag ninep.OpenMode) (ninep.FileHandle, error) {
	if flag.RequestsMutation() {
		return nil, ninep.ErrWriteNotAllowed
	}
	return r.Underlying.OpenFile(ctx, path, flag)
}

func (r readOnly9FS) ListDir(ctx context.Context, path string) (ninep.FileInfoIterator, error) {
	return r.Underlying.ListDir(ctx, path)
}

func (r readOnly9FS) Stat(ctx context.Context, path string) (fs.FileInfo, error) {
	return r.Underlying.Stat(ctx, path)
}

func (r readOnly9FS) WriteStat(ctx context.Context, path string, stat ninep.Stat) error {
	return ninep.ErrWriteNotAllowed
}
func (r readOnly9FS) Delete(ctx context.Context, path string) error { return ninep.ErrWriteNotAllowed }

func (r readOnly9FS) Traverse(ctx context.Context, path string) (ninep.TraversableFile, error) {
	return ninep.BasicTraverse(ctx, r.Underlying, path)
}
