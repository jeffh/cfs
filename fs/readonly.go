package fs

import (
	"context"
	"io/fs"

	ninep "github.com/jeffh/cfs/ninep"
)

// ReadOnly wraps a FileSystem and returns a read-only version of it.
func ReadOnly(fsys ninep.FileSystem) ninep.FileSystem {
	return readOnlyFS{Underlying: fsys}
}

// TODO: support WalkableFileSystem if underlying supports it
type readOnlyFS struct {
	Underlying ninep.FileSystem
}

var _ ninep.FileSystem = readOnlyFS{}

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
	return r.Underlying.OpenFile(ctx, path, flag)
}

func (r readOnlyFS) ListDir(ctx context.Context, path string) (ninep.FileInfoIterator, error) {
	return r.Underlying.ListDir(ctx, path)
}

func (r readOnlyFS) Stat(ctx context.Context, path string) (fs.FileInfo, error) {
	return r.Underlying.Stat(ctx, path)
}

func (r readOnlyFS) WriteStat(ctx context.Context, path string, stat ninep.Stat) error {
	return ninep.ErrWriteNotAllowed
}
func (r readOnlyFS) Delete(ctx context.Context, path string) error { return ninep.ErrWriteNotAllowed }

func (r readOnlyFS) Traverse(ctx context.Context, path string) (ninep.TraversableFile, error) {
	return ninep.BasicTraverse(ctx, r.Underlying, path)
}
