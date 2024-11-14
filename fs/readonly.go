package fs

import (
	"context"
	"io/fs"
	"iter"
	"path/filepath"

	ninep "github.com/jeffh/cfs/ninep"
)

// ReadOnly wraps a FileSystem and returns a read-only version of it.
func ReadOnly(fsys ninep.FileSystem) ninep.FileSystem {
	return Restrict(fsys, DisallowCreation|DisallowMutation)
}

type Disallow uint64

const (
	DisallowMakeDirectory Disallow = 1 << iota
	DisallowCreateFile
	DisallowOpenFile
	DisallowListDir
	DisallowStat
	DisallowWriteStat
	DisallowDelete
	DisallowOpenFileTruncate
	DisallowRemoveOnClose

	DisallowCreation = DisallowMakeDirectory | DisallowCreateFile
	DisallowMutation = DisallowWriteStat | DisallowDelete | DisallowOpenFileTruncate | DisallowRemoveOnClose
)

// Restrict returns a new FileSystem that disallows certain operations.
func Restrict(fs ninep.FileSystem, disallow Disallow) ninep.FileSystem {
	return &proxyFileSystem{
		Underlying: fs,
		Disallow:   disallow,
	}
}

// Sub returns a new FileSystem that operates on a subdirectory of the given FileSystem.
func Sub(fs ninep.FileSystem, subdir string) ninep.FileSystem {
	return &subFileSystem{
		subdir:     subdir,
		Underlying: fs,
	}
}

type proxyFileSystem struct {
	Disallow   Disallow
	Underlying ninep.FileSystem
}

func (f *proxyFileSystem) MakeDir(ctx context.Context, path string, mode ninep.Mode) error {
	if f.Disallow&DisallowMakeDirectory != 0 {
		return ninep.ErrWriteNotAllowed
	}
	return f.Underlying.MakeDir(ctx, path, mode)
}

func (f *proxyFileSystem) CreateFile(ctx context.Context, path string, flag ninep.OpenMode, mode ninep.Mode) (ninep.FileHandle, error) {
	if f.Disallow&DisallowCreateFile != 0 {
		return nil, ninep.ErrWriteNotAllowed
	}
	return f.Underlying.CreateFile(ctx, path, flag, mode)
}

func (f *proxyFileSystem) OpenFile(ctx context.Context, path string, flag ninep.OpenMode) (ninep.FileHandle, error) {
	if f.Disallow&DisallowOpenFile != 0 {
		return nil, ninep.ErrInvalidAccess
	}
	if f.Disallow&DisallowOpenFileTruncate != 0 && flag&ninep.OTRUNC != 0 {
		return nil, ninep.ErrWriteNotAllowed
	}
	if f.Disallow&DisallowRemoveOnClose != 0 && flag&ninep.ORCLOSE != 0 {
		return nil, ninep.ErrWriteNotAllowed
	}
	return f.Underlying.OpenFile(ctx, path, flag)
}

func (f *proxyFileSystem) ListDir(ctx context.Context, path string) iter.Seq2[fs.FileInfo, error] {
	if f.Disallow&DisallowListDir != 0 {
		return ninep.FileInfoErrorIterator(ninep.ErrReadNotAllowed)
	}
	return f.Underlying.ListDir(ctx, path)
}

func (f *proxyFileSystem) Stat(ctx context.Context, path string) (fs.FileInfo, error) {
	if f.Disallow&DisallowStat != 0 {
		return nil, ninep.ErrWriteNotAllowed
	}
	return f.Underlying.Stat(ctx, path)
}

func (f *proxyFileSystem) WriteStat(ctx context.Context, path string, stat ninep.Stat) error {
	if f.Disallow&DisallowWriteStat != 0 {
		return ninep.ErrWriteNotAllowed
	}
	return f.Underlying.WriteStat(ctx, path, stat)
}

func (f *proxyFileSystem) Delete(ctx context.Context, path string) error {
	if f.Disallow&DisallowDelete != 0 {
		return ninep.ErrWriteNotAllowed
	}
	return f.Underlying.Delete(ctx, path)
}

func (f *proxyFileSystem) Traverse(ctx context.Context, path string) (ninep.TraversableFile, error) {
	return ninep.BasicTraverse(ctx, f.Underlying, path)
}

type subFileSystem struct {
	subdir     string
	Underlying ninep.FileSystem
}

func (f *subFileSystem) pathFor(path string) string {
	return filepath.Join(f.subdir, filepath.Clean(path))
}

func (f *subFileSystem) MakeDir(ctx context.Context, path string, mode ninep.Mode) error {
	if !fs.ValidPath(path) {
		return ninep.ErrBadFormat
	}
	return f.Underlying.MakeDir(ctx, f.pathFor(path), mode)
}
func (f *subFileSystem) CreateFile(ctx context.Context, path string, flag ninep.OpenMode, mode ninep.Mode) (ninep.FileHandle, error) {
	if !fs.ValidPath(path) {
		return nil, ninep.ErrBadFormat
	}
	return f.Underlying.CreateFile(ctx, f.pathFor(path), flag, mode)
}
func (f *subFileSystem) OpenFile(ctx context.Context, path string, flag ninep.OpenMode) (ninep.FileHandle, error) {
	if !fs.ValidPath(path) {
		return nil, ninep.ErrBadFormat
	}
	return f.Underlying.OpenFile(ctx, f.pathFor(path), flag)
}
func (f *subFileSystem) ListDir(ctx context.Context, path string) iter.Seq2[fs.FileInfo, error] {
	if !fs.ValidPath(path) {
		return ninep.FileInfoErrorIterator(ninep.ErrBadFormat)
	}
	return f.Underlying.ListDir(ctx, f.pathFor(path))
}
func (f *subFileSystem) Stat(ctx context.Context, path string) (fs.FileInfo, error) {
	if !fs.ValidPath(path) {
		return nil, ninep.ErrBadFormat
	}
	return f.Underlying.Stat(ctx, f.pathFor(path))
}
func (f *subFileSystem) WriteStat(ctx context.Context, path string, stat ninep.Stat) error {
	if !fs.ValidPath(path) {
		return ninep.ErrBadFormat
	}
	return f.Underlying.WriteStat(ctx, f.pathFor(path), stat)
}
func (f *subFileSystem) Delete(ctx context.Context, path string) error {
	if !fs.ValidPath(path) {
		return ninep.ErrBadFormat
	}
	return f.Underlying.Delete(ctx, f.pathFor(path))
}
