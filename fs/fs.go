package fs

import (
	"context"
	"io/fs"
	"iter"
	"time"

	"github.com/jeffh/cfs/ninep"
)

// FS wraps a FileSystem to provide a ninep.FileSystem interface
// This provides an interface using io/fs when possible.
func FS(f FileSystem) ninep.FileSystem {
	return &fileSystem{underlying: f}
}

// FileInfoChangeRequest is used to change the stat of a file or directory.
// If a value is nil, the caller does not want that part of the stat to be changed.
type FileInfoChangeRequest struct {
	Name             *string
	Owner            *string
	Group            *string
	LastModifiedUser *string
	Mode             *fs.FileMode
	ModTime          *time.Time
	AccessTime       *time.Time
	Length           *int64
}

// FileSystem is a bridging interface to provide a ninep.FileSystem interface.
// This allows implementing using familiar fs types without knowing about the 9p
// protocol. Note that various fs.FileMode values cannot be translated to 9p.
type FileSystem interface {
	// Creates a directory. Implementions should recursively make directories
	// whenever possible.
	MakeDir(ctx context.Context, path string, mode fs.FileMode) error
	// Creates a file and opens it for reading/writing
	CreateFile(ctx context.Context, path string, flag int, mode fs.FileMode) (ninep.FileHandle, error)
	// Opens an existing file for reading/writing
	OpenFile(ctx context.Context, path string, flag int) (ninep.FileHandle, error)
	// Lists directories and files in a given path. Does not include '.' or '..'
	// fs.FileInfo may optionally implement Statable. Iterations are allowed to
	// return a nil FileInfo if they have an error.
	ListDir(ctx context.Context, path string) iter.Seq2[fs.FileInfo, error]
	// Lists stats about a given file or directory. Implementations may return
	// fs.ErrNotExist for directories that do not exist.
	Stat(ctx context.Context, path string) (fs.FileInfo, error)
	// Writes stats about a given file or directory.
	// The req contains all desired changes. If a value is nil, the caller
	// does not want that part of the stat to be changed.
	WriteStat(ctx context.Context, path string, info FileInfoChangeRequest) error
	// Deletes a file or directory. Implementations may reject directories that aren't empty
	Delete(ctx context.Context, path string) error
}

type fileSystem struct {
	underlying FileSystem
}

var _ ninep.FileSystem = (*fileSystem)(nil)

func (f *fileSystem) MakeDir(ctx context.Context, path string, mode ninep.Mode) error {
	return f.underlying.MakeDir(ctx, path, mode.ToFsMode())
}

func (f *fileSystem) CreateFile(ctx context.Context, path string, flag ninep.OpenMode, mode ninep.Mode) (ninep.FileHandle, error) {
	return f.underlying.CreateFile(ctx, path, mode.ToOsFlag(flag), mode.ToFsMode())
}

func (f *fileSystem) OpenFile(ctx context.Context, path string, flag ninep.OpenMode) (ninep.FileHandle, error) {
	return f.underlying.OpenFile(ctx, path, flag.ToOsFlag())
}

func (f *fileSystem) ListDir(ctx context.Context, path string) iter.Seq2[fs.FileInfo, error] {
	return f.underlying.ListDir(ctx, path)
}

func (f *fileSystem) Stat(ctx context.Context, path string) (fs.FileInfo, error) {
	return f.underlying.Stat(ctx, path)
}

func (f *fileSystem) WriteStat(ctx context.Context, path string, st ninep.Stat) error {
	req := FileInfoChangeRequest{}
	if !st.NameNoTouch() {
		req.Name = ptr(st.Name())
	}
	if !st.ModeNoTouch() {
		req.Mode = ptr(st.Mode().ToFsMode())
	}
	if !st.GidNoTouch() {
		req.Group = ptr(st.Gid())
	}
	if !st.UidNoTouch() {
		req.Owner = ptr(st.Uid())
	}
	if !st.MuidNoTouch() {
		req.LastModifiedUser = ptr(st.Muid())
	}
	if !st.MtimeNoTouch() {
		req.ModTime = ptr(time.Unix(int64(st.Mtime()), 0))
	}
	if !st.AtimeNoTouch() {
		req.AccessTime = ptr(time.Unix(int64(st.Atime()), 0))
	}
	if !st.LengthNoTouch() {
		req.Length = ptr(int64(st.Length()))
	}
	return f.underlying.WriteStat(ctx, path, req)
}

func (f *fileSystem) Delete(ctx context.Context, path string) error {
	return f.underlying.Delete(ctx, path)
}

func ptr[T any](v T) *T { return &v }
