package fs

import (
	"context"
	"io/fs"
	"iter"

	"github.com/jeffh/cfs/ninep"
)

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
	// Writes stats about a given file or directory. Implementations perform an all-or-nothing write.
	// Callers must use NoTouch values to indicate the underlying
	// implementation should not overwrite values.
	//
	// Implementers must return fs.ErrNotExist for files that do not exist
	//
	// The following stat values are off-limits and must be NoTouch values, according to spec:
	// - Uid (some implementations may break spec and support this value)
	// - Muid
	// - Device (aka - Dev)
	// - Type
	// - Qid
	// - Modifying Mode to change M_DIR
	WriteStat(ctx context.Context, path string, info fs.FileInfo) error
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
	return f.underlying.WriteStat(ctx, path, st.FileInfo())
}

func (f *fileSystem) Delete(ctx context.Context, path string) error {
	return f.underlying.Delete(ctx, path)
}
