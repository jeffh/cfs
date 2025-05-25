package fs

import (
	"bytes"
	"context"
	"io/fs"
	"iter"
	"os"
	"strings"
	"time"

	ninep "github.com/jeffh/cfs/ninep"
)

type lineFileHandle struct {
	info  fs.FileInfo
	mode  ninep.OpenMode
	buf   *bytes.Buffer
	flush func(*bytes.Buffer) error
}

////////////////////////////////////////////////

// Env implements a basic file system to the process environment variables.
func Env() ninep.FileSystem {
	return envFs{}
}

type envFs struct{}

func (d envFs) key(path string) string {
	return strings.TrimPrefix(path, "/")
}

// MakeDir creates a local directory as subdirectory of the root directory of Dir
func (d envFs) MakeDir(ctx context.Context, path string, mode ninep.Mode) error {
	return ninep.ErrWriteNotAllowed
}

// CreateFile creates a new file as a descendent of the root directory of envFs
func (d envFs) CreateFile(ctx context.Context, path string, flag ninep.OpenMode, mode ninep.Mode) (ninep.FileHandle, error) {
	key := d.key(path)
	var contents string
	if flag&ninep.OTRUNC == 0 {
		contents = os.Getenv(key)
	}
	onFlush := func(p []byte) error {
		_ = os.Setenv(key, string(p))
		return nil
	}
	return ninep.NewMemoryFileHandle([]byte(contents), nil, onFlush), nil
}

// OpenFile opens an existing file that is a descendent of the root directory of envFs for reading/writing
func (d envFs) OpenFile(ctx context.Context, path string, flag ninep.OpenMode) (ninep.FileHandle, error) {
	var contents string
	key := d.key(path)
	if flag&ninep.OTRUNC == 0 {
		contents = os.Getenv(key)
	}
	onFlush := func(p []byte) error {
		_ = os.Setenv(key, string(p))
		return nil
	}
	return ninep.NewMemoryFileHandle([]byte(contents), nil, onFlush), nil
}

// ListDir lists all files and directories in a given subdirectory
func (d envFs) ListDir(ctx context.Context, path string) iter.Seq2[fs.FileInfo, error] {
	if path != "/" {
		return ninep.FileInfoErrorIterator(fs.ErrNotExist)
	}
	return func(yield func(fs.FileInfo, error) bool) {
		vars := os.Environ()
		now := time.Now()
		for _, v := range vars {
			pair := strings.SplitN(v, "=", 2)
			info := ninep.MakeFileInfoWithSize(pair[0], ninep.Readable|ninep.Writeable, now, int64(len(pair[1])))
			if !yield(info, nil) {
				return
			}
		}
	}
}

// Stat returns information about a given file or directory
func (d envFs) Stat(ctx context.Context, path string) (fs.FileInfo, error) {
	if path == "/" {
		return ninep.MakeFileInfo(".", fs.ModeDir|ninep.Readable|ninep.Executable, time.Now()), nil
	}
	key := d.key(path)
	value, found := os.LookupEnv(key)
	if found {
		fi := ninep.MakeFileInfoWithSize(key, ninep.Readable|ninep.Writeable, time.Now(), int64(len(value)))
		return fi, nil
	} else {
		return nil, fs.ErrNotExist
	}
}

// WriteStat updates file or directory metadata.
func (d envFs) WriteStat(ctx context.Context, path string, s ninep.Stat) error {
	if path == "/" {
		return ninep.ErrWriteNotAllowed
	}
	key := d.key(path)
	value, found := os.LookupEnv(key)
	if !found {
		return fs.ErrNotExist
	}

	if !s.NameNoTouch() && path != s.Name() {
		_ = os.Unsetenv(key)
		newKey := d.key(s.Name())
		_ = os.Setenv(newKey, value)
		key = newKey
	}

	// this should be last since it's really hard to undo this
	if !s.LengthNoTouch() {
		if s.Length() < uint64(len(value)) {
			value = value[:s.Length()]
		}
		_ = os.Setenv(key, value)
	}
	return nil
}

// Delete a file or directory. Deleting the root directory will be an error.
func (d envFs) Delete(ctx context.Context, path string) error {
	if path == "/" {
		return fs.ErrPermission
	}
	key := d.key(path)
	_, found := os.LookupEnv(key)
	if found {
		_ = os.Unsetenv(key)
		return nil
	}
	return fs.ErrNotExist
}

func (d envFs) Traverse(ctx context.Context, path string) (ninep.TraversableFile, error) {
	return ninep.BasicTraverse(ctx, d, path)
}
