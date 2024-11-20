package billy

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"

	bill "github.com/go-git/go-billy/v5"
	"github.com/go-git/go-billy/v5/helper/chroot"
	"github.com/jeffh/cfs/fs/proxy"
	"github.com/jeffh/cfs/ninep"
)

var ErrUnsupported = errors.New("Unsupported")

const trace = false

// ToBillyFS returns a billy file system proxy of the given ninep file system mount.
// Currently, the returned file system proxy doesn't implement TempFile or Symlinks.
func ToBillyFS(fsm proxy.FileSystemMount) bill.Filesystem {
	return chroot.New(&bfs{fsm, ""}, string(filepath.Separator))
}

type bfs struct {
	FS     proxy.FileSystemMount
	Prefix string
}

func (fs *bfs) Create(filename string) (bill.File, error) {
	return fs.OpenFile(filename, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0666)
}
func (fs *bfs) Open(filename string) (bill.File, error) {
	return fs.OpenFile(filename, os.O_RDONLY, 0)
}
func (fs *bfs) OpenFile(filename string, flag int, perm fs.FileMode) (bill.File, error) {
	if trace {
		fmt.Printf("billy.FileSystem.OpenFile(%#v, %#v, %#v)\n", filename, flag, perm)
	}
	ctx := context.Background()
	path := filepath.Join(fs.Prefix, filename)
	if flag&os.O_CREATE != 0 {
		h, err := fs.FS.CreateFile(ctx, path, ninep.OpenModeFromOS(flag), ninep.ModeFromFS(perm))
		if err != nil {
			return nil, err
		}
		// TODO: path here should be relative to prefix?
		return &bfile{fs.FS, h, path, 0}, nil
	} else {
		h, err := fs.FS.OpenFile(ctx, path, ninep.OpenModeFromOS(flag))
		if err != nil {
			return nil, err
		}
		offset := int64(0)
		if flag&os.O_APPEND != 0 {
			st, err := fs.FS.Stat(ctx, path)
			if err != nil {
				return nil, err
			}
			offset = st.Size()
		}
		// TODO: path here should be relative to prefix?
		return &bfile{fs.FS, h, path, offset}, nil
	}
}
func (fs *bfs) Stat(filename string) (os.FileInfo, error) {
	if trace {
		fmt.Printf("billy.FileSystem.Stat(%#v) -> %#v\n", filename, filepath.Join(fs.Prefix, filename))
	}
	return fs.FS.Stat(context.Background(), filepath.Join(fs.Prefix, filename))
}
func (fs *bfs) Rename(oldpath, newpath string) error {
	if trace {
		fmt.Printf("billy.FileSystem.Rename(%#v, %#v)\n", oldpath, newpath)
	}
	st := ninep.SyncStatWithName(filepath.Join(fs.Prefix, newpath))
	return fs.FS.WriteStat(context.Background(), filepath.Join(fs.Prefix, oldpath), st)
}
func (fs *bfs) Remove(filename string) error {
	if trace {
		fmt.Printf("billy.FileSystem.Remove(%#v)\n", filename)
	}
	return fs.FS.Delete(context.Background(), filepath.Join(fs.Prefix, filename))
}
func (fs *bfs) Join(elem ...string) string { return filepath.Clean(filepath.Join(elem...)) }

func (fs *bfs) ReadDir(path string) ([]os.FileInfo, error) {
	if trace {
		fmt.Printf("billy.FileSystem.ReadDir(%#v)\n", path)
	}
	it := fs.FS.ListDir(context.Background(), filepath.Join(fs.Prefix, path))
	return ninep.FileInfoSliceFromIterator(it, -1)
}
func (fs *bfs) MkdirAll(filename string, perm fs.FileMode) error {
	if trace {
		fmt.Printf("billy.FileSystem.ReadDir(%#v, %#v)\n", filename, perm)
	}
	return fs.FS.MakeDir(context.Background(), filepath.Join(fs.Prefix, filename), ninep.ModeFromFS(perm))
}

func (fs *bfs) Lstat(filename string) (os.FileInfo, error) { return fs.Stat(filename) }
func (fs *bfs) Symlink(target, link string) error          { return ErrUnsupported }
func (fs *bfs) Readlink(link string) (string, error)       { return "", ErrUnsupported }

func (fs *bfs) TempFile(dir, prefix string) (bill.File, error) { return nil, ErrUnsupported }

type bfile struct {
	fsm      proxy.FileSystemMount
	f        ninep.FileHandle
	filename string
	offset   int64
}

func (f *bfile) Name() string { return f.filename }
func (f *bfile) Write(p []byte) (int, error) {
	n, err := f.f.WriteAt(p, f.offset)
	f.offset += int64(n)
	return n, err
}
func (f *bfile) Read(p []byte) (int, error) {
	n, err := f.f.ReadAt(p, f.offset)
	f.offset += int64(n)
	return n, err
}
func (f *bfile) ReadAt(p []byte, offset int64) (int, error) {
	f.offset = offset
	return f.Read(p)
}
func (f *bfile) Seek(offset int64, whence int) (int64, error) {
	switch whence {
	case io.SeekCurrent:
		f.offset += offset
	case io.SeekStart:
		f.offset = offset
	case io.SeekEnd:
		info, err := f.fsm.Stat(context.Background(), f.filename)
		if err != nil {
			return 0, err
		}
		f.offset = info.Size() - offset
	}
	return f.offset, nil
}
func (f *bfile) Close() error {
	return f.f.Close()
}
func (f *bfile) Truncate(size int64) error {
	if size < 0 {
		size = 0
	}
	st := ninep.SyncStat()
	st.SetLength(uint64(size))
	return f.fsm.WriteStat(context.Background(), f.filename, st)
}
func (f *bfile) Lock() error {
	return ErrUnsupported
}
func (f *bfile) Unlock() error {
	return ErrUnsupported
}
