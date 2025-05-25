package proxy

import (
	"context"
	"fmt"
	"io"
	"io/fs"
	"iter"
	"os"
	"path/filepath"
	"strings"

	"github.com/jeffh/cfs/ninep"
)

// TODO: it would be nice to make FileSystemMount conform to FileSystem
type FileSystemMount struct {
	FS     ninep.TraversableFileSystem // required
	Prefix string                      // required
	Client ninep.Client                // optional
	Addr   string                      // optional
	Clean  func() error                // optional
}

var _ ninep.TraversableFileSystem = (*FileSystemMount)(nil)

func (fsm *FileSystemMount) String() string {
	return fmt.Sprintf("%s/%s", fsm.Addr, fsm.Prefix)
}

func (fsm *FileSystemMount) Close() error {
	if fsm.Clean != nil {
		err := fsm.Clean()
		if err != nil {
			_ = fsm.Client.Close()
			return err
		}
	}
	if fsm.Client != nil {
		return fsm.Client.Close()
	}
	return nil
}

type FileSystemMountConfig struct {
	Addr   string
	Prefix string
}

func ParseMount(arg string) (FileSystemMountConfig, bool) {
	parts := strings.SplitN(arg, "/", 2)
	count := len(parts)
	if count == 1 {
		return FileSystemMountConfig{Addr: parts[0]}, true
	} else if count >= 2 {
		return FileSystemMountConfig{
			Addr:   parts[0],
			Prefix: parts[1],
		}, true
	}
	return FileSystemMountConfig{}, false
}

func ParseMounts(args []string) []FileSystemMountConfig {
	fsmc := make([]FileSystemMountConfig, 0, len(args))
	for _, arg := range args {
		m, ok := ParseMount(arg)
		if ok {
			fsmc = append(fsmc, m)
		}
	}
	return fsmc
}

func PrintMountsHelp(w io.Writer) {
	_, _ = fmt.Fprintf(w, "\nMounts refers to a 9p server and path and are represented like <SERVER>:<PORT><PATH> like 'localhost:6666/prefix/path'.\n")
	_, _ = fmt.Fprintf(w, "\nThere are a few special mount values that are recognized:\n")
	_, _ = fmt.Fprintf(w, "  - ':memory' indicates an in memory file system that gets discarded after the program exits.\n")
	_, _ = fmt.Fprintf(w, "  - ':tmp' indicates an on-disk temporarily directory that gets discarded after the program exits.\n")
	_, _ = fmt.Fprintf(w, "  -  starting with a '/' or '.' indicates an on-disk local path.\n")
}

func (fsm *FileSystemMount) Join(elem ...string) string {
	path := filepath.Join(fsm.Prefix, filepath.Join(elem...))
	if fsm.Prefix == "" || strings.HasPrefix(fsm.Prefix, path) {
		return path
	}
	return fsm.Prefix
}

func (fsm *FileSystemMount) MakeDir(ctx context.Context, path string, mode ninep.Mode) error {
	return fsm.FS.MakeDir(ctx, fsm.Join(path), mode)
}
func (fsm *FileSystemMount) CreateFile(ctx context.Context, path string, flag ninep.OpenMode, mode ninep.Mode) (ninep.FileHandle, error) {
	return fsm.FS.CreateFile(ctx, fsm.Join(path), flag, mode)
}
func (fsm *FileSystemMount) OpenFile(ctx context.Context, path string, flag ninep.OpenMode) (ninep.FileHandle, error) {
	return fsm.FS.OpenFile(ctx, fsm.Join(path), flag)
}
func (fsm *FileSystemMount) ListDir(ctx context.Context, path string) iter.Seq2[fs.FileInfo, error] {
	it := fsm.FS.ListDir(ctx, fsm.Join(path))
	return ninep.Map2(it, func(fi fs.FileInfo) fs.FileInfo {
		name := fi.Name()
		if strings.HasPrefix(fsm.Prefix, name) {
			name = "/" + name[len(fsm.Prefix):]
		}
		return ninep.FileInfoWithName(fi, name)
	})
}
func (fsm *FileSystemMount) Stat(ctx context.Context, path string) (os.FileInfo, error) {
	st, err := fsm.FS.Stat(ctx, fsm.Join(path))
	if err != nil {
		return nil, err
	}

	return ninep.FileInfoWithName(st, path), err
}
func (fsm *FileSystemMount) WriteStat(ctx context.Context, path string, s ninep.Stat) error {
	if !s.NameNoTouch() {
		s = s.CopyWithNewName(fsm.Join(s.Name()))
	}
	return fsm.FS.WriteStat(ctx, fsm.Join(path), s)
}
func (fsm *FileSystemMount) Delete(ctx context.Context, path string) error {
	return fsm.FS.Delete(ctx, fsm.Join(path))
}
func (fsm *FileSystemMount) Traverse(ctx context.Context, path string) (ninep.TraversableFile, error) {
	return fsm.FS.Traverse(ctx, fsm.Join(path))
}
