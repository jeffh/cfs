// Mounts a 9p file server as a user-space local file system mount (using FUSE)
package fuse

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/user"
	"path/filepath"
	"strconv"
	"sync"
	"syscall"
	"unsafe"

	fs "github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/jeffh/cfs/ninep"
)

const timeout = 0
const timeoutMsec = 0

// A helper function for starting a fuse mount point
func MountAndServeFS(ctx context.Context, f ninep.FileSystem, prefix, level string, L *slog.Logger, mountpoint string, opts *fs.Options) error {
	L = ninep.CreateLogger(level, "fuse", L)
	fscfg := &FsConfig{
		L:      L,
		prefix: prefix,
	}
	root := &Dir{fs: f, path: "", config: fscfg, timeout: 10}

	out := make(chan error, 1)

	go func() {
		var cancel context.CancelFunc
		ctx, cancel = context.WithCancel(ctx)
		defer cancel()

		srv, err := fs.Mount(mountpoint, root, opts)
		if err != nil {
			out <- err
			close(out)
			return
		}
		go func() {
			select {
			case <-ctx.Done():
			case <-out:
				return
			}
			err := srv.Unmount()
			if err != nil {
				fmt.Printf("Error when unmounting: %s", err)
			}
		}()

		srv.Wait()
		close(out)
	}()

	err, ok := <-out
	if ok {
		return err
	}
	return nil
}

type FsConfig struct {
	// set to true to disable automatic mapping of usernames/groupnames to
	// local uids and gids
	doNotMapIds bool
	prefix      string

	m    sync.Mutex
	inos map[string]uint64

	L *slog.Logger
}

func (c *FsConfig) putIno(path string, value *interface{}) {
	c.m.Lock()
	defer c.m.Unlock()
	c.inos[path] = uint64(uintptr(unsafe.Pointer(value)))
}

func (c *FsConfig) getIno(path string) (uint64, bool) {
	c.m.Lock()
	defer c.m.Unlock()
	v, ok := c.inos[path]
	return v, ok
}

func (c *FsConfig) path(p string) string {
	return filepath.Join(c.prefix, p)
}

///////////////////////////////////////////////////////////

var _ fs.NodeReaddirer = (*Dir)(nil)
var _ fs.NodeCreater = (*Dir)(nil)
var _ fs.NodeFsyncer = (*Dir)(nil)
var _ fs.NodeGetattrer = (*Dir)(nil)
var _ fs.NodeLinker = (*Dir)(nil)
var _ fs.NodeLookuper = (*Dir)(nil)
var _ fs.NodeMkdirer = (*Dir)(nil)
var _ fs.NodeRenamer = (*Dir)(nil)
var _ fs.NodeRmdirer = (*Dir)(nil)
var _ fs.NodeSetattrer = (*Dir)(nil)
var _ fs.NodeUnlinker = (*Dir)(nil)
var _ fs.NodeLookuper = (*Dir)(nil)

// var _ fs.NodeGetxattrer = (*Dir)(nil)
// var _ fs.NodeRemovexattrer = (*Dir)(nil)
// var _ fs.NodeSetxattrer = (*Dir)(nil)

// var _ fs.NodeCopyFileRanger = (*Dir)(nil)

type Dir struct {
	fs.Inode
	fs     ninep.FileSystem
	path   string
	config *FsConfig

	timeout uint64
}

// func (n *Dir) Getattr(ctx context.Context, a *fuse.Attr) error {
func (n *Dir) Getattr(ctx context.Context, h fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	path := n.config.path(n.path)
	st, err := n.fs.Stat(ctx, path)
	if err != nil {
		if n.config.L != nil {
			n.config.L.DebugContext(ctx, "Dir.Getattr", slog.String("path", path), slog.String("err", err.Error()))
		}
		return mapErr(err, syscall.EACCES)
	} else {
		if n.config.L != nil {
			n.config.L.InfoContext(ctx, "Dir.Getattr", slog.String("path", path))
		}
	}
	if errno := fillAttr(n.config, st, &out.Attr); errno != 0 {
		return errno
	}
	out.AttrValid = n.timeout
	return 0
}

func (n *Dir) Setattr(ctx context.Context, f fs.FileHandle, in *fuse.SetAttrIn, out *fuse.AttrOut) syscall.Errno {
	st := ninep.SyncStat()
	if size, ok := in.GetSize(); ok {
		st.SetLength(size)
	}
	if atime, ok := in.GetATime(); ok {
		st.SetAtime(uint32(atime.Unix()))
	}
	if mtime, ok := in.GetMTime(); ok {
		st.SetMtime(uint32(mtime.Unix()))
	}
	if mode, ok := in.GetMode(); ok {
		st.SetMode(ninep.ModeFromOS(os.FileMode(mode)))
	}
	path := n.config.path(n.path)
	errno := mapErr(n.fs.WriteStat(ctx, path, st), syscall.EINVAL) // TODO: what is the proper error here?
	if errno != 0 {
		if n.config.L != nil {
			n.config.L.ErrorContext(ctx, "Dir.Setattr", slog.String("path", path), slog.Any("st", st), slog.String("err", syscall.Errno(errno).Error()))
		}
		return errno
	} else {
		if n.config.L != nil {
			n.config.L.InfoContext(ctx, "Dir.Setattr", slog.String("path", path), slog.Any("st", st))
		}
	}

	stat, err := n.fs.Stat(ctx, path)
	if err != nil {
		return mapErr(err, syscall.EINVAL) // TODO: what is the proper error here?
	}

	errno = fillAttr(n.config, stat, &out.Attr)
	if errno != 0 {
		return errno
	}

	return 0
}

func (n *Dir) Setxattr(ctx context.Context, in *fuse.SetXAttrIn) error {
	// technically, we can't support this because we're not the underlying fs,
	// but not implementing this causes errors when using cp (in macos).
	return nil
}

func (n *Dir) Rename(ctx context.Context, name string, newParent fs.InodeEmbedder, newName string, flags uint32) syscall.Errno {
	//Rename(ctx context.Context, req *fuse.RenameRequest, newDir fs.Node) error {

	nd := newParent.(*Dir)
	path := n.config.path(n.path)
	newpath := nd.config.path(nd.path)

	oldpath := filepath.Join(path, name)
	newpath = filepath.Join(newpath, newName)
	if n.config.L != nil {
		n.config.L.InfoContext(ctx, "Dir.Rename", slog.String("path", path), slog.String("oldpath", oldpath), slog.String("newpath", newpath))
	}
	st := ninep.SyncStatWithName(newpath)
	return mapErr(n.fs.WriteStat(ctx, oldpath, st), syscall.EINVAL) // TODO: what is a better error message?
}

func (n *Dir) Link(ctx context.Context, target fs.InodeEmbedder, name string, out *fuse.EntryOut) (node *fs.Inode, errno syscall.Errno) {
	errno = syscall.EINVAL
	return
}

func (n *Dir) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	path := n.config.path(n.path)
	if n.config.L != nil {
		n.config.L.DebugContext(ctx, "Dir.ReadDirAll", slog.String("path", path))
	}
	entries := make([]fuse.DirEntry, 0, 16)
	i := 0
	for info, err := range n.fs.ListDir(ctx, path) {
		if err != nil {
			return nil, mapErr(err, 0)
		}
		dt := uint32(info.Mode())
		stat := info.Sys().(ninep.Stat)
		entries = append(entries, fuse.DirEntry{
			Ino:  stat.Qid().Path(),
			Mode: dt,
			Name: stat.Name(),
		})
		if n.config.L != nil {
			n.config.L.DebugContext(ctx, "Dir.ReadDirAll", slog.String("path", path), slog.Int("i", i), slog.String("name", stat.Name()), slog.String("mode", stat.Mode().String()), slog.Uint64("dt", uint64(dt)))
		}
		i++
	}
	return fs.NewListDirStream(entries), 0
}

func (n *Dir) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	*out = fuse.EntryOut{}

	path := n.config.path(n.path)
	path = filepath.Join(n.path, name)
	if n.config.L != nil {
		n.config.L.DebugContext(ctx, "Dir.Lookup.begin", slog.String("path", path), slog.String("name", name))
	}
	info, err := n.fs.Stat(ctx, path)
	if err != nil {
		return nil, mapErr(err, 0)
	}

	if errno := fillAttr(n.config, info, &out.Attr); errno != 0 {
		return nil, errno
	}

	stat := info.Sys().(ninep.Stat)
	stable := fs.StableAttr{
		Mode: fillMode(info),
		Ino:  stat.Qid().Path(),
	}
	if n.config.L != nil {
		n.config.L.DebugContext(ctx, "Dir.Lookup", slog.String("path", path), slog.String("name", name), slog.Uint64("ino", stat.Qid().Path()), slog.Bool("isDir", info.IsDir()))
	}

	out.NodeId = stable.Ino
	out.EntryValid = n.timeout
	out.AttrValid = n.timeout

	var inode *fs.Inode
	if info.IsDir() {
		inode = n.NewInode(ctx, &Dir{fs: n.fs, path: path, config: n.config, timeout: n.timeout}, stable)
	} else {
		inode = n.NewInode(ctx, &File{fs: n.fs, path: path, config: n.config, timeout: n.timeout}, stable)
	}
	return inode, 0
}

func (n *Dir) Mkdir(ctx context.Context, name string, mode uint32, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	path := n.config.path(n.path)
	path = filepath.Join(path, name)
	mod := flagModeToMode(0, os.FileMode(mode))
	if n.config.L != nil {
		n.config.L.DebugContext(ctx, "Dir.Mkdir", slog.String("path", path), slog.String("name", name), slog.String("mode", mod.String()))
	}

	err := n.fs.MakeDir(ctx, path, mod)
	if err != nil {
		if n.config.L != nil {
			n.config.L.ErrorContext(ctx, "Dir.MkDir", slog.String("path", path), slog.String("name", name), slog.String("mode", mod.String()), slog.String("err", err.Error()))
		}
		return nil, mapErr(err, syscall.EINVAL) // TODO: what's better to report here?
	}

	info, err := n.fs.Stat(ctx, path)
	if err != nil {
		if n.config.L != nil {
			n.config.L.ErrorContext(ctx, "Dir.MkDir", slog.String("path", path), slog.String("name", name), slog.String("mode", mod.String()), slog.String("err", err.Error()))
		}
		return nil, mapErr(err, syscall.EINVAL) // TODO: what's better to report here?
	}

	stat := info.Sys().(ninep.Stat)
	stable := fs.StableAttr{
		Mode: fillMode(info),
		Ino:  stat.Qid().Path(),
	}

	out.NodeId = stable.Ino
	out.EntryValid = n.timeout
	out.AttrValid = n.timeout

	if errno := fillAttr(n.config, info, &out.Attr); errno != 0 {
		return nil, errno
	}
	if n.config.L != nil {
		n.config.L.InfoContext(ctx, "Dir.MkDir", slog.String("path", path), slog.String("name", name), slog.String("mode", mod.String()), slog.Uint64("ino", stat.Qid().Path()))
	}

	inode := n.NewInode(ctx, &Dir{fs: n.fs, path: path, config: n.config, timeout: n.timeout}, stable)
	return inode, 0
}

func (n *Dir) Create(ctx context.Context, name string, flags uint32, mode uint32, out *fuse.EntryOut) (node *fs.Inode, fh fs.FileHandle, fuseFlags uint32, errno syscall.Errno) {
	path := n.config.path(n.path)
	if n.config.L != nil {
		n.config.L.DebugContext(ctx, "Dir.CreateFile.begin", slog.String("path", path), slog.String("name", name), slog.Uint64("flags", uint64(flags)), slog.Uint64("mode", uint64(mode)))
	}
	path = filepath.Join(path, name)
	flg := flagToOpenMode(flags)
	mod := flagModeToMode(flags, os.FileMode(mode))

	if flags&syscall.O_CREAT == 0 {
		if n.config.L != nil {
			n.config.L.ErrorContext(ctx, "Dir.CreateFile", slog.String("path", path), slog.String("name", name), slog.String("err", "FUSE did not get OpenCreate"))
		}
		return nil, nil, 0, syscall.EINVAL
	}

	if flags&syscall.O_DIRECTORY != 0 {
		return nil, nil, 0, syscall.EINVAL
	} else {
		h, err := n.fs.CreateFile(ctx, path, flg, mod)
		if err != nil {
			if n.config.L != nil {
				n.config.L.ErrorContext(ctx, "Dir.CreateFile", slog.String("path", path), slog.String("name", name), slog.String("err", err.Error()))
			}
			return nil, nil, 0, mapErr(err, 0)
		}

		resFlags := uint32(fuse.FOPEN_DIRECT_IO)

		info, err := n.fs.Stat(ctx, path)
		if err != nil {
			h.Close()
			return nil, nil, 0, mapErr(err, syscall.EINVAL) // TODO: what's better to report here?
		}

		stat := info.Sys().(ninep.Stat)
		stable := fs.StableAttr{
			Mode: fillMode(info),
			Ino:  stat.Qid().Path(),
		}

		if errno := fillAttr(n.config, info, &out.Attr); errno != 0 {
			h.Close()
			return nil, nil, 0, errno
		}
		if n.config.L != nil {
			n.config.L.InfoContext(
				ctx,
				"Dir.CreateFile",
				slog.String("path", path),
				slog.String("name", name),
				slog.Uint64("flags", uint64(flags)),
				slog.Uint64("mode", uint64(mode)),
				slog.Uint64("ino", stat.Qid().Path()),
			)
		}

		out.NodeId = stable.Ino
		out.EntryValid = n.timeout
		out.AttrValid = n.timeout

		inode := n.NewInode(ctx, &File{fs: n.fs, path: path, config: n.config, timeout: n.timeout}, stable)
		return inode, &FileHandle{fs: n.fs, path: path, h: h, config: n.config}, resFlags, 0
	}
}

func (n *Dir) Rmdir(ctx context.Context, name string) syscall.Errno { return n.remove(ctx, name, true) }
func (n *Dir) Unlink(ctx context.Context, name string) syscall.Errno {
	return n.remove(ctx, name, false)
}

func (n *Dir) remove(ctx context.Context, name string, isDir bool) syscall.Errno {
	path := n.config.path(n.path)
	if n.config.L != nil {
		n.config.L.DebugContext(ctx, "Dir.Remove", slog.String("path", path), slog.String("name", name))
	}
	path = filepath.Join(path, name)
	stat, err := n.fs.Stat(ctx, path)
	if err != nil {
		return mapErr(err, syscall.ENOENT)
	}
	if isDir {
		if !stat.IsDir() {
			return syscall.ENOTDIR
		}
	} else {
		if stat.IsDir() {
			return syscall.EISDIR
		}
	}
	return mapErr(n.fs.Delete(ctx, path), syscall.EINVAL)
}

func (n *Dir) Fsync(ctx context.Context, h fs.FileHandle, flags uint32) syscall.Errno {
	path := n.config.path(n.path)
	return mapErr(n.fs.WriteStat(ctx, path, ninep.SyncStat()), 0)
}

///////////////////////////////////////////////////////////////

type File struct {
	fs.Inode
	fs     ninep.FileSystem
	path   string
	config *FsConfig

	timeout uint64
}

var _ fs.NodeOpener = (*File)(nil)
var _ fs.NodeReader = (*File)(nil)
var _ fs.NodeSetattrer = (*File)(nil)
var _ fs.NodeSetxattrer = (*File)(nil)
var _ fs.NodeFsyncer = (*File)(nil)
var _ fs.NodeReader = (*File)(nil)

func (n *File) Getattr(ctx context.Context, f FileHandle, out *fuse.AttrOut) syscall.Errno {
	// func (n *File) Attr(ctx context.Context, a *fuse.Attr) error {

	path := n.config.path(n.path)
	if n.config.L != nil {
		n.config.L.DebugContext(ctx, "File.Getattr", slog.String("path", path))
	}
	st, err := n.fs.Stat(ctx, path)
	if err != nil {
		return mapErr(err, 0)
	}
	if errno := fillAttr(n.config, st, &out.Attr); errno != 0 {
		return errno
	}
	out.AttrValid = n.timeout
	return 0
}

func (n *File) Setattr(ctx context.Context, f fs.FileHandle, in *fuse.SetAttrIn, out *fuse.AttrOut) syscall.Errno {
	st := ninep.SyncStat()
	if size, ok := in.GetSize(); ok {
		st.SetLength(size)
	}
	if atime, ok := in.GetATime(); ok {
		st.SetAtime(uint32(atime.Unix()))
	}
	if mtime, ok := in.GetMTime(); ok {
		st.SetMtime(uint32(mtime.Unix()))
	}
	if mode, ok := in.GetMode(); ok {
		st.SetMode(ninep.ModeFromOS(os.FileMode(mode)))
	}
	path := n.config.path(n.path)
	if n.config.L != nil {
		n.config.L.DebugContext(ctx, "File.Setattr", slog.String("path", path), slog.String("st", st.String()))
	}
	if errno := mapErr(n.fs.WriteStat(ctx, path, st), syscall.EINVAL); errno != 0 {
		return errno
	}

	stat, err := n.fs.Stat(ctx, path)
	if err != nil {
		return mapErr(err, 0)
	}
	if errno := fillAttr(n.config, stat, &out.Attr); errno != 0 {
		return errno
	}

	return 0
}

func (n *File) Setxattr(ctx context.Context, attr string, data []byte, flags uint32) syscall.Errno {
	if n.config.L != nil {
		n.config.L.DebugContext(ctx, "File.Setxattr", slog.String("path", n.config.path(n.path)), slog.String("attr", attr))
	}
	// technically, we can't support this because we're not the underlying fs,
	// but not implementing this causes errors when using cp (in macos).
	return 0
}

func (n *File) Open(ctx context.Context, flags uint32) (fh fs.FileHandle, fuseFlags uint32, errno syscall.Errno) {
	path := n.config.path(n.path)
	if n.config.L != nil {
		n.config.L.DebugContext(ctx, "File.Open", slog.String("path", path), slog.Uint64("flags", uint64(flags)))
	}
	// st, err := n.fs.Stat(ctx, path)
	// if err != nil {
	// 	return nil, 0, mapErr(err, syscall.EBADF)
	// }
	mode := ninep.OpenMode(flags) // TODO: convert openflags
	h, err := n.fs.OpenFile(ctx, path, mode)
	if err != nil {
		return nil, 0, mapErr(err, syscall.EINVAL)
	}

	// resp.Handle = fuse.HandleID(stat.Qid().Path())

	// resp.Flags |= fuse.OpenDirectIO | fuse.OpenPurgeUBC
	// const PurgeUBC = 1 << 31
	// fuseFlags = uint32(PurgeUBC)
	fuseFlags = fuse.FOPEN_DIRECT_IO
	// TODO: support appendonly (OpenNonSeekable)
	fh = &FileHandle{n.fs, path, h, n.config}
	return
}

func (n *File) Fsync(ctx context.Context, f fs.FileHandle, flags uint32) syscall.Errno {
	path := n.config.path(n.path)
	if n.config.L != nil {
		n.config.L.DebugContext(ctx, "File.Fsync", slog.String("path", path))
	}
	h := f.(FileHandle)
	return mapErr(h.h.Sync(), syscall.EINVAL)
}

func (n *File) Read(ctx context.Context, f fs.FileHandle, dest []byte, off int64) (fuse.ReadResult, syscall.Errno) {
	path := n.config.path(n.path)
	if n.config.L != nil {
		n.config.L.DebugContext(ctx, "File.Read", slog.String("path", path), slog.Int64("off", off), slog.Int("len", len(dest)))
	}
	fh := f.(*FileHandle)
	return fh.Read(ctx, dest, off)
}

/////////////////////////////////////////////////////////////////

type FileHandle struct {
	fs     ninep.FileSystem
	path   string
	h      ninep.FileHandle
	config *FsConfig
}

var _ fs.FileAllocater = (*FileHandle)(nil)
var _ fs.FileHandle = (*FileHandle)(nil)
var _ fs.FileFlusher = (*FileHandle)(nil)
var _ fs.FileReader = (*FileHandle)(nil)
var _ fs.FileReleaser = (*FileHandle)(nil)
var _ fs.FileWriter = (*FileHandle)(nil)

func (h *FileHandle) Allocate(ctx context.Context, off uint64, size uint64, mode uint32) syscall.Errno {
	path := h.config.path(h.path)
	st := ninep.SyncStat()
	st.SetLength(off + size)
	err := h.fs.WriteStat(ctx, path, st)
	return mapErr(err, syscall.ENOTSUP)
}

// func (h *FileHandle) Setlkw(ctx context.Context, owner uint64, lk *fuse.FileLock, flags uint32) syscall.Errno {
// 	fmt.Printf("SetlockWait\n")
// 	return 0
// }

// func (h *FileHandle) Setlk(ctx context.Context, owner uint64, lk *fuse.FileLock, flags uint32) syscall.Errno {
// 	fmt.Printf("Setlock\n")
// 	return 0
// }

// func (h *FileHandle) Lseek(ctx context.Context, off uint64, whence uint32) (uint64, syscall.Errno) {
// 	fmt.Printf("Seek\n")
// 	return 0, 0
// }

func (h *FileHandle) Read(ctx context.Context, dest []byte, off int64) (fuse.ReadResult, syscall.Errno) {
	// func (h *FileHandle) Read(ctx context.Context, req *fuse.ReadRequest, resp *fuse.ReadResponse) error {
	if h.config.L != nil {
		h.config.L.DebugContext(ctx, "File.Read", slog.String("path", h.config.path(h.path)), slog.Int("len", len(dest)), slog.Int64("off", off))
	}
	buf := make([]byte, len(dest))
	n, err := h.h.ReadAt(buf, off)
	if err == io.EOF {
		err = nil
	}
	return fuse.ReadResultData(buf[:n]), mapErr(err, syscall.EIO)
}

func (h *FileHandle) Write(ctx context.Context, data []byte, off int64) (written uint32, errno syscall.Errno) {
	n, err := h.h.WriteAt(data, off)
	return uint32(n), mapErr(err, syscall.EIO)
}

func (h *FileHandle) Release(ctx context.Context) syscall.Errno {
	return mapErr(h.h.Close(), syscall.EINVAL)
}

func (h *FileHandle) Fsync(ctx context.Context, flags uint32) syscall.Errno {
	return mapErr(h.h.Sync(), syscall.EBADF)
}

func (h *FileHandle) Flush(ctx context.Context) syscall.Errno {
	return mapErr(h.h.Sync(), syscall.EBADF)
}

///////////////////////////////////////////////////////////////////

func flagToOpenMode(flg uint32) ninep.OpenMode { return ninep.OpenModeFromOS(int(flg)) }

func flagModeToMode(flg uint32, mode os.FileMode) ninep.Mode {
	m := ninep.ModeFromOS(mode)

	f := int(flg)

	if f&os.O_APPEND != 0 {
		m |= ninep.M_APPEND
	}
	if f&os.O_EXCL != 0 {
		m |= ninep.M_EXCL
	}
	return m
}

func mapErr(err error, defErr syscall.Errno) syscall.Errno {
	if err == nil {
		return 0
	}
	if err == io.EOF {
		return syscall.EIO
	}
	if err == ninep.ErrUnsupported {
		return syscall.ENOSYS
	}
	if os.IsNotExist(err) {
		return syscall.ENOENT
	}
	if errors.Is(err, os.ErrExist) {
		return syscall.EEXIST
	}
	if err == io.ErrNoProgress {
		return syscall.ENODATA
	}
	if err == os.ErrPermission {
		return syscall.EPERM
	}
	if err == os.ErrClosed {
		return syscall.EBADF
	}
	if err == os.ErrInvalid {
		return syscall.EINVAL
	}
	if err == ninep.ErrInvalidAccess {
		return syscall.EACCES
	}
	fmt.Printf("[unmapped error] %s\n", err)
	return defErr
}

func fillMode(info os.FileInfo) uint32 {
	mode := info.Mode()
	dt := uint32(mode & os.ModePerm)
	if mode&os.ModeDir != 0 {
		dt |= fuse.S_IFDIR
	} else if mode&os.ModeSymlink != 0 {
		dt |= fuse.S_IFLNK
	} else if mode&os.ModeAppend != 0 {
		dt |= fuse.S_IFIFO
	} else if mode&os.ModeType == 0 {
		dt |= fuse.S_IFREG
	}
	fmt.Printf("MODE: %#v (%s -> %s)\n", info.Name(), mode.String(), os.FileMode(dt).String())
	return dt
}

func fillAttr(config *FsConfig, st os.FileInfo, out *fuse.Attr) syscall.Errno {
	stat := st.Sys().(ninep.Stat)
	out.Ino = stat.Qid().Path()
	out.Mode = uint32(st.Mode())
	out.Atime = uint64(stat.Atime())
	out.Mtime = uint64(st.ModTime().Unix())
	out.Ctime = out.Mtime
	out.Size = stat.Length()
	out.Blocks = stat.Length() / 512 // just a proxy
	if !config.doNotMapIds {
		username := stat.Uid()
		usr, err := user.Lookup(username)
		if err == nil {
			uid, err := strconv.Atoi(usr.Uid)
			// n.tracef("File.Attr(%#v).Atoi(uid=%s) -> %s\n", path, usr.Uid, err)
			if err != nil {
				return syscall.EINVAL
			}
			out.Uid = uint32(uid)
		}

		groupname := stat.Gid()
		grp, err := user.LookupGroup(groupname)
		if err == nil {
			gid, err := strconv.Atoi(grp.Gid)
			// n.tracef("File.Attr(%#v).Atoi(gid=%s) -> %s\n", path, grp.Gid, err)
			if err != nil {
				return syscall.EINVAL
			}
			out.Gid = uint32(gid)
		}
	}
	return 0
}
