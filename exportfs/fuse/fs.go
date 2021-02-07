// Mounts a 9p file server as a user-space local file system mount (using FUSE)
package fuse

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/user"
	"path/filepath"
	"strconv"
	"sync"
	"syscall"
	"unsafe"

	fs "github.com/hanwen/go-fuse/fs"
	"github.com/hanwen/go-fuse/fuse"
	"github.com/jeffh/cfs/ninep"
)

// A helper function for starting a fuse mount point
func MountAndServeFS(ctx context.Context, f ninep.FileSystem, prefix string, loggable ninep.Loggable, mountpoint string, opts *fs.Options) error {
	fscfg := &FsConfig{
		Loggable: loggable,
		prefix:   prefix,
	}
	root := &Dir{fs: f, path: "", config: fscfg}

	srv, err := fs.Mount(mountpoint, root, opts)
	if err != nil {
		return err
	}

	srv.Wait()
	return nil
	// c, err := fs.Mount(mountpoint, opts)
	// if err != nil {
	// 	return err
	// }

	// subctx, cancel := context.WithCancel(ctx)
	// defer cancel()
	// defer c.Close()
	// defer fuse.Unmount(mountpoint)

	// errCh := make(chan error)
	// go func() {
	// 	errCh <- fs.Serve(c, &Fs{Fs: f, Config: fscfg})
	// }()

	// // check if the mount process has an error to report
	// <-c.Ready
	// if err := c.MountError; err != nil {
	// 	return err
	// }

	// select {
	// case err := <-errCh:
	// 	return err
	// case <-subctx.Done():
	// }
	// return nil
}

type FsConfig struct {
	// set to true to disable automatic mapping of usernames/groupnames to
	// local uids and gids
	doNotMapIds bool
	prefix      string

	m    sync.Mutex
	inos map[string]uint64

	ninep.Loggable
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

// var _ fs.NodeGetxattrer = (*Dir)(nil)
// var _ fs.NodeRemovexattrer = (*Dir)(nil)
// var _ fs.NodeSetxattrer = (*Dir)(nil)

// var _ fs.NodeCopyFileRanger = (*Dir)(nil)

type Dir struct {
	fs.Inode
	fs     ninep.FileSystem
	path   string
	config *FsConfig
}

func (n *Dir) tracef(format string, values ...interface{}) {
	n.config.Loggable.Tracef(format, values...)
}
func (n *Dir) errorf(format string, values ...interface{}) {
	n.config.Loggable.Errorf(format, values...)
}

// func (n *Dir) Getattr(ctx context.Context, a *fuse.Attr) error {
func (n *Dir) Getattr(ctx context.Context, h fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	path := n.config.path(n.path)
	n.tracef("Dir.Attr(%v)\n", path)
	st, err := n.fs.Stat(ctx, path)
	if err != nil {
		return mapErr(err, syscall.EACCES)
	}
	stat := st.Sys().(ninep.Stat)
	out.Ino = stat.Qid().Path()
	// out.Size = uint64(stat.Length())
	out.Mode = uint32(st.Mode())
	mtime := st.ModTime()
	out.Atime = uint64(stat.Atime())
	out.Mtime = uint64(mtime.Unix())
	out.Ctime = uint64(mtime.Unix())
	if !n.config.doNotMapIds {
		username := stat.Uid()
		usr, err := user.Lookup(username)
		if err == nil {
			uid, err := strconv.Atoi(usr.Uid)
			n.tracef("[%v]Dir.Attr() -> %s\n", path, err)
			if err != nil {
				return mapErr(err, syscall.EINVAL)
			}
			out.Uid = uint32(uid)
		}

		groupname := stat.Gid()
		grp, err := user.LookupGroup(groupname)
		if err == nil {
			gid, err := strconv.Atoi(grp.Gid)
			n.tracef("[%v]Dir.Attr() -> %s\n", path, err)
			if err != nil {
				return mapErr(err, syscall.EINVAL)
			}
			out.Gid = uint32(gid)
		}
	}
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
	return mapErr(n.fs.WriteStat(ctx, path, st), syscall.EINVAL) // TODO: what is the proper error here?
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
	n.tracef("[%v]Dir.Rename(%#v, %#v)\n", path, oldpath, newpath)
	st := ninep.SyncStatWithName(newpath)
	return mapErr(n.fs.WriteStat(ctx, oldpath, st), syscall.EINVAL) // TODO: what is a better error message?
}

func (n *Dir) Link(ctx context.Context, target fs.InodeEmbedder, name string, out *fuse.EntryOut) (node *fs.Inode, errno syscall.Errno) {
	errno = syscall.EINVAL
	return
}

func (n *Dir) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	path := n.config.path(n.path)
	n.tracef("[%v]Dir.ReadDirAll()\n", path)
	itr, err := n.fs.ListDir(ctx, path)
	if err != nil {
		return nil, mapErr(err, 0)
	}
	defer itr.Close()
	infos, err := ninep.FileInfoSliceFromIterator(itr, -1)
	if err != nil {
		return nil, mapErr(err, 0)
	}

	entries := make([]fuse.DirEntry, len(infos))
	for i, info := range infos {
		stat := info.Sys().(ninep.Stat)
		dt := uint32(0)
		mode := info.Mode()
		if mode&os.ModeDir != 0 {
			dt = fuse.S_IFDIR
		} else if mode&os.ModeSymlink != 0 {
			dt = fuse.S_IFLNK
		} else if mode&os.ModeAppend != 0 {
			dt = fuse.S_IFIFO
		} else if mode&os.ModeType == 0 {
			dt = fuse.S_IFREG
		}
		entries[i] = fuse.DirEntry{
			Ino:  stat.Qid().Path(),
			Mode: dt,
			Name: stat.Name(),
		}
	}
	return fs.NewListDirStream(entries), 0
}

func (n *Dir) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	path := n.config.path(n.path)
	n.tracef("[%v]Dir.Lookup(%#v)\n", path, name)
	path = filepath.Join(n.path, name)
	info, err := n.fs.Stat(ctx, path)
	n.tracef(" -> %#v %v\n", path, err)
	if err != nil {
		return nil, mapErr(err, 0)
	}

	stat := info.Sys().(ninep.Stat)
	stable := fs.StableAttr{
		Mode: uint32(info.Mode()),
		Ino:  stat.Qid().Path(),
	}

	var inode *fs.Inode
	if info.IsDir() {
		inode = n.NewInode(ctx, &Dir{fs: n.fs, path: path, config: n.config}, stable)
	} else {
		inode = n.NewInode(ctx, &File{fs: n.fs, path: path, config: n.config}, stable)
	}
	return inode, 0
}

func (n *Dir) Mkdir(ctx context.Context, name string, mode uint32, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	path := n.config.path(n.path)
	n.tracef("[%v]Dir.Mkdir(%#v)\n", path, name)
	path = filepath.Join(path, name)
	mod := flagModeToMode(0, os.FileMode(mode))

	err := n.fs.MakeDir(ctx, path, mod)
	if err != nil {
		return nil, mapErr(err, syscall.EINVAL) // TODO: what's better to report here?
	}

	info, err := n.fs.Stat(ctx, path)
	n.tracef(" -> %#v %v\n", path, err)
	if err != nil {
		return nil, mapErr(err, syscall.EINVAL) // TODO: what's better to report here?
	}

	stat := info.Sys().(ninep.Stat)
	stable := fs.StableAttr{
		Mode: uint32(info.Mode()),
		Ino:  stat.Qid().Path(),
	}

	inode := n.NewInode(ctx, &Dir{fs: n.fs, path: path, config: n.config}, stable)
	return inode, 0
}

func (n *Dir) Create(ctx context.Context, name string, flags uint32, mode uint32, out *fuse.EntryOut) (node *fs.Inode, fh fs.FileHandle, fuseFlags uint32, errno syscall.Errno) {
	path := n.config.path(n.path)
	n.tracef("[%v]Dir.CreateFile(%#v)\n", path, name)
	path = filepath.Join(path, name)
	flg := flagToOpenMode(flags)
	mod := flagModeToMode(flags, os.FileMode(mode))

	if flags&syscall.O_CREAT == 0 {
		n.tracef("FUSE: did not get OpenCreate\n")
		return nil, nil, 0, syscall.EINVAL
	}

	if flags&syscall.O_DIRECTORY != 0 {
		return nil, nil, 0, syscall.EINVAL
	} else {
		h, err := n.fs.CreateFile(ctx, path, flg, mod)
		n.tracef("CreateFile(%#v, %s, %s) => %s\n", path, flg, mod, err)
		if err != nil {
			return nil, nil, 0, mapErr(err, 0)
		}

		const PurgeUBC = 1 << 31
		resFlags := uint32(PurgeUBC)

		info, err := n.fs.Stat(ctx, path)
		n.tracef(" -> %#v %v\n", path, err)
		if err != nil {
			return nil, nil, 0, mapErr(err, syscall.EINVAL) // TODO: what's better to report here?
		}

		stat := info.Sys().(ninep.Stat)
		stable := fs.StableAttr{
			Mode: uint32(info.Mode()),
			Ino:  stat.Qid().Path(),
		}

		inode := n.NewInode(ctx, &File{fs: n.fs, path: path, config: n.config}, stable)
		return inode, &FileHandle{fs: n.fs, path: path, h: h, config: n.config}, resFlags, 0
	}
}

func (n *Dir) Rmdir(ctx context.Context, name string) syscall.Errno { return n.remove(ctx, name, true) }
func (n *Dir) Unlink(ctx context.Context, name string) syscall.Errno {
	return n.remove(ctx, name, false)
}

func (n *Dir) remove(ctx context.Context, name string, isDir bool) syscall.Errno {
	path := n.config.path(n.path)
	n.tracef("[%v]Dir.Remove(%#v)\n", path, name)
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
}

var _ fs.NodeLookuper = (*File)(nil)
var _ fs.NodeOpener = (*File)(nil)
var _ fs.NodeSetattrer = (*File)(nil)
var _ fs.NodeSetxattrer = (*File)(nil)
var _ fs.NodeFsyncer = (*File)(nil)

func (n *File) tracef(format string, values ...interface{}) {
	n.config.Loggable.Tracef(format, values...)
}
func (n *File) errorf(format string, values ...interface{}) {
	n.config.Loggable.Errorf(format, values...)
}

func (n *File) Getattr(ctx context.Context, f FileHandle, out *fuse.AttrOut) syscall.Errno {
	// func (n *File) Attr(ctx context.Context, a *fuse.Attr) error {

	path := n.config.path(n.path)
	n.tracef("File.Attr(%#v)\n", path)
	st, err := n.fs.Stat(ctx, path)
	if err != nil {
		return mapErr(err, 0)
	}
	stat := st.Sys().(ninep.Stat)
	out.Ino = stat.Qid().Path()
	out.Mode = uint32(st.Mode())
	out.Atime = uint64(stat.Atime())
	out.Mtime = uint64(st.ModTime().Unix())
	out.Ctime = out.Mtime
	out.Size = stat.Length()
	if !n.config.doNotMapIds {
		username := stat.Uid()
		usr, err := user.Lookup(username)
		if err == nil {
			uid, err := strconv.Atoi(usr.Uid)
			n.tracef("File.Attr(%#v).Atoi(uid=%s) -> %s\n", path, usr.Uid, err)
			if err != nil {
				return syscall.EINVAL
			}
			out.Uid = uint32(uid)
		}

		groupname := stat.Gid()
		grp, err := user.LookupGroup(groupname)
		if err == nil {
			gid, err := strconv.Atoi(grp.Gid)
			n.tracef("File.Attr(%#v).Atoi(gid=%s) -> %s\n", path, grp.Gid, err)
			if err != nil {
				return syscall.EINVAL
			}
			out.Gid = uint32(gid)
		}
	}
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
	n.tracef("[%v]File.Setattr() %s -> %s\n", path, st, st)
	return mapErr(n.fs.WriteStat(ctx, path, st), syscall.EINVAL)
}

func (n *File) Setxattr(ctx context.Context, attr string, data []byte, flags uint32) syscall.Errno {
	n.tracef("SETXATTR %s\n", attr)
	// technically, we can't support this because we're not the underlying fs,
	// but not implementing this causes errors when using cp (in macos).
	return 0
}

func (n *File) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	n.tracef("[%v]File.Lookup(%#v)\n", n.config.path(n.path), name)
	return nil, syscall.ENOENT
}

func (n *File) Open(ctx context.Context, flags uint32) (fh fs.FileHandle, fuseFlags uint32, errno syscall.Errno) {
	// func (n *File) Open(ctx context.Context, req *fuse.OpenRequest, resp *fuse.OpenResponse) (fs.Handle, error) {
	path := n.config.path(n.path)
	n.tracef("[%v]File.Open()\n", path)
	// st, err := n.fs.Stat(ctx, path)
	// if err != nil {
	// 	return nil, 0, mapErr(err, syscall.EBADF)
	// }
	mode := ninep.OpenMode(flags) // TODO: convert openflags
	h, err := n.fs.OpenFile(ctx, path, mode)
	n.tracef("FUSE: FILE OPEN: %#v %s %v\n", path, mode, err)
	if err != nil {
		return nil, 0, mapErr(err, syscall.EINVAL)
	}

	// resp.Handle = fuse.HandleID(stat.Qid().Path())

	const PurgeUBC = 1 << 31
	// resp.Flags |= fuse.OpenDirectIO | fuse.OpenPurgeUBC
	fuseFlags = uint32(PurgeUBC)
	// TODO: support appendonly (OpenNonSeekable)
	return &FileHandle{n.fs, path, h, n.config}, fuseFlags, 0
}

func (n *File) Fsync(ctx context.Context, f fs.FileHandle, flags uint32) syscall.Errno {
	h := f.(FileHandle)
	return mapErr(h.h.Sync(), syscall.EINVAL)
}

/////////////////////////////////////////////////////////////////

type FileHandle struct {
	fs     ninep.FileSystem
	path   string
	h      ninep.FileHandle
	config *FsConfig
}

var _ fs.FileHandle = (*FileHandle)(nil)
var _ fs.FileFlusher = (*FileHandle)(nil)
var _ fs.FileReader = (*FileHandle)(nil)
var _ fs.FileReleaser = (*FileHandle)(nil)
var _ fs.FileWriter = (*FileHandle)(nil)

func (n *FileHandle) tracef(format string, values ...interface{}) {
	n.config.Loggable.Tracef(format, values...)
}
func (n *FileHandle) errorf(format string, values ...interface{}) {
	n.config.Loggable.Errorf(format, values...)
}

func (h *FileHandle) Read(ctx context.Context, dest []byte, off int64) (fuse.ReadResult, syscall.Errno) {
	// func (h *FileHandle) Read(ctx context.Context, req *fuse.ReadRequest, resp *fuse.ReadResponse) error {
	h.tracef("FUSE: READ(%d, %d)\n", len(dest), off)
	n, err := h.h.ReadAt(dest, off)
	if err == io.EOF {
		err = nil
	}
	return fuse.ReadResultData(dest[:n]), mapErr(err, syscall.EIO)
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
