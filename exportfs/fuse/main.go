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
	"syscall"
	"time"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	"github.com/jeffh/cfs/ninep"
)

// A helper function for starting a fuse mount point
func MountAndServeFS(ctx context.Context, f ninep.FileSystem, prefix string, loggable ninep.Loggable, mountpoint string, opts ...fuse.MountOption) error {
	c, err := fuse.Mount(mountpoint, opts...)
	if err != nil {
		return err
	}

	fscfg := FsConfig{
		Loggable: loggable,
		prefix:   prefix,
	}

	subctx, cancel := context.WithCancel(ctx)
	defer cancel()
	defer c.Close()
	defer fuse.Unmount(mountpoint)

	errCh := make(chan error)
	go func() {
		errCh <- fs.Serve(c, &Fs{Fs: f, Config: fscfg})
	}()

	// check if the mount process has an error to report
	<-c.Ready
	if err := c.MountError; err != nil {
		return err
	}

	select {
	case err := <-errCh:
		return err
	case <-subctx.Done():
	}
	return nil
}

type FsConfig struct {
	// set to true to disable automatic mapping of usernames/groupnames to
	// local uids and gids
	doNotMapIds bool
	prefix      string

	ninep.Loggable
}

func (c *FsConfig) path(p string) string {
	return filepath.Join(c.prefix, p)
}

// Creates a fuse file system that exports a 9p file server as a fuse system
type Fs struct {
	Fs     ninep.FileSystem
	Config FsConfig
}

var _ fs.FS = (*Fs)(nil)

func (f *Fs) Root() (fs.Node, error) {
	return &Dir{f.Fs, "", &f.Config}, nil
}

///////////////////////////////////////////////////////////

var _ fs.Node = (*Dir)(nil)
var _ fs.NodeCreater = (*Dir)(nil)
var _ fs.NodeMkdirer = (*Dir)(nil)
var _ fs.NodeRemover = (*Dir)(nil)
var _ fs.NodeSetattrer = (*Dir)(nil)
var _ fs.NodeSetxattrer = (*Dir)(nil)
var _ fs.NodeRenamer = (*Dir)(nil)
var _ fs.NodeStringLookuper = (*Dir)(nil)
var _ fs.HandleReadDirAller = (*Dir)(nil)

type Dir struct {
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

func (n *Dir) Attr(ctx context.Context, a *fuse.Attr) error {
	path := n.config.path(n.path)
	n.tracef("Dir.Attr(%v)\n", path)
	st, err := n.fs.Stat(ctx, path)
	if err != nil {
		return mapErr(err)
	}
	stat := st.Sys().(ninep.Stat)
	a.Inode = stat.Qid().Path()
	// a.Size = uint64(stat.Length())
	a.Mode = st.Mode()
	mtime := st.ModTime()
	a.Atime = time.Unix(int64(stat.Atime()), 0)
	a.Mtime = mtime
	a.Ctime = mtime
	a.Crtime = mtime
	if !n.config.doNotMapIds {
		username := stat.Uid()
		usr, err := user.Lookup(username)
		if err == nil {
			uid, err := strconv.Atoi(usr.Uid)
			n.tracef("[%v]Dir.Attr() -> %s\n", path, err)
			if err != nil {
				return err
			}
			a.Uid = uint32(uid)
		}

		groupname := stat.Gid()
		grp, err := user.LookupGroup(groupname)
		if err == nil {
			gid, err := strconv.Atoi(grp.Gid)
			n.tracef("[%v]Dir.Attr() -> %s\n", path, err)
			if err != nil {
				return err
			}
			a.Gid = uint32(gid)
		}
	}
	return nil
}

func (n *Dir) Setattr(ctx context.Context, req *fuse.SetattrRequest, resp *fuse.SetattrResponse) error {
	st := ninep.SyncStat()
	if req.Valid.Size() {
		st.SetLength(req.Size)
	}
	if req.Valid.Atime() {
		st.SetAtime(uint32(req.Atime.Unix()))
	}
	if req.Valid.Mtime() {
		st.SetMtime(uint32(req.Mtime.Unix()))
	}
	if req.Valid.Mode() {
		st.SetMode(ninep.ModeFromOS(req.Mode))
	}
	path := n.config.path(n.path)
	return mapErr(n.fs.WriteStat(ctx, path, st))
}

func (n *Dir) Setxattr(ctx context.Context, req *fuse.SetxattrRequest) error {
	// technically, we can't support this because we're not the underlying fs,
	// but not implementing this causes errors when using cp (in macos).
	return nil
}

func (n *Dir) Rename(ctx context.Context, req *fuse.RenameRequest, newDir fs.Node) error {
	nd := newDir.(*Dir)
	path := n.config.path(n.path)
	newpath := nd.config.path(nd.path)

	oldpath := filepath.Join(path, req.OldName)
	newpath = filepath.Join(newpath, req.NewName)
	n.tracef("[%v]Dir.Rename(%#v, %#v)\n", path, oldpath, newpath)
	st := ninep.SyncStatWithName(newpath)
	return mapErr(n.fs.WriteStat(ctx, oldpath, st))
}

func (n *Dir) ReadDirAll(ctx context.Context) ([]fuse.Dirent, error) {
	path := n.config.path(n.path)
	n.tracef("[%v]Dir.ReadDirAll()\n", path)
	itr, err := n.fs.ListDir(ctx, path)
	if err != nil {
		return nil, mapErr(err)
	}
	defer itr.Close()
	infos, err := ninep.FileInfoSliceFromIterator(itr, -1)
	if err != nil {
		return nil, mapErr(err)
	}

	entries := make([]fuse.Dirent, len(infos))
	for i, info := range infos {
		stat := info.Sys().(ninep.Stat)
		dt := fuse.DT_Unknown
		mode := info.Mode()
		if mode&os.ModeDir != 0 {
			dt = fuse.DT_Dir
		} else if mode&os.ModeSymlink != 0 {
			dt = fuse.DT_Link
		} else if mode&os.ModeAppend != 0 {
			dt = fuse.DT_FIFO
		} else if mode&os.ModeType == 0 {
			dt = fuse.DT_File
		}
		entries[i] = fuse.Dirent{
			Inode: stat.Qid().Path(),
			Type:  dt,
			Name:  stat.Name(),
		}
	}
	return entries, nil
}

func (n *Dir) Lookup(ctx context.Context, name string) (fs.Node, error) {
	path := n.config.path(n.path)
	n.tracef("[%v]Dir.Lookup(%#v)\n", path, name)
	path = filepath.Join(n.path, name)
	info, err := n.fs.Stat(ctx, path)
	n.tracef(" -> %#v %v\n", path, err)
	if err != nil {
		return nil, mapErr(err)
	}

	if info.IsDir() {
		return &Dir{n.fs, path, n.config}, nil
	}

	return &File{n.fs, path, n.config}, nil
}

func (n *Dir) Mkdir(ctx context.Context, req *fuse.MkdirRequest) (fs.Node, error) {
	path := n.config.path(n.path)
	n.tracef("[%v]Dir.Mkdir(%#v)\n", path, req.Name)
	path = filepath.Join(path, req.Name)
	mode := flagModeToMode(0, req.Mode)

	err := n.fs.MakeDir(ctx, path, mode)
	if err != nil {
		return nil, mapErr(err)
	}

	return &Dir{n.fs, path, n.config}, nil
}

func (n *Dir) Create(ctx context.Context, req *fuse.CreateRequest, resp *fuse.CreateResponse) (fs.Node, fs.Handle, error) {
	path := n.config.path(n.path)
	n.tracef("[%v]Dir.CreateFile(%#v)\n", path, req.Name)
	path = filepath.Join(path, req.Name)
	flg := flagToOpenMode(req.Flags)
	mode := flagModeToMode(req.Flags, req.Mode)

	if req.Flags&fuse.OpenCreate == 0 {
		fmt.Printf("FUSE: did not get OpenCreate\n")
		return nil, nil, syscall.EINVAL
	}

	if req.Flags&fuse.OpenDirectory != 0 {
		return nil, nil, syscall.EINVAL
	} else {
		h, err := n.fs.CreateFile(ctx, path, flg, mode)
		fmt.Printf("CreateFile(%#v, %s, %s) => %s\n", path, flg, mode, err)
		if err != nil {
			return nil, nil, mapErr(err)
		}

		// resp.Flags |= fuse.OpenDirectIO | fuse.OpenPurgeUBC
		resp.Flags |= fuse.OpenPurgeUBC

		return &File{n.fs, path, n.config}, &FileHandle{n.fs, path, h, n.config}, nil
	}
}

func (n *Dir) Remove(ctx context.Context, req *fuse.RemoveRequest) error {
	path := n.config.path(n.path)
	n.tracef("[%v]Dir.Remove(%#v)\n", path, req.Name)
	path = filepath.Join(path, req.Name)
	stat, err := n.fs.Stat(ctx, path)
	if err != nil {
		return mapErr(err)
	}
	if req.Dir {
		if !stat.IsDir() {
			return syscall.ENOTDIR
		}
	} else {
		if stat.IsDir() {
			return syscall.EISDIR
		}
	}
	return mapErr(n.fs.Delete(ctx, path))
}

///////////////////////////////////////////////////////////////

type File struct {
	fs     ninep.FileSystem
	path   string
	config *FsConfig
}

var _ fs.Node = (*File)(nil)
var _ fs.NodeStringLookuper = (*File)(nil)
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

func (n *File) Attr(ctx context.Context, a *fuse.Attr) error {
	path := n.config.path(n.path)
	n.tracef("File.Attr(%#v)\n", path)
	st, err := n.fs.Stat(ctx, path)
	if err != nil {
		return mapErr(err)
	}
	stat := st.Sys().(ninep.Stat)
	a.Inode = stat.Qid().Path()
	a.Mode = st.Mode()
	a.Atime = time.Unix(int64(stat.Atime()), 0)
	a.Mtime = st.ModTime()
	a.Ctime = a.Mtime
	a.Crtime = a.Mtime
	a.Size = stat.Length()
	if !n.config.doNotMapIds {
		username := stat.Uid()
		usr, err := user.Lookup(username)
		if err == nil {
			uid, err := strconv.Atoi(usr.Uid)
			n.tracef("File.Attr(%#v).Atoi(uid=%s) -> %s\n", path, usr.Uid, err)
			if err != nil {
				return err
			}
			a.Uid = uint32(uid)
		}

		groupname := stat.Gid()
		grp, err := user.LookupGroup(groupname)
		if err == nil {
			gid, err := strconv.Atoi(grp.Gid)
			n.tracef("File.Attr(%#v).Atoi(gid=%s) -> %s\n", path, grp.Gid, err)
			if err != nil {
				return err
			}
			a.Gid = uint32(gid)
		}
	}
	return nil
}

func (n *File) Setattr(ctx context.Context, req *fuse.SetattrRequest, resp *fuse.SetattrResponse) error {
	st := ninep.SyncStat()
	if req.Valid.Size() {
		st.SetLength(req.Size)
	}
	if req.Valid.Atime() {
		st.SetAtime(uint32(req.Atime.Unix()))
	}
	if req.Valid.Mtime() {
		st.SetMtime(uint32(req.Mtime.Unix()))
	}
	if req.Valid.Mode() {
		st.SetMode(ninep.ModeFromOS(req.Mode))
	}
	path := n.config.path(n.path)
	n.tracef("[%v]File.Setattr() %s -> %s\n", path, req.Valid, st)
	return mapErr(n.fs.WriteStat(ctx, path, st))
}

func (n *File) Setxattr(ctx context.Context, req *fuse.SetxattrRequest) error {
	fmt.Printf("SETXATTR %s\n", req)
	// technically, we can't support this because we're not the underlying fs,
	// but not implementing this causes errors when using cp (in macos).
	return nil
}

func (n *File) Lookup(ctx context.Context, name string) (fs.Node, error) {
	n.tracef("[%v]File.Lookup(%#v)\n", n.config.path(n.path), name)
	return nil, fuse.ENOENT
}

func (n *File) Open(ctx context.Context, req *fuse.OpenRequest, resp *fuse.OpenResponse) (fs.Handle, error) {
	path := n.config.path(n.path)
	n.tracef("[%v]File.Open()\n", path)
	st, err := n.fs.Stat(ctx, path)
	if err != nil {
		return nil, mapErr(err)
	}
	stat := st.Sys().(ninep.Stat)
	mode := ninep.OpenMode(req.Flags & fuse.OpenAccessModeMask) // TODO: convert openflags
	h, err := n.fs.OpenFile(ctx, path, mode)
	n.tracef("FUSE: FILE OPEN: %#v %s %v\n", path, mode, err)
	if err != nil {
		return nil, mapErr(err)
	}
	resp.Handle = fuse.HandleID(stat.Qid().Path())
	// resp.Flags |= fuse.OpenDirectIO | fuse.OpenPurgeUBC
	resp.Flags |= fuse.OpenPurgeUBC
	// TODO: support appendonly (OpenNonSeekable)
	return &FileHandle{n.fs, path, h, n.config}, nil
}

func (n *File) Fsync(ctx context.Context, req *fuse.FsyncRequest) error {
	path := n.config.path(n.path)
	return mapErr(n.fs.WriteStat(ctx, path, ninep.SyncStat()))
}

// func (n Node) Create(ctx context.Context, req *fuse.CreateRequest, resp *fuse.CreateResponse) (fs.Node, fs.Handle, error) {
// }

/////////////////////////////////////////////////////////////////

type FileHandle struct {
	fs     ninep.FileSystem
	path   string
	h      ninep.FileHandle
	config *FsConfig
}

var _ fs.Handle = (*FileHandle)(nil)
var _ fs.HandleFlusher = (*FileHandle)(nil)
var _ fs.HandleReader = (*FileHandle)(nil)
var _ fs.HandleReleaser = (*FileHandle)(nil)
var _ fs.HandleWriter = (*FileHandle)(nil)
var _ fs.NodeFsyncer = (*FileHandle)(nil)

func (n *FileHandle) tracef(format string, values ...interface{}) {
	n.config.Loggable.Tracef(format, values...)
}
func (n *FileHandle) errorf(format string, values ...interface{}) {
	n.config.Loggable.Errorf(format, values...)
}

func (h *FileHandle) Read(ctx context.Context, req *fuse.ReadRequest, resp *fuse.ReadResponse) error {
	h.tracef("FUSE: READ(%d) || %d || %d << %d\n", req.Size, len(resp.Data), cap(resp.Data), req.Offset)
	h.tracef("FUSE: READ %#v\n", h.h)
	resp.Data = resp.Data[:req.Size]
	n, err := h.h.ReadAt(resp.Data, req.Offset)
	if err == io.EOF {
		err = nil
	}
	resp.Data = resp.Data[:n]
	return mapErr(err)
}

func (h *FileHandle) Write(ctx context.Context, req *fuse.WriteRequest, resp *fuse.WriteResponse) error {
	n, err := h.h.WriteAt(req.Data, req.Offset)
	resp.Size = n
	return mapErr(err)
}

func (h *FileHandle) Release(ctx context.Context, req *fuse.ReleaseRequest) error {
	return mapErr(h.h.Close())
}

func (h *FileHandle) Fsync(ctx context.Context, req *fuse.FsyncRequest) error {
	return mapErr(h.h.Sync())
}

func (h *FileHandle) Flush(ctx context.Context, req *fuse.FlushRequest) error {
	return mapErr(h.h.Sync())
}

///////////////////////////////////////////////////////////////////

func flagToOpenMode(flg fuse.OpenFlags) ninep.OpenMode {
	var m ninep.OpenMode
	if flg&fuse.OpenAccessModeMask == fuse.OpenReadWrite {
		m = ninep.ORDWR
	} else if flg&fuse.OpenAccessModeMask == fuse.OpenWriteOnly {
		m = ninep.OWRITE
	} else if flg&fuse.OpenWriteOnly == fuse.OpenReadOnly {
		m = ninep.OREAD
	}

	if flg&fuse.OpenTruncate != 0 {
		m |= ninep.OTRUNC
	}

	return m
}

func flagModeToMode(flg fuse.OpenFlags, mode os.FileMode) ninep.Mode {
	m := ninep.ModeFromOS(mode)

	if flg&fuse.OpenAppend != 0 {
		m |= ninep.M_APPEND
	}
	if flg&fuse.OpenExclusive != 0 {
		m |= ninep.M_EXCL
	}
	return m
}

func mapErr(err error) error {
	if err == io.EOF {
		return fuse.EIO
	}
	if err == ninep.ErrUnsupported {
		return fuse.ENOSYS
	}
	if os.IsNotExist(err) {
		return fuse.ENOENT
	}
	if errors.Is(err, os.ErrExist) {
		return fuse.EEXIST
	}
	if err == io.ErrNoProgress {
		return syscall.ENODATA
	}
	if err == os.ErrPermission {
		return fuse.EPERM
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
	return err
}
