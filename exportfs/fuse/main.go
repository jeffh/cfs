package fuse

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"syscall"
	"time"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	"github.com/jeffh/cfs/ninep"
)

// A helper function for starting a fuse mount point
func MountAndServeFS(f ninep.FileSystem, mountpoint string, opts ...fuse.MountOption) error {
	c, err := fuse.Mount(mountpoint, opts...)
	if err != nil {
		return err
	}
	defer c.Close()

	err = fs.Serve(c, Fs{f})
	if err != nil {
		return err
	}

	// check if the mount process has an error to report
	<-c.Ready
	if err := c.MountError; err != nil {
		return err
	}
	return nil
}

// Creates a fuse file system that exports a 9p file server as a fuse system
type Fs struct {
	Fs ninep.FileSystem
}

var _ fs.FS = (*Fs)(nil)

func (f Fs) Root() (fs.Node, error) {
	return &Dir{f.Fs, ""}, nil
}

///////////////////////////////////////////////////////////

var _ fs.Node = (*Dir)(nil)
var _ fs.NodeCreater = (*Dir)(nil)

// var _ fs.NodeForgetter = (*Dir)(nil)
var _ fs.NodeMkdirer = (*Dir)(nil)
var _ fs.NodeRemover = (*Dir)(nil)

// var _ fs.NodeRenamer = (*Dir)(nil)
var _ fs.NodeStringLookuper = (*Dir)(nil)
var _ fs.HandleReadDirAller = (*Dir)(nil)

type Dir struct {
	fs   ninep.FileSystem
	path string
}

func (n *Dir) Attr(ctx context.Context, a *fuse.Attr) error {
	fmt.Printf("[%v]Dir.Attr()\n", n.path)
	st, err := n.fs.Stat(n.path)
	if err != nil {
		return mapErr(err)
	}
	stat := st.Sys().(ninep.Stat)
	// a.Inode = stat.Qid().Path()
	a.Size = uint64(st.Size())
	a.Mode = st.Mode()
	mtime := st.ModTime()
	a.Atime = time.Unix(int64(stat.Atime()), 0)
	a.Mtime = mtime
	a.Ctime = mtime
	a.Crtime = mtime
	return nil
}

func (n *Dir) ReadDirAll(ctx context.Context) ([]fuse.Dirent, error) {
	fmt.Printf("[%v]Dir.ReadDirAll()\n", n.path)
	itr, err := n.fs.ListDir(n.path)
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
	fmt.Printf("[%v]Dir.Lookup(%#v)\n", n.path, name)
	path := filepath.Join(n.path, name)
	info, err := n.fs.Stat(path)
	fmt.Printf(" -> %#v %v\n", path, err)
	if err != nil {
		return nil, mapErr(err)
	}

	if info.IsDir() {
		return &Dir{n.fs, path}, nil
	}

	return &File{n.fs, path}, nil
}

func (n *Dir) Mkdir(ctx context.Context, req *fuse.MkdirRequest) (fs.Node, error) {
	fmt.Printf("[%v]Dir.Mkdir(%#v)\n", n.path, req.Name)
	path := filepath.Join(n.path, req.Name)
	mode := flagModeToMode(0, req.Mode)

	err := n.fs.MakeDir(path, mode)
	if err != nil {
		return nil, mapErr(err)
	}

	return &Dir{n.fs, path}, nil
}

func (n *Dir) Create(ctx context.Context, req *fuse.CreateRequest, resp *fuse.CreateResponse) (fs.Node, fs.Handle, error) {
	fmt.Printf("[%v]Dir.CreateFile(%#v)\n", n.path, req.Name)
	path := filepath.Join(n.path, req.Name)
	flg := flagToOpenMode(req.Flags)
	mode := flagModeToMode(req.Flags, req.Mode)

	if req.Flags&fuse.OpenCreate == 0 {
		return nil, nil, syscall.EINVAL
	}

	if req.Flags&fuse.OpenDirectory != 0 {
		return nil, nil, syscall.EINVAL
	} else {
		h, err := n.fs.CreateFile(path, flg, mode)
		if err != nil {
			return nil, nil, mapErr(err)
		}

		return &File{n.fs, path}, &FileHandle{n.fs, path, h}, nil
	}
}

func (n *Dir) Remove(ctx context.Context, req *fuse.RemoveRequest) error {
	fmt.Printf("[%v]Dir.Remove(%#v)\n", n.path, req.Name)
	path := filepath.Join(n.path, req.Name)
	stat, err := n.fs.Stat(path)
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
	return mapErr(n.fs.Delete(path))
}

///////////////////////////////////////////////////////////////

type File struct {
	fs   ninep.FileSystem
	path string
}

func (n *File) Attr(ctx context.Context, a *fuse.Attr) error {
	fmt.Printf("[%v]File.Attr()\n", n.path)
	st, err := n.fs.Stat(n.path)
	if err != nil {
		return mapErr(err)
	}
	stat := st.Sys().(ninep.Stat)
	// a.Inode = stat.Qid().Path()
	a.Mode = st.Mode()
	a.Atime = time.Unix(int64(stat.Atime()), 0)
	a.Mtime = st.ModTime()
	a.Size = uint64(st.Size())
	return nil
}

func (n *File) Lookup(ctx context.Context, name string) (fs.Node, error) {
	fmt.Printf("[%v]File.Lookup(%#v)\n", n.path, name)
	return nil, fuse.ENOENT
}

func (n *File) Open(ctx context.Context, req *fuse.OpenRequest, resp *fuse.OpenResponse) (fs.Handle, error) {
	fmt.Printf("[%v]File.Open()\n", n.path)
	st, err := n.fs.Stat(n.path)
	if err != nil {
		return nil, mapErr(err)
	}
	stat := st.Sys().(ninep.Stat)
	h, err := n.fs.OpenFile(n.path, ninep.OpenMode(req.Flags)) // TODO: convert openflags
	fmt.Printf("FUSE: FILE OPEN: %#v %v\n", n.path, err)
	if err != nil {
		return nil, mapErr(err)
	}
	resp.Handle = fuse.HandleID(stat.Qid().Path())
	resp.Flags = fuse.OpenDirectIO
	// TODO: support appendonly (OpenNonSeekable)
	return &FileHandle{n.fs, n.path, h}, nil
}

func (n *File) Fsync(ctx context.Context, req *fuse.FsyncRequest) error {
	return mapErr(n.fs.WriteStat(n.path, ninep.SyncStat()))
}

// func (n Node) Create(ctx context.Context, req *fuse.CreateRequest, resp *fuse.CreateResponse) (fs.Node, fs.Handle, error) {
// }

/////////////////////////////////////////////////////////////////

type FileHandle struct {
	fs   ninep.FileSystem
	path string
	h    ninep.FileHandle
}

func (h *FileHandle) Read(ctx context.Context, req *fuse.ReadRequest, resp *fuse.ReadResponse) error {
	fmt.Printf("FUSE: READ(%d) || %d\n", req.Size, len(resp.Data))
	n, err := h.h.ReadAt(resp.Data[:req.Size], req.Offset)
	resp.Data = resp.Data[:n]
	if err == io.EOF {
		err = nil
		if n == 0 {
			resp.Data = nil
		}
	}
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

///////////////////////////////////////////////////////////////////

func flagToOpenMode(flg fuse.OpenFlags) ninep.OpenMode {
	var m ninep.OpenMode
	if flg&fuse.OpenReadWrite != 0 {
		m = ninep.ORDWR
	} else if flg&fuse.OpenWriteOnly != 0 {
		m = ninep.OWRITE
	} else if flg&fuse.OpenReadOnly != 0 {
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
	if err == ninep.ErrUnsupported {
		return fuse.ENOSYS
	}
	if os.IsNotExist(err) {
		return fuse.ENOENT
	}
	if os.IsExist(err) {
		return fuse.EEXIST
	}
	if err == io.ErrNoProgress {
		return syscall.ENODATA
	}
	return err
}
