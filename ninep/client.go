package ninep

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"iter"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

var (
	ErrListingOnNonDir   = errors.New("cannot list files for path that's not a directory")
	ErrOpenDirNotAllowed = errors.New("cannot open directories")
)

type cltTransaction struct {
	req *cltRequest
	res *cltResponse
	ch  chan cltChResponse
}

func createClientTransaction(t Tag, maxMsgSize uint32) cltTransaction {
	req := createClientRequest(t, maxMsgSize)
	res := createClientResponse(maxMsgSize)
	return cltTransaction{
		req: &req,
		res: &res,
	}
}

func (t *cltTransaction) reset() {
	t.req.reset()
	t.res.reset()
}

func (t *cltTransaction) clone() *cltTransaction {
	tp := createClientTransaction(t.req.tag, uint32(cap(t.req.outMsg)))
	n := copy(tp.req.outMsg, t.req.outMsg)
	tp.req.outMsg = tp.req.outMsg[:n]
	n = copy(tp.res.inMsg, t.res.inMsg)
	tp.res.inMsg = tp.res.inMsg[:n]
	return &tp
}

func (t *cltTransaction) sendAndReceive(rw net.Conn) (Message, error) {
	rw.SetDeadline(time.Time{})
	err := t.req.writeRequest(rw)
	if err != nil {
		return nil, fmt.Errorf("Failed to write %s: %w", t.req.requestType(), err)
	}
	err = t.res.readReply(rw)
	if err != nil {
		return nil, fmt.Errorf("Failed to read reply to %s: %w", t.req.requestType(), err)
	}
	return t.res.Reply(), nil
}

type cltChResponse struct {
	res *cltResponse
	err error
}

// Represents the interaction interface for a client implementation of the 9p protocol
type Client interface {
	io.Closer
	// 9p Client Protocol
	Clunk(f Fid) error                                                                     // close file
	Create(f Fid, name string, perm Mode, mode OpenMode) (q Qid, iounit uint32, err error) // create file
	Open(f Fid, flag OpenMode) (q Qid, iounit uint32, err error)                           // open file
	Read(f Fid, p []byte, offset uint64) (int, error)                                      // read opened file or directory
	Remove(f Fid) error                                                                    // delete file or directory, then clunk the fid
	Stat(f Fid) (Stat, error)                                                              // file info of a file or directory
	Walk(f, newF Fid, path []string) ([]Qid, error)                                        // traverse to a new file or directory
	Write(f Fid, p []byte, offset uint64) (int, error)                                     // write to an opened file
	WriteStat(f Fid, s Stat) error                                                         // update file info a file or directory

	// Experimental: Implementation specific construction right now. Although may be better to unify
	Fs() (*FileSystemProxy, error)

	// Absent:
	// Auth() - assumed the constructor will manage this
	// Attach() - assumed the constructor will manage this
}

/////////////////////////////////////////////////////////

// Provides operations on a Fid. Caller must understand the state of the Fid to
// perform the correct operations.
type FileProxy struct {
	// required
	fs  *FileSystemProxy
	fid Fid
	qid Qid

	// cache
	st Stat
}

var _ FileHandle = (*FileProxy)(nil)
var _ TraversableFile = (*FileProxy)(nil)

// Returns the server's abbreviated metadata about the file or directory this file proxy represents.
func (f *FileProxy) Type() QidType { return f.qid.Type() }

// Returns a new FileProxy of the new path relative to this file. It is the
// caller responsibility to Close() the returned file proxy.
func (f *FileProxy) TraverseFile(path string) (*FileProxy, error) {
	fid := f.fs.allocFid()
	qid, err := f.fs.walk(f.fid, path)
	if err != nil {
		return nil, err
	}
	return &FileProxy{f.fs, fid, qid, nil}, nil
}

func (f *FileProxy) Traverse(ctx context.Context, path string) (TraversableFile, error) {
	return f.TraverseFile(path)
}

// Returns file info of the fid. Caches the value locally and uses that when available
func (f *FileProxy) FileInfo() (os.FileInfo, error) {
	st, err := f.Stat()
	if err != nil {
		return nil, err
	}
	return st.FileInfo(), err
}

// Returns file info of the fid. Unlike FileProxy.Stat(), this always fetches
// from the server The new value will still be cached.
func (f *FileProxy) FetchFileInfo() (os.FileInfo, error) {
	st, err := f.FetchStat()
	if err != nil {
		return nil, err
	}
	return st.FileInfo(), err
}

// Returns file info of the fid. Caches the value locally and uses that when available
func (f *FileProxy) Stat() (Stat, error) {
	if f.st == nil {
		st, err := f.fs.c.Stat(f.fid)
		if err != nil {
			return nil, err
		}
		f.st = st
	}
	return f.st, nil
}

// Returns file info of the fid. Unlike FileProxy.Stat(), this always fetches
// from the server The new value will still be cached.
func (f *FileProxy) FetchStat() (Stat, error) {
	st, err := f.fs.c.Stat(f.fid)
	if err != nil {
		return nil, err
	}
	f.st = st
	return st, nil
}

func (f *FileProxy) WriteStat(st Stat) error {
	err := f.fs.c.WriteStat(f.fid, st)
	if err == nil {
		f.st = nil
	}
	return err
}

// Alias to Create() with M_DIR always set. Closes the file that is created
// since it should be empty and writes are disallowed against directories.
func (f *FileProxy) CreateDir(name string, mode Mode) (*FileProxy, error) {
	fp, err := f.CreateFile(name, ORDWR, mode|M_DIR)
	if err == nil {
		fp.Close()
	}
	return fp, err
}

// Creates a new subdirectory with the given file mode
func (f *FileProxy) MakeDir(name string, mode Mode) (TraversableFile, error) {
	return f.CreateDir(name, mode)
}

// Creates a new subdirectory or file. Returns that created file's FileProxy.
func (f *FileProxy) CreateFile(name string, flag OpenMode, mode Mode) (*FileProxy, error) {
	fs := f.fs
	fid := fs.allocFid()

	qids, err := fs.c.Walk(f.fid, fid, nil)
	if err != nil {
		return nil, err
	}

	var h *FileProxy
	_, _, err = fs.c.Create(fid, name, mode, flag)
	if err == nil {
		h = &FileProxy{fs, fid, qids[len(qids)-1], nil}
	} else {
		fs.c.Clunk(fid)
		fs.releaseFid(fid)
	}
	return h, err
}

// Creates a new file or directory underneath this currrent directory. Returns that created file's FileProxy.
func (f *FileProxy) Create(name string, flag OpenMode, mode Mode) (TraversableFile, error) {
	return f.CreateFile(name, flag, mode)
}

// Opens a file for reading/writing. Use only if you used FileSystemProxy.Traverse()
func (f *FileProxy) Open(flag OpenMode) error {
	qid, _, err := f.fs.c.Open(f.fid, flag)
	if err == nil {
		if qid.Type().IsDir() {
			err = ErrOpenDirNotAllowed
		}
	}
	return err
}

// Produces a reader that starts at given offset. Assumes you've called Create() or Open() with read first
func (f *FileProxy) Reader(start int64) io.Reader { return ReaderStartingAt(f, start) }

// Produces a write that starts at given offset. Assumes you've called Create() or Open() with write first
func (f *FileProxy) Writer(start int64) io.Writer { return WriterStartingAt(f, start) }

// Reads an open file.
func (f *FileProxy) ReadAt(p []byte, offset int64) (int, error) {
	size, err := f.fs.c.Read(f.fid, p, uint64(offset))
	if size == 0 && err == nil {
		err = io.EOF
	}
	return int(size), err
}

// Writes to an open file.
func (f *FileProxy) WriteAt(p []byte, offset int64) (int, error) {
	size, err := f.fs.c.Write(f.fid, p, uint64(offset))
	return int(size), err
}

// Alias to f.WriteStat(SyncStat())
func (f *FileProxy) Sync() error { return f.fs.c.WriteStat(f.fid, SyncStat()) }

// Closes an open file
func (f *FileProxy) Close() error {
	err := f.fs.c.Clunk(f.fid)
	f.fs.releaseFid(f.fid)
	return err
}

// Deletes the file or directory that this FileProxy points to. Implies Close()
func (f *FileProxy) Delete() error {
	err := f.fs.c.Remove(f.fid)
	f.fs.releaseFid(f.fid)
	return err
}

func (fp *FileProxy) eachStat(yield func(Stat, error) bool) {
	buf := make([]byte, fp.fs.c.MaxMessageSize())
	var rst []byte
	var offset int
	readStat := func(fs *FileSystemProxy, b []byte) (Stat, []byte, error) {
		st := Stat(b)
		size := st.Size()
		if int(size) > len(b) {
			fs.c.Errorf("Invalid format while reading dir: (wanted: %d bytes, had: %d bytes)", size, len(b))
			return nil, b, ErrBadFormat
		}
		st = Stat(b[:size+2])
		return st, b[2+size:], nil
	}

	for {
		var err error
		var outSt Stat
		if len(rst) > 0 {
			var st Stat
			st, rst, err = readStat(fp.fs, rst)
			if err != nil {
				yield(nil, err)
				return
			}
			outSt = st
		}

		if outSt == nil {
			var n int
			n, err = fp.ReadAt(buf, int64(offset))
			if n > 0 {
				rst = buf[:n]
				var st Stat
				st, rst, err = readStat(fp.fs, rst)
				if err != nil {
					yield(nil, err)
					return
				}
				outSt = st
				offset += n
			}
		}

		// EOF indicates the end
		if errors.Is(err, io.EOF) {
			yield(outSt, nil)
			return
		} else if !yield(outSt, err) {
			return
		}
	}
}

// List files in a directory
func (f *FileProxy) ListDirStat() iter.Seq2[Stat, error] {
	return func(yield func(Stat, error) bool) {
		qid, _, err := f.fs.c.Open(f.fid, OREAD)
		if err == nil {
			if !qid.Type().IsDir() {
				yield(nil, ErrListingOnNonDir)
				return
				// } else {
				// 	return itr, nil
			}
		} else {
			yield(nil, err)
			return
		}

		f.eachStat(yield)
	}
}

// Lists files in a directory
func (f *FileProxy) ListDir() iter.Seq2[fs.FileInfo, error] {
	return func(yield func(fs.FileInfo, error) bool) {
		for stat, err := range f.ListDirStat() {
			if !yield(stat.FileInfo(), err) {
				break
			}
		}
	}
}

///////////////////////

// The contract that FileSystemProxy expects from the underlying 9p client
// Also provides a higher-level API to use a 9p Client
//
// FileSystemProxyClient assumes complete ownership of Client since it
// allocates and tracks it own Fids.
//
// Using most of the high-level APIs here has a high-churn of Fids, (since each
// call presumes a 9p Walk). If you want to have higher performance, use
// Traverse() and reuse the FileProxy references for repeated operations on the
// same file and minimize repeated Walk-ing to the same file.
type FileSystemProxyClient interface {
	Client

	// Extra - Max size the client uses for messaging. FS Proxy uses it for buffer sizes
	MaxMessageSize() uint32

	// Logging
	Errorf(format string, values ...interface{})
	Tracef(format string, values ...interface{})
}

// A 9p client's representation a file on a remote FileSystem
//
// Using this can be more efficient that using paths all the time, otherwise
// the client effectively cd-s to the given path each time which can be slower.
type FileSystemProxy struct {
	c     FileSystemProxyClient
	rootF Fid
	rootQ Qid

	mut      sync.Mutex
	usedFids map[Fid]bool
}

var _ FileSystem = (*FileSystemProxy)(nil)

func (fs *FileSystemProxy) allocFid() Fid {
	f := Fid(0)
	fs.mut.Lock()
	if fs.usedFids == nil {
		fs.usedFids = make(map[Fid]bool)
		fs.usedFids[fs.rootF] = true
	}
	for i := Fid(2); i < MAX_FID; i++ {
		if _, ok := fs.usedFids[i]; !ok {
			f = i
			fs.usedFids[f] = true
			break
		}
	}
	fs.mut.Unlock()
	return f
}
func (fs *FileSystemProxy) releaseFid(f Fid) {
	fs.mut.Lock()
	delete(fs.usedFids, f)
	fs.mut.Unlock()
}

func (fs *FileSystemProxy) walk(fid Fid, path string) (Qid, error) {
	parts := PathSplit(path) //[1:]
	qids, err := fs.c.Walk(fs.rootF, fid, parts)
	if err != nil {
		// Best attempt to notify server that we're dropping this fid
		fs.c.Clunk(fid)
		return nil, err
	}
	if len(qids) < len(parts) {
		// TODO: it would be nice to know how many dirs we traversed
		return nil, os.ErrNotExist
	}
	return qids[len(qids)-1], nil
}

//////////

func (fs *FileSystemProxy) MakeDir(ctx context.Context, path string, mode Mode) error {
	fid := fs.allocFid()

	prefix := ""
	filename := path
	i := strings.LastIndex(path, "/")
	if i != -1 {
		prefix = path[:i]
		filename = path[i+1:]
	} else {
		prefix = "/"
	}
	if _, err := fs.walk(fid, prefix); err != nil {
		return err
	}
	_, _, err := fs.c.Create(fid, filename, mode|M_DIR, ORDWR)
	fs.c.Clunk(fid)
	fs.releaseFid(fid)
	return err
}
func (fs *FileSystemProxy) CreateFile(ctx context.Context, path string, flag OpenMode, mode Mode) (FileHandle, error) {
	fid := fs.allocFid()

	prefix := ""
	filename := path
	i := strings.LastIndex(path, "/")
	if i != -1 {
		prefix = path[:i]
		filename = path[i+1:]
	}
	qid, err := fs.walk(fid, prefix)
	if err != nil {
		return nil, err
	}
	var h FileHandle
	_, _, err = fs.c.Create(fid, filename, mode, flag)
	if err == nil {
		h = &FileProxy{fs, fid, qid, nil}
	} else {
		fs.c.Clunk(fid)
		fs.releaseFid(fid)
	}
	return h, err
}
func (fs *FileSystemProxy) OpenFile(ctx context.Context, path string, flag OpenMode) (FileHandle, error) {
	fid := fs.allocFid()
	_, err := fs.walk(fid, path)
	if err != nil {
		return nil, err
	}
	var h FileHandle
	qid, _, err := fs.c.Open(fid, flag)
	if err == nil {
		if qid.Type().IsDir() {
			fs.c.Clunk(fid)
			fs.releaseFid(fid)
			err = ErrOpenDirNotAllowed
		} else {
			h = &FileProxy{fs, fid, qid, nil}
		}
	} else {
		fs.c.Clunk(fid)
		fs.releaseFid(fid)
	}
	return h, err
}

func (fsp *FileSystemProxy) ListDir(ctx context.Context, path string) iter.Seq2[fs.FileInfo, error] {
	return statToFileInfo(fsp.ListDirStat(path))
}

func (fs *FileSystemProxy) ListDirStat(path string) iter.Seq2[Stat, error] {
	return func(yield func(Stat, error) bool) {
		fid := fs.allocFid()
		fs.c.Tracef("ListDir(%#v) %s", path, fid)
		q, err := fs.walk(fid, path)
		if err != nil {
			fs.releaseFid(fid)
			yield(nil, err)
			return
		}

		qid, _, err := fs.c.Open(fid, OREAD)
		if err == nil {
			if !qid.Type().IsDir() {
				yield(nil, ErrListingOnNonDir)
				return
				// } else {
				// 	return itr, nil
			}
		} else {
			yield(nil, err)
			return
		}
		fp := FileProxy{fs, fid, q, nil}
		fp.eachStat(yield)
	}
}
func (fs *FileSystemProxy) Stat(ctx context.Context, path string) (os.FileInfo, error) {
	fid := fs.allocFid()
	defer fs.releaseFid(fid)
	if _, err := fs.walk(fid, path); err != nil {
		return nil, err
	}
	st, err := fs.c.Stat(fid)
	fs.c.Clunk(fid)
	return st.FileInfo(), err
}
func (fs *FileSystemProxy) WriteStat(ctx context.Context, path string, s Stat) error {
	fid := fs.allocFid()
	defer fs.releaseFid(fid)
	if _, err := fs.walk(fid, path); err != nil {
		return err
	}
	err := fs.c.WriteStat(fid, s)
	fs.c.Clunk(fid)
	return err
}
func (fs *FileSystemProxy) Delete(ctx context.Context, path string) error {
	fid := fs.allocFid()
	defer fs.releaseFid(fid)
	if _, err := fs.walk(fid, path); err != nil {
		return err
	}
	// regardless of this call, the server should drop the fid
	return fs.c.Remove(fid)
}

// Walks to a given path an returns a FileProxy to that path. It is expected
// for the caller to call Close on the returned file proxy.
func (fs *FileSystemProxy) TraverseFile(path string) (*FileProxy, error) {
	fid := fs.allocFid()
	qid, err := fs.walk(fid, path)
	if err != nil {
		return nil, err
	}
	return &FileProxy{fs, fid, qid, nil}, nil
}

func (fs *FileSystemProxy) Traverse(ctx context.Context, path string) (TraversableFile, error) {
	f, err := fs.TraverseFile(path)
	return f, err
}

/////////////////////////////////////////////////////

var _ Traversable = (*FileSystemProxy)(nil)
var _ TraversableFile = (*FileProxy)(nil)

type TraversableFileSystem interface {
	FileSystem
	Traversable
}

type Traversable interface {
	Traverse(ctx context.Context, path string) (TraversableFile, error)
}

// TODO: think about this interface more
type TraversableFile interface {
	Traversable
	FileHandle

	Type() QidType
	FileInfo() (os.FileInfo, error)
	Stat() (Stat, error)
	WriteStat(st Stat) error
	MakeDir(name string, mode Mode) (TraversableFile, error)
	Create(name string, flag OpenMode, mode Mode) (TraversableFile, error)
	Open(flag OpenMode) error
	Reader(start int64) io.Reader
	Writer(start int64) io.Writer
	Delete() error

	ListDir() iter.Seq2[fs.FileInfo, error]
}

// An inefficient, but flexible implementation of TraversableFile
type BasicTraversableFile struct {
	FS   FileSystem
	Path string
	Info os.FileInfo
	St   Stat

	FileHandle
}

var _ TraversableFile = (*BasicTraversableFile)(nil)

// Returns a new TraversableFile from a given filesystem and path.
//
// This is an easy way to add TraversableFileSystem support to a FileSystem by
// delegating to to this function.
func BasicTraverse(ctx context.Context, fs FileSystem, path string) (TraversableFile, error) {
	info, err := fs.Stat(ctx, path)
	if err != nil {
		return nil, err
	}
	st := StatFromFileInfo(info)
	f := &BasicTraversableFile{
		FS:   fs,
		Path: path,
		Info: info,
		St:   st,
	}
	return f, nil
}

// Traverses down to a possible file or directory.
// An error is returned if the file or directory does not exist.
func (t *BasicTraversableFile) Traverse(ctx context.Context, path string) (TraversableFile, error) {
	fpath := filepath.Join(t.Path, path)
	info, err := t.FS.Stat(ctx, fpath)
	if err != nil {
		return nil, err
	}

	st := StatFromFileInfo(info)

	f := &BasicTraversableFile{
		FS:   t.FS,
		Path: fpath,
		St:   st,
		Info: info,
	}
	return f, nil
}

// Returns server's metadata about this file or directory.
func (t *BasicTraversableFile) Type() QidType {
	if t.St == nil {
		return 0
	}
	return t.St.Qid().Type()
}

// Returns os.FileInfo metadata about this file or directory
func (t *BasicTraversableFile) FileInfo() (os.FileInfo, error) {
	if t.Info == nil {
		return nil, os.ErrNotExist
	}
	return t.Info, nil
}

// Returns Plan9 Stat metadata about this file or directory
func (t *BasicTraversableFile) Stat() (Stat, error) {
	if t.St == nil {
		return nil, os.ErrNotExist
	}
	return t.St, nil
}

// Updates the server's file or directory metadata
func (t *BasicTraversableFile) WriteStat(s Stat) error {
	ctx := context.Background()
	err := t.FS.WriteStat(ctx, t.Path, s)
	if err != nil {
		return err
	}
	info, err := t.FS.Stat(ctx, t.Path)
	if err != nil {
		return err
	}
	t.Info = info
	t.St = StatFromFileInfo(info)
	return nil
}

// Create a subdirectory under this directory.
//
// Calling this from a non-directory has undefined behavior.
func (t *BasicTraversableFile) MakeDir(name string, mode Mode) (TraversableFile, error) {
	ctx := context.Background()
	fpath := filepath.Join(t.Path, name)
	err := t.FS.MakeDir(ctx, fpath, mode)
	if err != nil {
		return nil, err
	}

	info, err := t.FS.Stat(ctx, fpath)
	if err != nil {
		return nil, err
	}

	f := &BasicTraversableFile{
		FS:   t.FS,
		Path: fpath,
		St:   info.Sys().(Stat),
		Info: info,
	}
	return f, nil
}

// Creates a file under this directory
//
// Calling this from a non-directory has undefined behavior
func (t *BasicTraversableFile) Create(name string, flag OpenMode, mode Mode) (TraversableFile, error) {
	ctx := context.Background()
	fpath := filepath.Join(t.Path, name)
	h, err := t.FS.CreateFile(ctx, fpath, flag, mode)
	if err != nil {
		return nil, err
	}

	info, err := t.FS.Stat(ctx, fpath)
	if err != nil {
		h.Close()
		return nil, err
	}

	f := &BasicTraversableFile{
		FS:         t.FS,
		Path:       fpath,
		St:         info.Sys().(Stat),
		Info:       info,
		FileHandle: h,
	}
	return f, nil
}

// Opens this file for reading. Required for Reader(), Writer() methods.
//
// Calling this from a non-directory has undefined behavior
func (t *BasicTraversableFile) Open(flag OpenMode) error {
	ctx := context.Background()
	h, err := t.FS.OpenFile(ctx, t.Path, flag)
	if err != nil {
		return err
	}

	var info os.FileInfo
	if t.Info == nil {
		info, err = t.FS.Stat(ctx, t.Path)
		if err != nil {
			h.Close()
			return err
		}
		t.Info = info
		t.St = info.Sys().(Stat)
	}

	t.FileHandle = h
	return nil
}

// Returns a reader from the beginning of a file that's opened.
// Open or Create must be called before calling this.
func (t *BasicTraversableFile) Reader(start int64) io.Reader { return Reader(t.FileHandle) }

// Returns a writer from the beginning of a file that's opened.
// Open or Create must be called before calling this.
func (t *BasicTraversableFile) Writer(start int64) io.Writer { return Writer(t.FileHandle) }

// Deletes the current file or directory.
//
// Returns any error reported by the server.
func (t *BasicTraversableFile) Delete() error {
	ctx := context.Background()
	err := t.FS.Delete(ctx, t.Path)
	if err == nil {
		t.FileHandle = nil
		t.St = nil
		t.Info = nil
	}
	return nil
}

// Lists files and directories under this current directory
//
// Calling this from a non-directory has undefined behavior
func (t *BasicTraversableFile) ListDir() iter.Seq2[fs.FileInfo, error] {
	ctx := context.Background()
	return t.FS.ListDir(ctx, t.Path)
}
