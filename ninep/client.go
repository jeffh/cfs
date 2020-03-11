package ninep

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
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

type Client interface {
	io.Closer
	// Core Client Protocol
	Clunk(f Fid) error
	Create(f Fid, name string, perm Mode, mode OpenMode) (q Qid, iounit uint32, err error)
	Open(f Fid, flag OpenMode) (q Qid, iounit uint32, err error)
	Read(f Fid, p []byte, offset uint64) (int, error)
	Remove(f Fid) error
	Stat(f Fid) (Stat, error)
	Walk(f, newF Fid, path []string) ([]Qid, error)
	Write(f Fid, p []byte, offset uint64) (int, error)
	WriteStat(f Fid, s Stat) error

	Fs() (*FileSystemProxy, error)

	// Absent:
	// Auth() - assumed the constructor will manage this
	// Attach() - assumed the constructor will manage this
	// Fs() - implementation specific construction right now. Although may be better to unify
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

func (f *FileProxy) Type() QidType     { return f.qid.Type() }
func (f *FileProxy) IsDir() bool       { return f.qid.Type().IsDir() }
func (f *FileProxy) IsSymLink() bool   { return f.qid.Type()&QT_SYMLINK != 0 }
func (f *FileProxy) IsAuth() bool      { return f.qid.Type()&QT_AUTH != 0 }
func (f *FileProxy) IsMount() bool     { return f.qid.Type()&QT_MOUNT != 0 }
func (f *FileProxy) IsExclusive() bool { return f.qid.Type()&QT_EXCL != 0 }
func (f *FileProxy) IsTemporary() bool { return f.qid.Type()&QT_TMP != 0 }

// Returns a new FileProxy of the new path relative to this file. It is the
// caller responsibility to Close() the returned file proxy.
func (f *FileProxy) Traverse(path string) (*FileProxy, error) {
	fid := f.fs.allocFid()
	qid, err := f.fs.walk(f.fid, path)
	if err != nil {
		return nil, err
	}
	return &FileProxy{f.fs, fid, qid, nil}, nil
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

// Alias to Create() with M_DIR always set
func (f *FileProxy) CreateDir(name string, mode Mode) (*FileProxy, error) {
	return f.Create(name, ORDWR, mode|M_DIR)
}

func (f *FileProxy) Create(name string, flag OpenMode, mode Mode) (*FileProxy, error) {
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

func (f *FileProxy) ReadAt(p []byte, offset int64) (int, error) {
	size, err := f.fs.c.Read(f.fid, p, uint64(offset))
	if size == 0 && err == nil {
		err = io.EOF
	}
	return int(size), err
}

func (f *FileProxy) WriteAt(p []byte, offset int64) (int, error) {
	size, err := f.fs.c.Write(f.fid, p, uint64(offset))
	return int(size), err
}

// Alias to f.WriteStat(SyncStat())
func (f *FileProxy) Sync() error { return f.fs.c.WriteStat(f.fid, SyncStat()) }

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

// Provides an iterator to list files in dir
func (f *FileProxy) ListDirStat() (StatIterator, error) {
	itr := &fileSystemProxyIterator{fp: f}

	qid, _, err := f.fs.c.Open(f.fid, OREAD)
	if err == nil {
		if !qid.Type().IsDir() {
			itr.Close()
			return nil, ErrListingOnNonDir
		} else {
			return itr, nil
		}
	} else {
		itr.Close()
		return nil, err
	}
}

func (f *FileProxy) ListDir() (FileInfoIterator, error) {
	return f.ListDirStat()
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
	if len(qids) != len(parts) {
		return nil, io.ErrUnexpectedEOF
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

type fileSystemProxyIterator struct {
	fp     *FileProxy
	rst    []byte
	buf    []byte
	offset int
}

var _ FileInfoIterator = (*fileSystemProxyIterator)(nil)
var _ StatIterator = (*fileSystemProxyIterator)(nil)

func (it *fileSystemProxyIterator) Reset() error {
	it.rst = nil
	it.buf = make([]byte, it.fp.fs.c.MaxMessageSize())
	it.offset = 0
	return nil
}

// Note: Stat must be copied if you wish to keep it beyond the next NextStat() call
func (it *fileSystemProxyIterator) NextStat() (Stat, error) {
	if it.buf == nil {
		if err := it.Reset(); err != nil {
			return nil, err
		}
	}
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

	var outSt Stat
	var err error

	if len(it.rst) > 0 {
		var st Stat
		st, it.rst, err = readStat(it.fp.fs, it.rst)
		if err != nil {
			return nil, err
		}
		outSt = st
	}

	if outSt == nil {
		var n int
		n, err = it.fp.ReadAt(it.buf, int64(it.offset))
		if n > 0 {
			it.rst = it.buf[:n]
			var st Stat
			st, it.rst, err = readStat(it.fp.fs, it.rst)
			if err != nil {
				return nil, err
			}
			outSt = st
			it.offset += n
		}
	}

	return outSt, err
}

// Note: FileInfo is safe to use after calling NextFileInfo() again
func (it *fileSystemProxyIterator) NextFileInfo() (os.FileInfo, error) {
	st, err := it.NextStat()
	if st != nil {
		return st.Clone().FileInfo(), err
	}
	return nil, err
}
func (it *fileSystemProxyIterator) Close() error { return it.fp.Close() }

func (fs *FileSystemProxy) ListDir(ctx context.Context, path string) (FileInfoIterator, error) {
	return fs.ListDirStat(path)
}

func (fs *FileSystemProxy) ListDirStat(path string) (StatIterator, error) {
	fid := fs.allocFid()
	fs.c.Tracef("ListDir(%#v) %s", path, fid)
	q, err := fs.walk(fid, path)
	if err != nil {
		fs.releaseFid(fid)
		return nil, err
	}

	itr := &fileSystemProxyIterator{fp: &FileProxy{fs, fid, q, nil}}

	qid, _, err := fs.c.Open(fid, OREAD)
	if err == nil {
		if !qid.Type().IsDir() {
			itr.Close()
			return nil, ErrListingOnNonDir
		} else {
			return itr, nil
		}
	} else {
		itr.Close()
		return nil, err
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
func (fs *FileSystemProxy) Traverse(path string) (*FileProxy, error) {
	fid := fs.allocFid()
	qid, err := fs.walk(fid, path)
	if err != nil {
		return nil, err
	}
	return &FileProxy{fs, fid, qid, nil}, nil
}
