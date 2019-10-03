package ninep

import (
	"context"
	"crypto/tls"
	"errors"
	"io"
	"net"
	"os"
	"strings"
	"sync"
	"time"
)

// A 9P client that supports low-level operations and higher-level functionality
type Client struct {
	rwc  net.Conn
	txns chan *cltTransaction

	Authorizee Authorizee

	Timeout                 time.Duration
	MaxMsgSize              uint32
	MaxSimultaneousRequests uint

	Loggable
}

// Returns an interface that conforms to the file system interface
// Can be used once Connect*() are called and successful
func (c *Client) Fs(user, mount string) (FileSystem, error) {
	afid := NO_FID
	f := Fid(1)
	if c.Authorizee != nil {
		afid = Fid(0)
		_, err := c.Auth(afid, user, mount)
		if err == nil {
			err = c.Authorizee.Prove(context.Background(), user, mount)
			if err != nil {
				c.Errorf("Failed to authorize, because of bad credentials: %s", err)
				return nil, err
			}
		} else {
			c.Errorf("Failed to authorize, continuing: %s", err)
			err = nil
		}
	}
	root, err := c.Attach(f, afid, user, mount)
	if err != nil {
		return nil, err
	}
	return &FileSystemProxy{c: c, rootF: f, rootQ: root}, nil
}

func (c *Client) ConnectTLS(addr string, tlsCfg *tls.Config) error {
	var err error
	c.rwc, err = tls.Dial("tcp", addr, tlsCfg)
	if err != nil {
		return err
	}
	if err = c.connect(); err != nil {
		return err
	}
	return nil
}

func (c *Client) Connect(addr string) error {
	var err error
	c.rwc, err = net.Dial("tcp", addr)
	if err != nil {
		return err
	}
	if err = c.connect(); err != nil {
		return err
	}
	return nil
}

func (c *Client) Close() error {
	err := c.rwc.Close()
	c.rwc = nil
	return err
}

func (c *Client) connect() error {
	{ // set default values
		if c.MaxMsgSize < MIN_MESSAGE_SIZE {
			c.MaxMsgSize = DEFAULT_MAX_MESSAGE_SIZE
		}

		if c.MaxSimultaneousRequests == 0 {
			c.MaxSimultaneousRequests = 1
		}
	}

	{ // version exchange
		verTxn := createClientTransaction(NO_TAG, c.MaxMsgSize)
		if err := c.acceptRversion(&verTxn, c.MaxMsgSize); err != nil {
			return err
		}
	}

	{ // initialization
		c.txns = make(chan *cltTransaction, c.MaxSimultaneousRequests)
		go func() {
			for i := uint(0); i < c.MaxSimultaneousRequests; i++ {
				t := createClientTransaction(Tag(i), c.MaxMsgSize)
				c.txns <- &t
			}
		}()
	}

	// we're ready to talk
	return nil
}

func (c *Client) writeRequest(t *cltTransaction) error {
	now := time.Now()
	c.rwc.SetReadDeadline(now.Add(c.Timeout))
	c.rwc.SetWriteDeadline(now.Add(c.Timeout))
	return t.writeRequest(c.rwc)
}

func (c *Client) acceptRversion(txn *cltTransaction, maxMsgSize uint32) error {
	c.Tracef("Tversion(%d, %s)", maxMsgSize, VERSION_9P2000)
	txn.Tversion(maxMsgSize, VERSION_9P2000)
	if err := c.writeRequest(txn); err != nil {
		c.Errorf("failed to write version: %s", err)
		return err
	}

	if err := txn.readReply(c.rwc); err != nil {
		c.Errorf("failed to read version: %s", err)
		return err
	}

	request, ok := txn.Reply().(Rversion)
	if !ok {
		c.Errorf("failed to negotiate version: unexpected message type: %d", txn.requestType())
		return ErrBadFormat
	}

	if !strings.HasPrefix(request.Version(), VERSION_9P) {
		c.Tracef("unsupported server version: %s", request.Version())
		return ErrBadFormat
	}

	size := request.MsgSize()
	if size > maxMsgSize {
		c.Errorf("server returned size higher than client gave: (server: %d > client: %d)", size, maxMsgSize)
		return ErrBadFormat
	}
	c.MaxMsgSize = request.MsgSize()

	return nil
}

func (c *Client) release(t *cltTransaction) {
	t.reset()
	c.txns <- t
}

func (c *Client) Auth(afid Fid, user, mnt string) (Qid, error) {
	txn := <-c.txns
	defer c.release(txn)

	txn.Tauth(afid, user, mnt)
	if err := c.writeRequest(txn); err != nil {
		c.Errorf("Tauth: Failed to write request: %s", err)
		return nil, err
	}

	if err := txn.readReply(c.rwc); err != nil {
		c.Errorf("failed to read version: %s", err)
		return nil, err
	}

	switch r := txn.Reply().(type) {
	case Rauth:
		c.Tracef("Rauth")
		return r.Aqid(), nil
	case Rerror:
		err := errors.New(r.Ename())
		c.Errorf("Expected Rauth from server, got error: %s", err)
		return nil, err
	default:
		c.Errorf("Expected Rauth from server")
		return nil, ErrBadFormat
	}
}

func (c *Client) Attach(fd, afid Fid, user, mnt string) (Qid, error) {
	txn := <-c.txns
	defer c.release(txn)

	c.Tracef("Tattach(%d, %d, %#v, %#v)", fd, afid, user, mnt)
	txn.Tattach(fd, afid, user, mnt)
	if err := c.writeRequest(txn); err != nil {
		c.Errorf("Tattach: Failed to write request: %s", err)
		return Qid{}, err
	}

	if err := txn.readReply(c.rwc); err != nil {
		c.Errorf("failed to read version: %s", err)
		return nil, err
	}

	switch r := txn.Reply().(type) {
	case Rattach:
		c.Tracef("Rattach: %s", r.Qid())
		return r.Qid(), nil
	case Rerror:
		err := errors.New(r.Ename())
		c.Errorf("Expected Rattach from server, got error: %s", err)
		return nil, err
	default:
		c.Errorf("Expected Rattach from server: %#v")
		return nil, ErrBadFormat
	}
}

func (c *Client) Walk(f, newF Fid, path []string) ([]Qid, error) {
	txn := <-c.txns
	defer c.release(txn)

	txn.Twalk(f, newF, path)
	if err := c.writeRequest(txn); err != nil {
		c.Errorf("Twalk: Failed to write request: %s", err)
		return nil, err
	}

	if err := txn.readReply(c.rwc); err != nil {
		c.Errorf("failed to read version: %s", err)
		return nil, err
	}

	switch r := txn.Reply().(type) {
	case Rwalk:
		c.Tracef("Rwalk")
		size := int(r.NumWqid())
		var qids []Qid
		if size > 0 {
			qids = make([]Qid, size)
			for i := 0; i < size; i++ {
				q := NewQid()
				copy(q, r.Wqid(i))
				qids[i] = q
			}
		}
		return qids, nil
	case Rerror:
		err := errors.New(r.Ename())
		c.Errorf("Expected Rattach from server, got error: %s", err)
		return nil, err
	default:
		c.Errorf("Expected Rattach from server")
		return nil, ErrBadFormat
	}
}

func (c *Client) Stat(f Fid) (Stat, error) {
	txn := <-c.txns
	defer c.release(txn)

	txn.Tstat(f)
	if err := c.writeRequest(txn); err != nil {
		c.Errorf("Tstat: Failed to write request: %s", err)
		return nil, err
	}

	if err := txn.readReply(c.rwc); err != nil {
		c.Errorf("failed to read version: %s", err)
		return nil, err
	}

	switch r := txn.Reply().(type) {
	case Rstat:
		c.Tracef("Rstat")
		st := r.Stat().Clone()
		return st, nil
	case Rerror:
		err := errors.New(r.Ename())
		c.Errorf("Expected Rstat from server, got error: %s", err)
		return nil, err
	default:
		c.Errorf("Expected Rstat from server")
		return nil, ErrBadFormat
	}
}

func (c *Client) WriteStat(f Fid, s Stat) error {
	txn := <-c.txns
	defer c.release(txn)

	txn.Twstat(f, s)
	if err := c.writeRequest(txn); err != nil {
		c.Errorf("Twstat: Failed to write request: %s", err)
		return err
	}

	if err := txn.readReply(c.rwc); err != nil {
		c.Errorf("failed to read version: %s", err)
		return err
	}

	switch r := txn.Reply().(type) {
	case Rwstat:
		c.Tracef("Rwstat")
		return nil
	case Rerror:
		err := errors.New(r.Ename())
		c.Errorf("Expected Rwstat from server, got error: %s", err)
		return err
	default:
		c.Errorf("Expected Rwstat from server")
		return ErrBadFormat
	}
}

func (c *Client) Read(f Fid, p []byte, offset uint64) (int, error) {
	txn := <-c.txns
	defer c.release(txn)

	c.Tracef("Read(%s, []byte(%d), %v)", f, len(p), offset)
	txn.Tread(f, offset, uint32(len(p)))
	if err := c.writeRequest(txn); err != nil {
		c.Errorf("Tread: Failed to write request: %s", err)
		return 0, err
	}

	if err := txn.readReply(c.rwc); err != nil {
		c.Errorf("failed to read version: %s", err)
		return 0, err
	}

	switch r := txn.Reply().(type) {
	case Rread:
		dat := r.Data()
		c.Tracef("Rread -> %d", len(dat))
		copy(p, dat)
		return len(dat), nil
	case Rerror:
		err := errors.New(r.Ename())
		c.Errorf("Expected Rread from server, got error: %s", err)
		return 0, err
	default:
		c.Errorf("Expected Rread from server")
		return 0, ErrBadFormat
	}
}

func (c *Client) Clunk(f Fid) error {
	txn := <-c.txns
	defer c.release(txn)

	txn.Tclunk(f)
	if err := c.writeRequest(txn); err != nil {
		c.Errorf("Tclunk: Failed to write request: %s", err)
		return err
	}

	if err := txn.readReply(c.rwc); err != nil {
		c.Errorf("failed to read version: %s", err)
		return err
	}

	switch r := txn.Reply().(type) {
	case Rclunk:
		c.Tracef("Rclunk")
		return nil
	case Rerror:
		err := errors.New(r.Ename())
		c.Errorf("Expected Rclunk from server, got error: %s", err)
		return nil
	default:
		c.Errorf("Expected Rclunk from server")
		return ErrBadFormat
	}
}

func (c *Client) Remove(f Fid) error {
	txn := <-c.txns
	defer c.release(txn)

	txn.Tremove(f)
	if err := c.writeRequest(txn); err != nil {
		c.Errorf("Tremove: Failed to write request: %s", err)
		return err
	}

	if err := txn.readReply(c.rwc); err != nil {
		c.Errorf("failed to read version: %s", err)
		return err
	}

	switch r := txn.Reply().(type) {
	case Rremove:
		c.Tracef("Rremove")
		return nil
	case Rerror:
		err := errors.New(r.Ename())
		c.Errorf("Expected Rremove from server, got error: %s", err)
		return err
	default:
		c.Errorf("Expected Rremove from server")
		return ErrBadFormat
	}
}

func (c *Client) Write(f Fid, data []byte, offset uint64) (uint32, error) {
	txn := <-c.txns
	defer c.release(txn)

	size := len(data)
	buf := txn.TwriteBuffer()
	if size > len(buf) {
		size = len(buf)
	}
	copy(buf, data[:size])

	txn.Twrite(f, offset, uint32(size))
	if err := c.writeRequest(txn); err != nil {
		c.Errorf("Twrite: Failed to write request: %s", err)
		return 0, err
	}

	if err := txn.readReply(c.rwc); err != nil {
		c.Errorf("failed to read version: %s", err)
		return 0, err
	}

	switch r := txn.Reply().(type) {
	case Rwrite:
		c.Tracef("Rwrite")
		n := r.Count()
		return n, nil
	case Rerror:
		err := errors.New(r.Ename())
		c.Errorf("Expected Rwrite from server, got error: %s", err)
		return 0, err
	default:
		c.Errorf("Expected Rwrite from server")
		return 0, ErrBadFormat
	}
}

func (c *Client) Open(f Fid, m OpenMode) (q Qid, iounit uint32, err error) {
	txn := <-c.txns
	defer c.release(txn)

	txn.Topen(f, m)
	if err = c.writeRequest(txn); err != nil {
		c.Errorf("Topen: Failed to write request: %s", err)
		return
	}

	if err = txn.readReply(c.rwc); err != nil {
		c.Errorf("failed to read version: %s", err)
		return
	}

	switch r := txn.Reply().(type) {
	case Ropen:
		c.Tracef("Ropen")
		q = r.Qid().Clone()
		iounit = r.Iounit()
		return
	case Rerror:
		err = errors.New(r.Ename())
		c.Errorf("Expected Ropen from server, got error: %s", err)
		return
	default:
		c.Errorf("Expected Ropen from server")
		return
	}
}

func (c *Client) Create(f Fid, name string, perm Mode, mode OpenMode) (q Qid, iounit uint32, err error) {
	txn := <-c.txns
	defer c.release(txn)

	txn.Tcreate(f, name, uint32(perm), mode)
	if err = c.writeRequest(txn); err != nil {
		c.Errorf("Tcreate: Failed to write request: %s", err)
		return
	}

	if err = txn.readReply(c.rwc); err != nil {
		c.Errorf("failed to read version: %s", err)
		return
	}

	switch r := txn.Reply().(type) {
	case Rcreate:
		c.Tracef("Rcreate")
		q = r.Qid().Clone()
		iounit = r.Iounit()
		return
	case Rerror:
		err = errors.New(r.Ename())
		c.Errorf("Expected Rcreate from server, got error: %s", err)
		return
	default:
		c.Errorf("Expected Rcreate from server")
		return
	}
}

/////////////////////////////////////////////////////////

type FileProxy struct {
	fs  *FileSystemProxy
	fid Fid
}

func (f *FileProxy) ReadAt(p []byte, offset int64) (int, error) {
	size, err := f.fs.c.Read(f.fid, p, uint64(offset))
	return int(size), err
}
func (f *FileProxy) WriteAt(p []byte, offset int64) (int, error) {
	size, err := f.fs.c.Write(f.fid, p, uint64(offset))
	return int(size), err
}
func (f *FileProxy) Sync() error { return f.fs.c.WriteStat(f.fid, SyncStat()) }
func (f *FileProxy) Close() error {
	err := f.fs.c.Clunk(f.fid)
	f.fs.releaseFid(f.fid)
	return err
}

///////////////////////

type FileSystemProxy struct {
	c     *Client
	rootF Fid
	rootQ Qid

	mut      sync.Mutex
	usedFids map[Fid]bool
}

func (fs *FileSystemProxy) splitPath(path string) []string {
	if path == "" {
		return nil
	}
	return strings.Split(path, "/")
}
func (fs *FileSystemProxy) allocFid() Fid {
	f := Fid(0)
	fs.mut.Lock()
	if fs.usedFids == nil {
		fs.usedFids = make(map[Fid]bool)
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

func (fs *FileSystemProxy) walk(fid Fid, path string) error {
	_, err := fs.c.Walk(fs.rootF, fid, fs.splitPath(path))
	if err != nil {
		// Best attempt to notify server that we're dropping this fid
		fs.c.Clunk(fid)
		return err
	}
	return nil
}

func (fs *FileSystemProxy) statsFromReader(maxMsgSize uint32, r io.ReaderAt) ([]Stat, error) {
	res := []Stat{}
	offset := 0
	buf := make([]byte, fs.c.MaxMsgSize)

	readStat := func(b []byte) (Stat, []byte, error) {
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
		n, err := r.ReadAt(buf, int64(offset))
		if n > 0 {
			rst := buf[:n]
			for len(rst) > 0 {
				var st Stat
				st, rst, err = readStat(rst)
				if err != nil {
					return nil, err
				}
				res = append(res, st.Clone())
			}
			offset += n
		}

		if n == 0 {
			break
		} else if err == io.EOF {
			err = nil
			break
		} else if err != nil {
			return res, err
		}
	}

	return res, nil
}

//////////

func (fs *FileSystemProxy) MakeDir(path string, mode Mode) error {
	// TODO: make directory recursively?
	fid := fs.allocFid()

	prefix := ""
	filename := path
	i := strings.LastIndex(path, "/")
	if i != -1 {
		prefix = path[:i]
		filename = path[i+1:]
	}
	if err := fs.walk(fid, prefix); err != nil {
		return err
	}
	_, _, err := fs.c.Create(fid, filename, mode|M_DIR, ORDWR)
	fs.c.Clunk(fid)
	fs.releaseFid(fid)
	return err
}
func (fs *FileSystemProxy) CreateFile(path string, flag OpenMode, mode Mode) (FileHandle, error) {
	fid := fs.allocFid()

	prefix := ""
	filename := path
	i := strings.LastIndex(path, "/")
	if i != -1 {
		prefix = path[:i]
		filename = path[i+1:]
	}
	if err := fs.walk(fid, prefix); err != nil {
		return nil, err
	}
	var h FileHandle
	_, _, err := fs.c.Create(fid, filename, mode, flag)
	if err == nil {
		h = &FileProxy{fs, fid}
	} else {
		fs.c.Clunk(fid)
		fs.releaseFid(fid)
	}
	return h, err
}
func (fs *FileSystemProxy) OpenFile(path string, flag OpenMode) (FileHandle, error) {
	fid := fs.allocFid()
	if err := fs.walk(fid, path); err != nil {
		return nil, err
	}
	var h FileHandle
	_, _, err := fs.c.Open(fid, flag)
	if err == nil {
		h = &FileProxy{fs, fid}
	} else {
		fs.c.Clunk(fid)
		fs.releaseFid(fid)
	}
	return h, err
}
func (fs *FileSystemProxy) ListDir(path string) ([]os.FileInfo, error) {
	fid := fs.allocFid()
	defer fs.releaseFid(fid)
	fs.c.Tracef("ListDir(%#v) %s", path, fid)
	if err := fs.walk(fid, path); err != nil {
		return nil, err
	}
	defer fs.c.Clunk(fid)
	var infos []os.FileInfo
	_, _, err := fs.c.Open(fid, OREAD)
	if err == nil {
		stats, err := fs.statsFromReader(fs.c.MaxMsgSize, &FileProxy{fs, fid})
		if err != nil {
			return nil, err
		}
		infos = FileInfosFromStats(stats)
	}
	return infos, err
}
func (fs *FileSystemProxy) Stat(path string) (os.FileInfo, error) {
	fid := fs.allocFid()
	defer fs.releaseFid(fid)
	if err := fs.walk(fid, path); err != nil {
		return nil, err
	}
	st, err := fs.c.Stat(fid)
	fs.c.Clunk(fid)
	return st.FileInfo(), err
}
func (fs *FileSystemProxy) WriteStat(path string, s Stat) error {
	fid := fs.allocFid()
	defer fs.releaseFid(fid)
	if err := fs.walk(fid, path); err != nil {
		return err
	}
	err := fs.c.WriteStat(fid, s)
	fs.c.Clunk(fid)
	return err
}
func (fs *FileSystemProxy) Delete(path string) error {
	fid := fs.allocFid()
	defer fs.releaseFid(fid)
	if err := fs.walk(fid, path); err != nil {
		return err
	}
	// regardless of this call, the server should drop the fid
	return fs.c.Remove(fid)
}
