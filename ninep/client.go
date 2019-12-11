package ninep

import (
	"context"
	"crypto/tls"
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

type cltChResponse struct {
	res *cltResponse
	err error
}

// TODO: before we can make this public, we need to audit what fields/methods should be public on Client
type clientSocketStrategy interface {
	WriteRequest(c *BasicClient, t *cltRequest) error
	ReadLoop(ctx context.Context, c *BasicClient)
}

type Client interface {
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

	// Absent:
	// Auth() - assumed the constructor will manage this
	// Attach() - assumed the constructor will manage this
	// Fs() - implementation specific construction right now. Although may be better to unify
}

// A 9P client that supports low-level operations and some higher-level functionality.
type BasicClient struct {
	m                sync.Mutex
	rwc              net.Conn
	requestPool      chan *cltRequest
	responsePool     chan *cltResponse
	pendingResponses chan *cltResponse
	readCancel       context.CancelFunc

	mut       sync.Mutex
	tagToTxns map[Tag]cltTransaction

	Authorizee Authorizee

	Timeout                 time.Duration
	MaxMsgSize              uint32
	MaxSimultaneousRequests uint
	Dialer                  Dialer
	MinMsgSize              uint32

	Loggable

	strategy clientSocketStrategy
	writeReq func(t *cltRequest)
}

var _ FileSystemProxyClient = (*BasicClient)(nil)

func (c *BasicClient) MaxMessageSize() uint32 { return c.MaxMsgSize }
func (c *BasicClient) numInflightReqs() int {
	c.mut.Lock()
	size := len(c.tagToTxns)
	c.mut.Unlock()
	return size
}

func (c *BasicClient) dial(network, addr string) (net.Conn, error) {
	if c.Dialer == nil {
		return net.Dial(network, addr)
	}
	return c.Dialer.Dial(network, addr)
}

func (c *BasicClient) abortTransactions(err error) {
	c.mut.Lock()
	for _, txn := range c.tagToTxns {
		select {
		case txn.ch <- cltChResponse{err: err}:
		case <-time.After(1 * time.Second):
		}
		close(txn.ch)
	}
	c.tagToTxns = make(map[Tag]cltTransaction)
	c.mut.Unlock()
	return
}

func (c *BasicClient) getTransaction(t Tag) (cltTransaction, bool) {
	c.mut.Lock()
	txn, ok := c.tagToTxns[t]
	c.mut.Unlock()
	return txn, ok
}

func (c *BasicClient) putTransaction(t Tag, txn cltTransaction) {
	c.mut.Lock()
	c.tagToTxns[t] = txn
	c.mut.Unlock()
}

func (c *BasicClient) sendRequest(txn *cltTransaction) <-chan cltChResponse {
	txn.ch = make(chan cltChResponse, 1)
	c.putTransaction(txn.req.tag, *txn)
	if err := c.writeRequest(txn.req); err != nil {
		txn.ch <- cltChResponse{err: err}
		close(txn.ch)
	}
	// reader loop will send on the channel
	return txn.ch
}

// Returns an interface that conforms to the file system interface
// Can be used once Connect*() are called and successful
func (c *BasicClient) Fs(user, mount string) (*FileSystemProxy, error) {
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

func (c *BasicClient) ConnectTLS(addr string, tlsCfg *tls.Config) error {
	var err error
	c.rwc, err = tls.Dial("tcp", addr, tlsCfg)
	if err != nil {
		return err
	}
	if err = c.connect(); err != nil {
		c.rwc.Close()
		return err
	}
	return nil
}

func (c *BasicClient) Connect(addr string) error {
	var err error
	c.rwc, err = c.dial("tcp", addr)
	if err != nil {
		return err
	}
	if err = c.connect(); err != nil {
		c.rwc.Close()
		return err
	}
	return nil
}

func (c *BasicClient) Close() error {
	c.m.Lock()
	defer c.m.Unlock()
	if c.readCancel != nil {
		go c.readCancel()
		c.readCancel = nil
		if c.requestPool != nil {
			close(c.requestPool)
			c.requestPool = nil
		}
		if c.responsePool != nil {
			close(c.responsePool)
			c.responsePool = nil
		}
		if c.pendingResponses != nil {
			close(c.pendingResponses)
			c.pendingResponses = nil
		}
	}
	err := c.rwc.Close()
	return err
}

func (c *BasicClient) connect() error {
	{
		// cleanup / initialization
		if c.readCancel != nil {
			c.readCancel()
			c.readCancel = nil
		}

		c.mut.Lock()
		c.tagToTxns = make(map[Tag]cltTransaction)
		c.mut.Unlock()
	}

	{ // set default values
		if c.MaxMsgSize < MIN_MESSAGE_SIZE {
			c.MaxMsgSize = DEFAULT_MAX_MESSAGE_SIZE
		}

		if c.MaxSimultaneousRequests == 0 {
			c.MaxSimultaneousRequests = 1
		}

		c.m.Lock()
		c.requestPool = make(chan *cltRequest, c.MaxSimultaneousRequests)
		c.responsePool = make(chan *cltResponse, c.MaxSimultaneousRequests)
		c.pendingResponses = make(chan *cltResponse, c.MaxSimultaneousRequests)
		c.m.Unlock()
	}

	{ // version exchange
		req := createClientRequest(NO_TAG, c.MaxMsgSize)
		res := createClientResponse(c.MaxMsgSize)
		verTxn := cltTransaction{
			req: &req,
			res: &res,
		}
		if err := c.acceptRversion(&verTxn, c.MaxMsgSize); err != nil {
			return err
		}
	}

	{ // initialization
		go func() {
			for i := uint(0); i < c.MaxSimultaneousRequests; i++ {
				t := createClientRequest(Tag(i), c.MaxMsgSize)
				c.requestPool <- &t
			}
			for i := uint(0); i < c.MaxSimultaneousRequests; i++ {
				t := createClientResponse(c.MaxMsgSize)
				c.responsePool <- &t
			}
		}()
	}

	// start bg reader
	{
		ctx, cancel := context.WithCancel(context.Background())
		c.readCancel = cancel
		strat := c.getStrategy()
		go strat.ReadLoop(ctx, c)
	}

	// we're ready to talk
	return nil
}

func (c *BasicClient) writeRequest(t *cltRequest) error {
	strat := c.getStrategy()
	return strat.WriteRequest(c, t)
}

func (c *BasicClient) getStrategy() clientSocketStrategy {
	if c.strategy == nil {
		c.strategy = &defaultClientSocketStrategy{}
	}
	return c.strategy
}

func (c *BasicClient) acceptRversion(txn *cltTransaction, maxMsgSize uint32) error {
	c.Tracef("Tversion(%d, %s)", maxMsgSize, VERSION_9P2000)
	txn.req.Tversion(maxMsgSize, VERSION_9P2000)
	if err := txn.req.writeRequest(c.rwc); err != nil {
		c.Errorf("failed to write version: %s", err)
		return err
	}

	if err := txn.res.readReply(c.rwc); err != nil {
		c.Errorf("failed to read version: %s", err)
		return err
	}

	request, ok := txn.res.Reply().(Rversion)
	if !ok {
		c.Errorf("failed to negotiate version: unexpected message type: %d", txn.req.requestType())
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
	if c.MinMsgSize > c.MaxMsgSize {
		c.Errorf("server returned size lower than client supports: (server: %d < client: [gave: %d; min: %d])", size, maxMsgSize, c.MinMsgSize)
		return ErrBadFormat
	}

	return nil
}

func (c *BasicClient) allocTxn() (cltTransaction, bool) {
	req, ok := <-c.requestPool
	var txn cltTransaction
	if ok {
		req.reset()
		txn = cltTransaction{
			req: req,
		}
		c.mut.Lock()
		c.tagToTxns[req.tag] = txn
		c.mut.Unlock()
	}
	return txn, ok
}

func (c *BasicClient) resetTxn(t Tag) cltTransaction {
	c.mut.Lock()
	oldTxn := c.tagToTxns[t]
	if oldTxn.res != nil {
		oldTxn.res.reset()
		c.responsePool <- oldTxn.res
	}

	req := oldTxn.req
	req.reset()
	txn := cltTransaction{
		req: req,
	}
	c.tagToTxns[t] = txn
	c.mut.Unlock()
	return txn
}

func (c *BasicClient) release(t Tag) {
	c.mut.Lock()
	txn, ok := c.tagToTxns[t]
	delete(c.tagToTxns, t)
	c.mut.Unlock()

	if ok {
		txn.req.reset()
		c.requestPool <- txn.req
		if txn.res != nil {
			txn.res.reset()
			c.responsePool <- txn.res
		}
	}
}

// Implements the clientSocketStrategy interface. Provides naive, sane behaviors:
//
// - Single write per request
// - Single read per request
type defaultClientSocketStrategy struct{}

var _ clientSocketStrategy = (*defaultClientSocketStrategy)(nil)

func (s *defaultClientSocketStrategy) WriteRequest(c *BasicClient, t *cltRequest) error {
	err := t.writeRequest(c.rwc)
	if err == nil {
		c.pendingResponses <- <-c.responsePool
	}
	return err
}

func (s *defaultClientSocketStrategy) ReadLoop(ctx context.Context, c *BasicClient) {
	for {
		select {
		case <-ctx.Done():
			c.abortTransactions(ctx.Err())
			return
		case res := <-c.pendingResponses:
			res.reset()
			err := res.readReply(c.rwc)
			if err != nil {
				c.Errorf("Error reading from server: %s", err)
				c.abortTransactions(err)
				return
			}

			txn, ok := c.getTransaction(res.reqTag())
			if !ok {
				c.Errorf("Server returned unrecognized tag: %d", res.reqTag())
				continue
			}
			c.Tracef("Server tag: %d", res.Reply().Tag())
			txn.res = res
			c.putTransaction(res.reqTag(), txn)
			txn.ch <- cltChResponse{res: res}
			close(txn.ch)
		}
	}
}

var mappedErrors []error = []error{
	os.ErrInvalid,
	os.ErrPermission,
	os.ErrExist,
	os.ErrNotExist,
	os.ErrClosed,
	os.ErrNoDeadline,
	io.EOF,
	io.ErrClosedPipe,
	io.ErrNoProgress,
	io.ErrShortBuffer,
	io.ErrShortWrite,
	io.ErrUnexpectedEOF,

	ErrBadFormat,
	ErrWriteNotAllowed,
	ErrReadNotAllowed,
	ErrSeekNotAllowed,
	ErrUnsupported,
	ErrNotImplemented,
	ErrInvalidAccess,
	ErrChangeUidNotAllowed,
	ErrMissingIterator,
}

func (c *BasicClient) asError(r Rerror) error {
	msg := r.Ename()
	// we want to preserve equality of errors to native os-styled errors
	for _, e := range mappedErrors {
		if msg == e.Error() {
			return e
		}
	}
	// else
	err := errors.New(msg)
	return err
}

func (c *BasicClient) Auth(afid Fid, user, mnt string) (Qid, error) {
	txn, ok := c.allocTxn()
	if !ok {
		return nil, io.ErrUnexpectedEOF
	}
	defer c.release(txn.req.tag)

	txn.req.Tauth(afid, user, mnt)
	res := <-c.sendRequest(&txn)
	if res.err != nil {
		c.Errorf("Tauth: Failed requesting: %s", res.err)
		return nil, res.err
	}

	switch r := res.res.Reply().(type) {
	case Rauth:
		c.Tracef("Rauth")
		return r.Aqid(), nil
	case Rerror:
		err := c.asError(r)
		c.Errorf("Expected Rauth from server, got error: %s", err)
		return nil, err
	default:
		c.Errorf("Expected Rauth from server")
		return nil, ErrBadFormat
	}
}

func (c *BasicClient) Attach(fd, afid Fid, user, mnt string) (Qid, error) {
	txn, ok := c.allocTxn()
	if !ok {
		return nil, io.ErrUnexpectedEOF
	}
	defer c.release(txn.req.tag)

	c.Tracef("Tattach(%d, %d, %#v, %#v)", fd, afid, user, mnt)
	txn.req.Tattach(fd, afid, user, mnt)
	res := <-c.sendRequest(&txn)
	if res.err != nil {
		c.Errorf("Tattach: Failed to write request: %s", res.err)
		return Qid{}, res.err
	}

	switch r := res.res.Reply().(type) {
	case Rattach:
		c.Tracef("Rattach: %s", r.Qid())
		return r.Qid(), nil
	case Rerror:
		err := c.asError(r)
		c.Errorf("Expected Rattach from server, got error: %s", err)
		return nil, err
	default:
		c.Errorf("Expected Rattach from server: %#v")
		return nil, ErrBadFormat
	}
}

func (c *BasicClient) Walk(f, newF Fid, path []string) ([]Qid, error) {
	txn, ok := c.allocTxn()
	if !ok {
		return nil, io.ErrUnexpectedEOF
	}
	defer c.release(txn.req.tag)

	txn.req.Twalk(f, newF, path)
	c.Tracef("Twalk %s -> %s %#v %d", f, newF, path, txn.req.Request().(interface{ Size() uint32 }).Size())
	res := <-c.sendRequest(&txn)
	if err := res.err; err != nil {
		c.Errorf("Twalk: Failed to write request: %s", err)
		return nil, err
	}

	switch r := res.res.Reply().(type) {
	case Rwalk:
		c.Tracef("Rwalk %d", r.NumWqid())
		size := int(r.NumWqid())

		if size != len(path) {
			return nil, os.ErrNotExist
		}
		var qids []Qid
		if size > 0 {
			qids = make([]Qid, size)
			for i := 0; i < size; i++ {
				q := NewQid()
				copy(q, r.Wqid(i))
				qids[i] = q
			}
		}
		if len(qids) != len(path) {
			return qids, os.ErrNotExist
		}
		return qids, nil
	case Rerror:
		err := c.asError(r)
		c.Errorf("Expected Rattach from server, got error: %s", err)
		return nil, err
	default:
		c.Errorf("Expected Rattach from server")
		return nil, ErrBadFormat
	}
}

func (c *BasicClient) Stat(f Fid) (Stat, error) {
	txn, ok := c.allocTxn()
	if !ok {
		return nil, io.ErrUnexpectedEOF
	}
	defer c.release(txn.req.tag)

	txn.req.Tstat(f)
	res := <-c.sendRequest(&txn)
	if err := res.err; err != nil {
		c.Errorf("Tstat: Failed to write request: %s", err)
		return nil, err
	}
	c.Tracef("Tstat %s", f)

	switch r := res.res.Reply().(type) {
	case Rstat:
		c.Tracef("Rstat %s", r.Stat())
		st := r.Stat().Clone()
		return st, nil
	case Rerror:
		err := c.asError(r)
		c.Errorf("Expected Rstat from server, got error: %s", err)
		return nil, err
	default:
		c.Errorf("Expected Rstat from server")
		return nil, ErrBadFormat
	}
}

func (c *BasicClient) WriteStat(f Fid, s Stat) error {
	txn, ok := c.allocTxn()
	if !ok {
		return io.ErrUnexpectedEOF
	}
	defer c.release(txn.req.tag)

	txn.req.Twstat(f, s)
	res := <-c.sendRequest(&txn)
	if err := res.err; err != nil {
		c.Errorf("Twstat: Failed to write request: %s", err)
		return err
	}

	switch r := res.res.Reply().(type) {
	case Rwstat:
		c.Tracef("Rwstat")
		return nil
	case Rerror:
		err := c.asError(r)
		c.Errorf("Expected Rwstat from server, got error: %s", err)
		return err
	default:
		c.Errorf("Expected Rwstat from server")
		return ErrBadFormat
	}
}

func (c *BasicClient) Read(f Fid, p []byte, offset uint64) (int, error) {
	txn, ok := c.allocTxn()
	if !ok {
		return 0, io.ErrUnexpectedEOF
	}
	defer c.release(txn.req.tag)

	c.Tracef("Read(%s, []byte(%d), %v)", f, len(p), offset)
	txn.req.Tread(f, offset, uint32(len(p)))
	res := <-c.sendRequest(&txn)
	if err := res.err; err != nil {
		c.Errorf("Tread: Failed to write request: %s", err)
		return 0, err
	}

	switch r := res.res.Reply().(type) {
	case Rread:
		dat := r.Data()
		c.Tracef("Rread -> %d", len(dat))
		copy(p, dat)
		return len(dat), nil
	case Rerror:
		err := c.asError(r)
		fmt.Fprintf(os.Stderr, "READ_ERROR: %#v\n", err)
		c.Errorf("Expected Rread from server, got error: %s", err)
		return 0, err
	default:
		c.Errorf("Expected Rread from server")
		return 0, ErrBadFormat
	}
}

func (c *BasicClient) Clunk(f Fid) error {
	txn, ok := c.allocTxn()
	if !ok {
		return io.ErrUnexpectedEOF
	}
	defer c.release(txn.req.tag)

	txn.req.Tclunk(f)
	res := <-c.sendRequest(&txn)
	if err := res.err; err != nil {
		c.Errorf("Tclunk: Failed to write request: %s", err)
		return err
	}

	switch r := res.res.Reply().(type) {
	case Rclunk:
		c.Tracef("Rclunk %s", f)
		return nil
	case Rerror:
		err := c.asError(r)
		c.Errorf("Expected Rclunk from server, got error: %s", err)
		return nil
	default:
		c.Errorf("Expected Rclunk from server")
		return ErrBadFormat
	}
}

func (c *BasicClient) Remove(f Fid) error {
	txn, ok := c.allocTxn()
	if !ok {
		return io.ErrUnexpectedEOF
	}
	defer c.release(txn.req.tag)

	txn.req.Tremove(f)
	res := <-c.sendRequest(&txn)
	if err := res.err; err != nil {
		c.Errorf("Tremove: Failed to write request: %s", err)
		return err
	}

	switch r := res.res.Reply().(type) {
	case Rremove:
		c.Tracef("Rremove")
		return nil
	case Rerror:
		err := c.asError(r)
		c.Errorf("Expected Rremove from server, got error: %s", err)
		return err
	default:
		c.Errorf("Expected Rremove from server")
		return ErrBadFormat
	}
}

// Like WriteMsg, but conforms to golang's io.Writer interface (max num bytes possible, else error)
func (c *BasicClient) Write(f Fid, data []byte, offset uint64) (int, error) {
	txn, ok := c.allocTxn()
	if !ok {
		return 0, io.ErrUnexpectedEOF
	}
	defer c.release(txn.req.tag)

	size := len(data)
	wrote := 0

	for wrote < size {
		buf := txn.req.TwriteBuffer()
		n := copy(buf, data[wrote:])

		txn.req.Twrite(f, offset, uint32(n))
		res := <-c.sendRequest(&txn)
		if err := res.err; err != nil {
			c.Errorf("Twrite: Failed to write request: %s", err)
			return wrote, err
		}

		switch r := res.res.Reply().(type) {
		case Rwrite:
			c.Tracef("Rwrite")
			cnt := r.Count()
			wrote += int(cnt)
			offset += uint64(cnt)
			txn = c.resetTxn(txn.req.tag)
		case Rerror:
			err := c.asError(r)
			c.Errorf("Expected Rwrite from server, got error: %s", err)
			return wrote, err
		default:
			c.Errorf("Expected Rwrite from server")
			return wrote, ErrBadFormat
		}
	}
	return wrote, nil
}

// The 9p protocol-level write. Will only write as large as negotiated message buffers allow
func (c *BasicClient) WriteMsg(f Fid, data []byte, offset uint64) (uint32, error) {
	txn, ok := c.allocTxn()
	if !ok {
		return 0, io.ErrUnexpectedEOF
	}
	defer c.release(txn.req.tag)

	size := len(data)
	buf := txn.req.TwriteBuffer()
	if size > len(buf) {
		size = len(buf)
	}
	copy(buf, data[:size])

	txn.req.Twrite(f, offset, uint32(size))
	res := <-c.sendRequest(&txn)
	if err := res.err; err != nil {
		c.Errorf("Twrite: Failed to write request: %s", err)
		return 0, err
	}

	switch r := res.res.Reply().(type) {
	case Rwrite:
		c.Tracef("Rwrite")
		n := r.Count()
		return n, nil
	case Rerror:
		err := c.asError(r)
		c.Errorf("Expected Rwrite from server, got error: %s", err)
		return 0, err
	default:
		c.Errorf("Expected Rwrite from server")
		return 0, ErrBadFormat
	}
}

func (c *BasicClient) Open(f Fid, m OpenMode) (q Qid, iounit uint32, err error) {
	txn, ok := c.allocTxn()
	if !ok {
		return nil, 0, io.ErrUnexpectedEOF
	}
	defer c.release(txn.req.tag)

	txn.req.Topen(f, m)
	res := <-c.sendRequest(&txn)
	if err = res.err; err != nil {
		c.Errorf("Topen: Failed to write request: %s", err)
		return
	}

	switch r := res.res.Reply().(type) {
	case Ropen:
		c.Tracef("Ropen")
		q = r.Qid().Clone()
		iounit = r.Iounit()
		return
	case Rerror:
		err = c.asError(r)
		c.Errorf("Expected Ropen from server, got error: %s", err)
		return
	default:
		c.Errorf("Expected Ropen from server")
		return
	}
}

func (c *BasicClient) Create(f Fid, name string, perm Mode, mode OpenMode) (q Qid, iounit uint32, err error) {
	txn, ok := c.allocTxn()
	if !ok {
		return nil, 0, io.ErrUnexpectedEOF
	}
	defer c.release(txn.req.tag)

	txn.req.Tcreate(f, name, uint32(perm), mode)
	res := <-c.sendRequest(&txn)
	if err = res.err; err != nil {
		c.Errorf("Tcreate: Failed to write request: %s", err)
		return
	}

	switch r := res.res.Reply().(type) {
	case Rcreate:
		c.Tracef("Rcreate")
		q = r.Qid().Clone()
		iounit = r.Iounit()
		return
	case Rerror:
		err = c.asError(r)
		c.Errorf("Expected Rcreate from server, got error: %s", err)
		return
	default:
		c.Errorf("Expected Rcreate from server")
		return
	}
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
	info os.FileInfo
}

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
func (f *FileProxy) Stat() (os.FileInfo, error) {
	if f.info != nil {
		st, err := f.fs.c.Stat(f.fid)
		if err != nil {
			return nil, err
		}
		f.info = st.FileInfo()
	}
	return f.info, nil
}

// Returns file info of the fid. Unlike FileProxy.Stat(), this always fetches
// from the server The new value will still be cached.
func (f *FileProxy) FetchStat() (os.FileInfo, error) {
	st, err := f.fs.c.Stat(f.fid)
	if err != nil {
		return nil, err
	}
	info := st.FileInfo()
	f.info = info
	return info, nil
}

func (f *FileProxy) WriteStat(st Stat) error {
	err := f.fs.c.WriteStat(f.fid, st)
	if err == nil {
		f.info = nil
	}
	return err
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

// Opens a file for reading/writing. Use only if you FileSystemProxy.Traverse()
func (f *FileProxy) Open(flag OpenMode) error {
	qid, _, err := f.fs.c.Open(f.fid, flag)
	if err == nil {
		if qid.Type().IsDir() {
			err = ErrOpenDirNotAllowed
		}
	}
	return err
}

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

///////////////////////

// The contract that FileSystemProxy expects from the underlying 9p client
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

func (fs *FileSystemProxy) MakeDir(path string, mode Mode) error {
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
func (fs *FileSystemProxy) CreateFile(path string, flag OpenMode, mode Mode) (FileHandle, error) {
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
func (fs *FileSystemProxy) OpenFile(path string, flag OpenMode) (FileHandle, error) {
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

func (it *fileSystemProxyIterator) Reset() error {
	it.rst = nil
	it.buf = make([]byte, it.fp.fs.c.MaxMessageSize())
	it.offset = 0
	return nil
}

func (it *fileSystemProxyIterator) NextFileInfo() (os.FileInfo, error) {
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

	var fi os.FileInfo
	var err error

	if len(it.rst) > 0 {
		var st Stat
		st, it.rst, err = readStat(it.fp.fs, it.rst)
		if err != nil {
			return nil, err
		}
		fi = StatFileInfo{st.Clone()}
	}

	if fi == nil {
		var n int
		n, err = it.fp.ReadAt(it.buf, int64(it.offset))
		if n > 0 {
			it.rst = it.buf[:n]
			var st Stat
			st, it.rst, err = readStat(it.fp.fs, it.rst)
			if err != nil {
				return nil, err
			}
			fi = StatFileInfo{st.Clone()}
			it.offset += n
		}
	}

	it.fp.fs.c.Tracef("NextFileInfo() -> %#v, %v", fi, err)

	return fi, err
}
func (it *fileSystemProxyIterator) Close() error { return it.fp.Close() }

func (fs *FileSystemProxy) ListDir(path string) (FileInfoIterator, error) {

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
func (fs *FileSystemProxy) Stat(path string) (os.FileInfo, error) {
	fid := fs.allocFid()
	defer fs.releaseFid(fid)
	if _, err := fs.walk(fid, path); err != nil {
		return nil, err
	}
	st, err := fs.c.Stat(fid)
	fs.c.Clunk(fid)
	return st.FileInfo(), err
}
func (fs *FileSystemProxy) WriteStat(path string, s Stat) error {
	fid := fs.allocFid()
	defer fs.releaseFid(fid)
	if _, err := fs.walk(fid, path); err != nil {
		return err
	}
	err := fs.c.WriteStat(fid, s)
	fs.c.Clunk(fid)
	return err
}
func (fs *FileSystemProxy) Delete(path string) error {
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
