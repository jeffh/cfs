package ninep

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"os"
	"sync"
	"sync/atomic"
)

type ClientTransport interface {
	Connect(d Dialer, addr, usr, mnt string, A Authorizee, L Loggable) error
	Disconnect() error

	MaxMessageSize() uint32

	AllocTransaction() (*cltTransaction, bool)
	ReleaseTransaction(t Tag)
	Request(txn *cltTransaction) (Message, error)
}

type SerialClientTransport struct {
	m   sync.Mutex
	rwc net.Conn
	txn cltTransaction

	maxMsgSize uint32
}

var _ ClientTransport = (*SerialClientTransport)(nil)

func (t *SerialClientTransport) MaxMessageSize() uint32 { return t.maxMsgSize }

func (t *SerialClientTransport) Connect(d Dialer, addr, usr, mnt string, A Authorizee, L Loggable) error {
	t.m.Lock()
	defer t.m.Unlock()
	var err error
	t.rwc, err = d.Dial("tcp", addr)
	if err != nil {
		return err
	}

	txn := createClientTransaction(NO_TAG, DEFAULT_MAX_MESSAGE_SIZE)
	msgSize, err := acceptRversion(L, t.rwc, &txn, DEFAULT_MAX_MESSAGE_SIZE, 0)
	if err != nil {
		t.rwc.Close()
		return err
	}

	t.maxMsgSize = msgSize
	t.txn = createClientTransaction(0, msgSize)
	return nil
}

func (t *SerialClientTransport) Disconnect() error {
	return t.rwc.Close()
}

func (t *SerialClientTransport) NegotiateConn() net.Conn { return t.rwc }
func (t *SerialClientTransport) AllocTransaction() (*cltTransaction, bool) {
	t.m.Lock()
	defer t.m.Unlock()
	t.txn.reset()
	return &t.txn, true
}
func (_ *SerialClientTransport) ReleaseTransaction(t Tag) {}
func (t *SerialClientTransport) Request(txn *cltTransaction) (Message, error) {
	t.m.Lock()
	defer t.m.Unlock()
	return txn.sendAndReceive(t.rwc)
}

////////////////////////////////////////////////////////////////////////////////////////////

type SerialRetryClientTransport struct {
	m   sync.Mutex
	rwc net.Conn
	txn cltTransaction

	nextClientFid uint32
	nextServerFid uint32

	mut  sync.Mutex
	fids map[Fid]fidState // clientFid -> serverFid

	d    Dialer
	addr string
	usr  string
	mnt  string
	A    Authorizee
	Loggable

	maxMsgSize uint32
}

var _ ClientTransport = (*SerialRetryClientTransport)(nil)

func (t *SerialRetryClientTransport) MaxMessageSize() uint32 { return t.maxMsgSize }

func (t *SerialRetryClientTransport) Connect(d Dialer, addr, usr, mnt string, A Authorizee, L Loggable) error {
	t.mut.Lock()
	t.fids = make(map[Fid]fidState)
	t.nextClientFid = 0
	t.nextServerFid = 0
	t.mut.Unlock()

	t.m.Lock()
	defer t.m.Unlock()
	t.d = d
	t.addr = addr
	t.usr = usr
	t.mnt = mnt
	t.A = A
	t.Loggable = L

	return t.unsafeConnect(d, addr, usr, mnt, A, L)
}

func (t *SerialRetryClientTransport) unsafeConnect(d Dialer, addr, usr, mnt string, A Authorizee, L Loggable) error {
	var err error
	t.rwc, err = d.Dial("tcp", addr)
	if err != nil {
		return err
	}

	txn := createClientTransaction(NO_TAG, DEFAULT_MAX_MESSAGE_SIZE)
	msgSize, err := acceptRversion(L, t.rwc, &txn, DEFAULT_MAX_MESSAGE_SIZE, 0)
	if err != nil {
		t.rwc.Close()
		return err
	}

	t.maxMsgSize = msgSize
	t.txn = createClientTransaction(0, msgSize)

	return nil
}

func (t *SerialRetryClientTransport) Disconnect() error {
	t.m.Lock()
	defer t.m.Unlock()
	return t.rwc.Close()
}

func (t *SerialRetryClientTransport) NegotiateConn() net.Conn { return t.rwc }
func (t *SerialRetryClientTransport) AllocTransaction() (*cltTransaction, bool) {
	t.m.Lock()
	defer t.m.Unlock()
	t.txn.reset()
	return &t.txn, true
}
func (_ *SerialRetryClientTransport) ReleaseTransaction(t Tag) {}
func (t *SerialRetryClientTransport) Request(txn *cltTransaction) (Message, error) {

	const numRetries = 3

	t.m.Lock()
	defer t.m.Unlock()

	newMappings := make(map[Fid]Fid)
	req := txn.req.Request()
	tmp := txn.req.clone()
	orig := tmp.Request()
	RemapFids(req, func(a Fid) Fid {
		if a == NO_FID {
			return a
		}
		value, found := t.fids[a]
		if found {
			return value.serverFid
		}

		v, found := newMappings[a]
		if found {
			return v
		}

		newFid := Fid(atomic.AddUint32(&t.nextServerFid, 1))
		newMappings[a] = newFid
		return newFid
	})
	for {
		msg, err := txn.sendAndReceive(t.rwc)
		fmt.Printf("sendAndRecv(%s) -> %s %v\n", txn.req.requestType(), txn.res.responseType(), err)

		switch req.(type) {
		case Tclunk:
			fid := orig.(Tclunk).Fid()
			t.Tracef("Save: Tclunk(%d, ..., ...)", fid)
			// always clunk
			t.mut.Lock()
			delete(t.fids, fid)
			t.mut.Unlock()
		default: // do nothing
		}

		if err == nil {
			switch m := msg.(type) {
			case Rauth:
				r := req.(Tauth)
				t.mut.Lock()
				t.fids[orig.(Tauth).Afid()] = fidState{
					qtype:     m.Aqid().Type(),
					path:      []string{""},
					mode:      M_AUTH,
					serverFid: r.Afid(),
					uname:     r.Uname(),
					aname:     r.Aname(),
				}
				t.Tracef("Save: Rauth(%d, ..., ...)", r.Afid())
				t.mut.Unlock()
			case Rattach:
				r := req.(Tattach)
				t.mut.Lock()
				t.fids[orig.(Tattach).Fid()] = fidState{
					qtype:      m.Qid().Type(),
					path:       []string{""},
					mode:       M_MOUNT,
					serverFid:  r.Fid(),
					serverAfid: r.Afid(),
					uname:      r.Uname(),
					aname:      r.Aname(),
				}
				t.Tracef("Save: Rattach(%d, %d, ..., ...)", r.Fid(), r.Afid())
				t.mut.Unlock()
			case Rwalk:
				r := req.(Twalk)
				var qid Qid
				if m.NumWqid() == r.NumWname() {
					qid = m.Wqid(int(m.NumWqid() - 1))
				}
				t.mut.Lock()
				t.fids[orig.(Twalk).NewFid()] = fidState{
					qtype:     qid.Type(),
					path:      []string{""},
					mode:      M_MOUNT,
					serverFid: r.NewFid(),
				}
				t.Tracef("Save: Rwalk(%d, %d, ..., ...)", r.Fid(), r.NewFid())
				t.mut.Unlock()
			default:
				// do nothing
			}

			return msg, nil
		}

		if IsClosedSocket(err) || IsTimeoutErr(err) || IsTemporaryErr(err) {
			txn = txn.clone()
			err = t.rwc.Close()
			if err != nil {
				return nil, err
			}
			err = t.unsafeConnect(t.d, t.addr, t.usr, t.mnt, t.A, t.Loggable)
			if err != nil {
				return nil, err
			}

			stateTxn := createClientTransaction(1, t.maxMsgSize)

			// since we're a new 9p session, "push" our internal state to the remote
			// server again
			for _, state := range t.fids {
				stateTxn.reset()
				t.Tracef("Restore: %#v", state)

				state.m.Lock()
				if state.mode&M_MOUNT != 0 {
					t.Tracef("Restore: Tattach(%d, %d, ..., ...)", state.serverFid, state.serverAfid)
					stateTxn.req.Tattach(state.serverFid, state.serverAfid, state.uname, state.aname)
					m, err := stateTxn.sendAndReceive(t.rwc)
					if err != nil {
						return nil, fmt.Errorf("Failed to reattach mount: %w", err)
					}
					_, ok := m.(Rattach)
					if !ok {
						state.m.Unlock()
						return nil, fmt.Errorf("Expected Rattach from server, got %s", stateTxn.res.responseType())
					}
				} else {
					for i := 0; i < numRetries; i++ {
						t.Tracef("Restore: Twalk(%d, %d, ..., ...)", state.serverFid, state.serverAfid)
						stateTxn.req.Twalk(recoverMntFid, state.serverFid, state.path)
						m, err := stateTxn.sendAndReceive(t.rwc)
						if err != nil {
							// clunk, b/c its easier right now
							stateTxn.reset()
							stateTxn.req.Tclunk(state.serverFid)
							_, _ = stateTxn.sendAndReceive(t.rwc) // ignoring error
							continue                              // retry
						}
						_, ok := m.(Rwalk)
						if !ok {
							state.m.Unlock()
							return nil, fmt.Errorf("Expected Rwalk from server, got %s", stateTxn.res.responseType())
						}
						if state.opened {
							stateTxn.reset()
							t.Tracef("Restore: Topen(%d, %s, ..., ...)", state.serverFid, state.flag)
							stateTxn.req.Topen(state.serverFid, state.flag)
							m, err = stateTxn.sendAndReceive(t.rwc)
							if err != nil {
								// clunk, b/c its easier right now
								stateTxn.reset()
								stateTxn.req.Tclunk(state.serverFid)
								_, e := stateTxn.sendAndReceive(t.rwc) // ignoring error
								if e != nil {
									state.m.Unlock()
									return nil, err
								}
								continue // retry
							}

							_, ok := m.(Ropen)
							if !ok {
								state.m.Unlock()
								return nil, fmt.Errorf("Failed to re-open file: expected Ropen, got %s", stateTxn.res.responseType())
							}
						}
					}
				}
				state.m.Unlock()

				if err != nil {
					// failed
					return nil, err
				}
			}
			continue
		}
		return msg, err
	}
}

////////////////////////////////////////////////////////////////////////////////////////////

// A 9P Client that uses a transport to manage parallel communications (if any)
// Defaults to serial behavior
type SerialClient struct {
	Transport ClientTransport

	Authorizee  Authorizee
	User, Mount string

	d Dialer

	Loggable
}

var _ FileSystemProxyClient = (*SerialClient)(nil)

func (c *SerialClient) MaxMessageSize() uint32 { return c.transport().MaxMessageSize() }

func (c *SerialClient) dialer() Dialer {
	if c.d == nil {
		c.d = &TCPDialer{}
	}
	return c.d
}
func (c *SerialClient) transport() ClientTransport {
	if c.Transport == nil {
		c.Transport = &SerialClientTransport{}
	}
	return c.Transport
}

// Returns an interface that conforms to the file system interface
// Can be used once Connect*() are called and successful
func (c *SerialClient) Fs() (*FileSystemProxy, error) {
	afid := NO_FID
	f := Fid(1)
	user, mount := c.User, c.Mount
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

func (c *SerialClient) ConnectTLS(addr string, tlsCfg *tls.Config) error {
	c.d = &TLSDialer{
		&TCPDialer{},
		tlsCfg,
	}
	return c.connect(addr)
}

func (c *SerialClient) connect(addr string) error {
	return c.transport().Connect(c.dialer(), addr, c.User, c.Mount, c.Authorizee, c.Loggable)
}

func (c *SerialClient) Connect(addr string) error {
	c.d = nil
	return c.connect(addr)
}

func (c *SerialClient) Close() error {
	return c.transport().Disconnect()
}

func (c *SerialClient) asError(r Rerror) error { return r.Error() }

func (c *SerialClient) Auth(afid Fid, user, mnt string) (Qid, error) {
	c.Tracef("Tauth(%d, %v, %v)", afid, user, mnt)
	t := c.transport()
	txn, ok := t.AllocTransaction()
	if !ok {
		return nil, io.ErrUnexpectedEOF
	}
	defer t.ReleaseTransaction(txn.req.tag)

	txn.req.Tauth(afid, user, mnt)
	msg, err := t.Request(txn)
	if err != nil {
		c.Errorf("Tauth: Failed requesting: %s", err)
		return nil, err
	}

	switch r := msg.(type) {
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

func (c *SerialClient) Attach(fd, afid Fid, user, mnt string) (Qid, error) {
	t := c.transport()
	txn, ok := t.AllocTransaction()
	if !ok {
		return nil, io.ErrUnexpectedEOF
	}
	defer t.ReleaseTransaction(txn.req.tag)

	c.Tracef("Tattach(%d, %d, %#v, %#v)", fd, afid, user, mnt)
	txn.req.Tattach(fd, afid, user, mnt)
	msg, err := t.Request(txn)
	if err != nil {
		c.Errorf("Tattach: Failed to write request: %s", err)
		return Qid{}, err
	}

	switch r := msg.(type) {
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

func (c *SerialClient) Walk(f, newF Fid, path []string) ([]Qid, error) {
	t := c.transport()
	txn, ok := t.AllocTransaction()
	if !ok {
		return nil, io.ErrUnexpectedEOF
	}
	defer t.ReleaseTransaction(txn.req.tag)
	txn.req.Twalk(f, newF, path)
	c.Tracef("Twalk %s -> %s %#v %d", f, newF, path, txn.req.Request().(interface{ Size() uint32 }).Size())
	msg, err := t.Request(txn)
	if err != nil {
		c.Errorf("Twalk: Failed to write request: %s", err)
		return nil, err
	}

	switch r := msg.(type) {
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

func (c *SerialClient) Stat(f Fid) (Stat, error) {
	c.Tracef("Stat(%d)", f)
	t := c.transport()
	txn, ok := t.AllocTransaction()
	if !ok {
		return nil, io.ErrUnexpectedEOF
	}
	defer t.ReleaseTransaction(txn.req.tag)
	txn.req.Tstat(f)
	msg, err := t.Request(txn)
	if err != nil {
		c.Errorf("Tstat: Failed to write request: %s", err)
		return nil, err
	}
	c.Tracef("Tstat %s", f)

	switch r := msg.(type) {
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

func (c *SerialClient) WriteStat(f Fid, s Stat) error {
	c.Tracef("WriteStat(%d, _)", f)
	t := c.transport()
	txn, ok := t.AllocTransaction()
	if !ok {
		return io.ErrUnexpectedEOF
	}
	defer t.ReleaseTransaction(txn.req.tag)
	txn.req.Twstat(f, s)
	msg, err := t.Request(txn)
	if err != nil {
		c.Errorf("Twstat: Failed to write request: %s", err)
		return err
	}

	switch r := msg.(type) {
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

func (c *SerialClient) Read(f Fid, p []byte, offset uint64) (int, error) {
	c.Tracef("Read(%s, []byte(%d), %v)", f, len(p), offset)
	t := c.transport()
	txn, ok := t.AllocTransaction()
	if !ok {
		return 0, io.ErrUnexpectedEOF
	}
	defer t.ReleaseTransaction(txn.req.tag)
	txn.req.Tread(f, offset, uint32(len(p)))
	msg, err := t.Request(txn)
	if err != nil {
		c.Errorf("Tread: Failed to write request: %s", err)
		return 0, err
	}

	switch r := msg.(type) {
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

func (c *SerialClient) Clunk(f Fid) error {
	t := c.transport()
	txn, ok := t.AllocTransaction()
	if !ok {
		return io.ErrUnexpectedEOF
	}
	defer t.ReleaseTransaction(txn.req.tag)
	txn.req.Tclunk(f)
	msg, err := t.Request(txn)
	if err != nil {
		c.Errorf("Tclunk: Failed to write request: %s", err)
		return err
	}

	switch r := msg.(type) {
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

func (c *SerialClient) Remove(f Fid) error {
	t := c.transport()
	txn, ok := t.AllocTransaction()
	if !ok {
		return io.ErrUnexpectedEOF
	}
	defer t.ReleaseTransaction(txn.req.tag)
	txn.req.Tremove(f)
	msg, err := t.Request(txn)
	if err != nil {
		c.Errorf("Tremove: Failed to write request: %s", err)
		return err
	}

	switch r := msg.(type) {
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
func (c *SerialClient) Write(f Fid, data []byte, offset uint64) (int, error) {
	size := len(data)
	wrote := 0

	t := c.transport()
	txn, ok := t.AllocTransaction()
	if !ok {
		return 0, io.ErrUnexpectedEOF
	}
	defer t.ReleaseTransaction(txn.req.tag)
	for wrote < size {
		buf := txn.req.TwriteBuffer()
		n := copy(buf, data[wrote:])

		txn.req.Twrite(f, offset, uint32(n))
		msg, err := t.Request(txn)
		if err != nil {
			c.Errorf("Twrite: Failed to write request: %s", err)
			return wrote, err
		}

		switch r := msg.(type) {
		case Rwrite:
			c.Tracef("Rwrite")
			cnt := r.Count()
			wrote += int(cnt)
			offset += uint64(cnt)
			txn.req.reset()
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
func (c *SerialClient) WriteMsg(f Fid, data []byte, offset uint64) (uint32, error) {
	t := c.transport()
	txn, ok := t.AllocTransaction()
	if !ok {
		return 0, io.ErrUnexpectedEOF
	}
	defer t.ReleaseTransaction(txn.req.tag)

	size := len(data)
	buf := txn.req.TwriteBuffer()
	if size > len(buf) {
		size = len(buf)
	}
	copy(buf, data[:size])

	txn.req.Twrite(f, offset, uint32(size))
	msg, err := t.Request(txn)
	if err != nil {
		c.Errorf("Twrite: Failed to write request: %s", err)
		return 0, err
	}

	switch r := msg.(type) {
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

func (c *SerialClient) Open(f Fid, m OpenMode) (q Qid, iounit uint32, err error) {
	c.Tracef("Open(%d, %s)", f, m)
	t := c.transport()
	txn, ok := t.AllocTransaction()
	if !ok {
		err = io.ErrUnexpectedEOF
		return
	}
	defer t.ReleaseTransaction(txn.req.tag)
	txn.req.Topen(f, m)
	msg, err := t.Request(txn)
	if err != nil {
		c.Errorf("Topen: Failed to write request: %s", err)
		return
	}

	switch r := msg.(type) {
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

func (c *SerialClient) Create(f Fid, name string, perm Mode, mode OpenMode) (q Qid, iounit uint32, err error) {
	c.Tracef("Create(%d, %#v, %s, %d)", f, name, perm, mode)
	t := c.transport()
	txn, ok := t.AllocTransaction()
	if !ok {
		err = io.ErrUnexpectedEOF
		return
	}
	defer t.ReleaseTransaction(txn.req.tag)
	txn.req.Tcreate(f, name, uint32(perm), mode)
	msg, err := t.Request(txn)
	if err != nil {
		c.Errorf("Tcreate: Failed to write request: %s", err)
		return
	}

	switch r := msg.(type) {
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
