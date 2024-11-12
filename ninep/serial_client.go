package ninep

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"os"
)

////////////////////////////////////////////////////////////////////////////////////////////

// A 9P Client that uses a transport to manage parallel communications (if any)
// Defaults to serial behavior
type BasicClient struct {
	Transport ClientTransport

	Authorizee  Authorizee
	User, Mount string
	Network     string

	d Dialer

	Loggable
}

var _ FileSystemProxyClient = (*BasicClient)(nil)

func (c *BasicClient) MaxMessageSize() uint32 { return c.transport().MaxMessageSize() }

func (c *BasicClient) dialer() Dialer {
	if c.d == nil {
		c.d = &TCPDialer{}
	}
	return c.d
}
func (c *BasicClient) transport() ClientTransport {
	if c.Transport == nil {
		c.Transport = &SerialClientTransport{}
	}
	return c.Transport
}

// Returns an interface that conforms to the file system interface
// Can be used once Connect*() are called and successful
func (c *BasicClient) Fs() (*FileSystemProxy, error) {
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

func (c *BasicClient) ConnectTLS(addr string, tlsCfg *tls.Config) error {
	c.d = &TLSDialer{
		&TCPDialer{},
		tlsCfg,
	}
	return c.connect(addr)
}

func (c *BasicClient) connect(addr string) error {
	return c.transport().Connect(c.dialer(), c.getNetwork(), addr, c.User, c.Mount, c.Authorizee, c.Loggable)
}

func (c *BasicClient) getNetwork() string {
	if c.Network == "" {
		return "tcp"
	}
	return c.Network
}

func (c *BasicClient) Connect(addr string) error {
	c.d = nil
	return c.connect(addr)
}

func (c *BasicClient) Close() error {
	return c.transport().Disconnect()
}

func (c *BasicClient) asError(r Rerror) error { return r.Error() }

func (c *BasicClient) Auth(afid Fid, user, mnt string) (Qid, error) {
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

func (c *BasicClient) Attach(fd, afid Fid, user, mnt string) (Qid, error) {
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
		c.Errorf("Expected Rattach from server, got: %#v", r)
		return nil, ErrBadFormat
	}
}

func (c *BasicClient) Walk(f, newF Fid, path []string) ([]Qid, error) {
	t := c.transport()
	txn, ok := t.AllocTransaction()
	if !ok {
		return nil, io.ErrUnexpectedEOF
	}
	defer t.ReleaseTransaction(txn.req.tag)
	txn.req.Twalk(f, newF, path)
	c.Tracef("Twalk %s -> %s %v %#v %d", f, newF, len(path), path, txn.req.Request().(interface{ Size() uint32 }).Size())
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
		c.Errorf("Expected Rwalk from server, got error: %s", err)
		return nil, err
	default:
		c.Errorf("Expected Rwalk from server, got %v", r)
		return nil, ErrBadFormat
	}
}

func (c *BasicClient) Stat(f Fid) (Stat, error) {
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

func (c *BasicClient) WriteStat(f Fid, s Stat) error {
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

func (c *BasicClient) Read(f Fid, p []byte, offset uint64) (int, error) {
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

func (c *BasicClient) Clunk(f Fid) error {
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

func (c *BasicClient) Remove(f Fid) error {
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
func (c *BasicClient) Write(f Fid, data []byte, offset uint64) (int, error) {
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
func (c *BasicClient) WriteMsg(f Fid, data []byte, offset uint64) (uint32, error) {
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

func (c *BasicClient) Open(f Fid, m OpenMode) (q Qid, iounit uint32, err error) {
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

func (c *BasicClient) Create(f Fid, name string, perm Mode, mode OpenMode) (q Qid, iounit uint32, err error) {
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
