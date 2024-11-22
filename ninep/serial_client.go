package ninep

import (
	"context"
	"crypto/tls"
	"io"
	"log/slog"
	"os"
)

////////////////////////////////////////////////////////////////////////////////////////////

// A 9P Client that uses a transport to manage parallel communications (if any)
// Defaults to serial behavior
type BasicClient struct {
	Transport ClientTransport

	Authorizee  Authorizee
	User, Mount string

	d Dialer

	Logger *slog.Logger
}

var _ Client = (*BasicClient)(nil)

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
				if c.Logger != nil {
					c.Logger.Error("client.auth.prove.failed", slog.String("error", err.Error()))
				}
				return nil, err
			}
		} else {
			if c.Logger != nil {
				c.Logger.Error("client.auth.failed", slog.String("error", err.Error()))
			}
			err = nil
		}
	}
	root, err := c.Attach(f, afid, user, mount)
	if err != nil {
		if c.Logger != nil {
			c.Logger.Error("client.attach.failed", slog.String("error", err.Error()))
		}
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
	network, addr := ParseAddr(addr)
	return c.transport().Connect(c.dialer(), network, addr, c.User, c.Mount, c.Authorizee, c.Logger)
}

func (c *BasicClient) Connect(addr string) error {
	c.d = nil
	return c.connect(addr)
}

func (c *BasicClient) Close() error {
	return c.transport().Disconnect()
}

func (c *BasicClient) asError(r Rerror) error { return r.ToError() }

func (c *BasicClient) Auth(afid Fid, user, mnt string) (Qid, error) {
	if c.Logger != nil {
		c.Logger.Debug("Tauth.request", slog.Uint64("afid", uint64(afid)), slog.String("user", user), slog.String("mnt", mnt))
	}
	t := c.transport()
	txn, ok := t.AllocTransaction()
	if !ok {
		return nil, io.ErrUnexpectedEOF
	}
	defer t.ReleaseTransaction(txn.req.tag)

	txn.req.Tauth(afid, user, mnt)
	msg, err := t.Request(txn)
	if err != nil {
		if c.Logger != nil {
			c.Logger.Error("Tauth.request.failed", slog.String("error", err.Error()))
		}
		return nil, err
	}

	switch r := msg.(type) {
	case Rauth:
		if c.Logger != nil {
			c.Logger.Debug("Tauth.response.ok")
		}
		return r.Aqid(), nil
	case Rerror:
		err := c.asError(r)
		if c.Logger != nil {
			c.Logger.Error("Tauth.response.error", slog.String("error", err.Error()))
		}
		return nil, err
	default:
		if c.Logger != nil {
			c.Logger.Error("Tauth.response.unexpected", slog.String("error", "expected Rauth from server"))
		}
		return nil, ErrInvalidMessage
	}
}

func (c *BasicClient) Attach(fd, afid Fid, user, mnt string) (Qid, error) {
	t := c.transport()
	txn, ok := t.AllocTransaction()
	if !ok {
		return nil, io.ErrUnexpectedEOF
	}
	defer t.ReleaseTransaction(txn.req.tag)

	if c.Logger != nil {
		c.Logger.Debug("Tattach.request", slog.Uint64("fd", uint64(fd)), slog.Uint64("afid", uint64(afid)), slog.String("user", user), slog.String("mnt", mnt))
	}
	txn.req.Tattach(fd, afid, user, mnt)
	msg, err := t.Request(txn)
	if err != nil {
		if c.Logger != nil {
			c.Logger.Error("Tattach.request.failed", slog.String("error", err.Error()))
		}
		return Qid{}, err
	}

	switch r := msg.(type) {
	case Rattach:
		if c.Logger != nil {
			c.Logger.Debug("Tattach.response.ok", slog.String("qid", r.Qid().String()))
		}
		return r.Qid(), nil
	case Rerror:
		err := c.asError(r)
		if c.Logger != nil {
			c.Logger.Error("Tattach.response.error", slog.Any("error", err))
		}
		return nil, err
	default:
		if c.Logger != nil {
			c.Logger.Error("Tattach.response.unexpected", slog.String("error", "expected Rattach from server"))
		}
		return nil, ErrInvalidMessage
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
	if c.Logger != nil {
		c.Logger.Debug(
			"Twalk.request",
			slog.Uint64("f", uint64(f)),
			slog.Uint64("newF", uint64(newF)),
			slog.Int("path_len", len(path)),
			slog.Any("path", path),
			slog.Uint64("size", uint64(txn.req.Request().(interface{ Size() uint32 }).Size())),
		)
	}
	msg, err := t.Request(txn)
	if err != nil {
		if c.Logger != nil {
			c.Logger.Warn("Twalk.request.failed", slog.String("error", err.Error()))
		}
		return nil, err
	}

	switch r := msg.(type) {
	case Rwalk:
		if c.Logger != nil {
			c.Logger.Debug("Rwalk", slog.Int("num_wqid", int(r.NumWqid())))
		}
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
		if c.Logger != nil {
			c.Logger.Error("Twalk.response.error", slog.Any("error", err))
		}
		return nil, err
	default:
		if c.Logger != nil {
			c.Logger.Error("Twalk.response.unexpected", slog.String("error", "expected Rwalk from server"))
		}
		return nil, ErrInvalidMessage
	}
}

func (c *BasicClient) Stat(f Fid) (Stat, error) {
	if c.Logger != nil {
		c.Logger.Debug("Tstat.request", slog.Uint64("f", uint64(f)))
	}
	t := c.transport()
	txn, ok := t.AllocTransaction()
	if !ok {
		return nil, io.ErrUnexpectedEOF
	}
	defer t.ReleaseTransaction(txn.req.tag)
	txn.req.Tstat(f)
	msg, err := t.Request(txn)
	if err != nil {
		if c.Logger != nil {
			c.Logger.Error("Tstat.request.failed", slog.String("error", err.Error()))
		}
		return nil, err
	}

	switch r := msg.(type) {
	case Rstat:
		if c.Logger != nil {
			c.Logger.Debug("Rstat", slog.Uint64("f", uint64(f)), slog.String("stat", r.Stat().String()))
		}
		st := r.Stat().Clone()
		return st, nil
	case Rerror:
		err := c.asError(r)
		if c.Logger != nil {
			c.Logger.Error("Tstat.response.error", slog.Uint64("f", uint64(f)), slog.Any("error", err))
		}
		return nil, err
	default:
		if c.Logger != nil {
			c.Logger.Error("Tstat.response.unexpected", slog.Uint64("f", uint64(f)), slog.String("error", "expected Rstat from server"))
		}
		return nil, ErrInvalidMessage
	}
}

func (c *BasicClient) WriteStat(f Fid, s Stat) error {
	if c.Logger != nil {
		c.Logger.Debug("Twstat.request", slog.Uint64("f", uint64(f)), slog.String("stat", s.String()))
	}
	t := c.transport()
	txn, ok := t.AllocTransaction()
	if !ok {
		return io.ErrUnexpectedEOF
	}
	defer t.ReleaseTransaction(txn.req.tag)
	txn.req.Twstat(f, s)
	msg, err := t.Request(txn)
	if err != nil {
		if c.Logger != nil {
			c.Logger.Error("Twstat.request.failed", slog.String("error", err.Error()))
		}
		return err
	}

	switch r := msg.(type) {
	case Rwstat:
		if c.Logger != nil {
			c.Logger.Debug("Rwstat", slog.Uint64("f", uint64(f)))
		}
		return nil
	case Rerror:
		err := c.asError(r)
		if c.Logger != nil {
			c.Logger.Error("Twstat.response.error", slog.Uint64("f", uint64(f)), slog.Any("error", err))
		}
		return err
	default:
		if c.Logger != nil {
			c.Logger.Error("Twstat.response.unexpected", slog.Uint64("f", uint64(f)), slog.String("error", "expected Rwstat from server"))
		}
		return ErrInvalidMessage
	}
}

func (c *BasicClient) Read(f Fid, p []byte, offset uint64) (int, error) {
	if c.Logger != nil {
		c.Logger.Debug("Tread.request", slog.Uint64("f", uint64(f)), slog.Int("len", len(p)), slog.Uint64("offset", offset))
	}
	t := c.transport()
	txn, ok := t.AllocTransaction()
	if !ok {
		return 0, io.ErrUnexpectedEOF
	}
	defer t.ReleaseTransaction(txn.req.tag)
	txn.req.Tread(f, offset, uint32(len(p)))
	msg, err := t.Request(txn)
	if err != nil {
		if c.Logger != nil {
			c.Logger.Error("Tread.request.failed", slog.Uint64("f", uint64(f)), slog.String("error", err.Error()))
		}
		return 0, err
	}

	switch r := msg.(type) {
	case Rread:
		dat := r.Data()
		if c.Logger != nil {
			c.Logger.Debug("Rread", slog.Uint64("f", uint64(f)), slog.Int("len", len(dat)))
		}
		copy(p, dat)
		return len(dat), nil
	case Rerror:
		err := c.asError(r)
		if c.Logger != nil {
			c.Logger.Error("Tread.response.error", slog.Uint64("f", uint64(f)), slog.Any("error", err))
		}
		return 0, err
	default:
		if c.Logger != nil {
			c.Logger.Error("Tread.response.unexpected", slog.Uint64("f", uint64(f)), slog.String("error", "expected Rread from server"))
		}
		return 0, ErrInvalidMessage
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
		if c.Logger != nil {
			c.Logger.Warn("Tclunk.request.failed", slog.Uint64("f", uint64(f)), slog.String("error", err.Error()))
		}
		return err
	}

	switch r := msg.(type) {
	case Rclunk:
		if c.Logger != nil {
			c.Logger.Debug("Rclunk", slog.Uint64("f", uint64(f)))
		}
		return nil
	case Rerror:
		err := c.asError(r)
		if c.Logger != nil {
			c.Logger.Error("Tclunk.response.error", slog.Uint64("f", uint64(f)), slog.Any("error", err))
		}
		return err
	default:
		if c.Logger != nil {
			c.Logger.Error("Tclunk.response.unexpected", slog.Uint64("f", uint64(f)), slog.String("error", "expected Rclunk from server"))
		}
		return ErrInvalidMessage
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
		if c.Logger != nil {
			c.Logger.Error("Tremove.request.failed", slog.Uint64("f", uint64(f)), slog.String("error", err.Error()))
		}
		return err
	}

	switch r := msg.(type) {
	case Rremove:
		if c.Logger != nil {
			c.Logger.Debug("Rremove", slog.Uint64("f", uint64(f)))
		}
		return nil
	case Rerror:
		err := c.asError(r)
		if c.Logger != nil {
			c.Logger.Error("Tremove.response.error", slog.Uint64("f", uint64(f)), slog.Any("error", err))
		}
		return err
	default:
		if c.Logger != nil {
			c.Logger.Error("Tremove.response.unexpected", slog.Uint64("f", uint64(f)), slog.String("error", "expected Rremove from server"))
		}
		return ErrInvalidMessage
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
			if c.Logger != nil {
				c.Logger.Error("Twrite.request.failed", slog.Uint64("f", uint64(f)), slog.String("error", err.Error()))
			}
			return wrote, err
		}

		switch r := msg.(type) {
		case Rwrite:
			if c.Logger != nil {
				c.Logger.Debug("Rwrite")
			}
			cnt := r.Count()
			wrote += int(cnt)
			offset += uint64(cnt)
			txn.req.reset()
		case Rerror:
			err := c.asError(r)
			if c.Logger != nil {
				c.Logger.Error("Twrite.response.error", slog.Uint64("f", uint64(f)), slog.Any("error", err))
			}
			return wrote, err
		default:
			if c.Logger != nil {
				c.Logger.Error("Twrite.response.unexpected", slog.Uint64("f", uint64(f)), slog.String("error", "expected Rwrite from server"))
			}
			return wrote, ErrInvalidMessage
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
		if c.Logger != nil {
			c.Logger.Error("Twrite.request.failed", slog.Uint64("f", uint64(f)), slog.String("error", err.Error()))
		}
		return 0, err
	}

	switch r := msg.(type) {
	case Rwrite:
		if c.Logger != nil {
			c.Logger.Debug("Rwrite")
		}
		n := r.Count()
		return n, nil
	case Rerror:
		err := c.asError(r)
		if c.Logger != nil {
			c.Logger.Error("Twrite.response.error", slog.Uint64("f", uint64(f)), slog.Any("error", err))
		}
		return 0, err
	default:
		if c.Logger != nil {
			c.Logger.Error("Twrite.response.unexpected", slog.Uint64("f", uint64(f)), slog.String("error", "expected Rwrite from server"))
		}
		return 0, ErrInvalidMessage
	}
}

func (c *BasicClient) Open(f Fid, m OpenMode) (q Qid, iounit uint32, err error) {
	if c.Logger != nil {
		c.Logger.Debug("Open", slog.Uint64("f", uint64(f)), slog.String("mode", m.String()))
	}
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
		if c.Logger != nil {
			c.Logger.Error("Topen.request.failed", slog.Uint64("f", uint64(f)), slog.String("error", err.Error()))
		}
		return
	}

	switch r := msg.(type) {
	case Ropen:
		if c.Logger != nil {
			c.Logger.Debug("Ropen")
		}
		q = r.Qid().Clone()
		iounit = r.Iounit()
		return
	case Rerror:
		err = c.asError(r)
		if c.Logger != nil {
			c.Logger.Error("Topen.response.error", slog.Uint64("f", uint64(f)), slog.Any("error", err))
		}
		return
	default:
		if c.Logger != nil {
			c.Logger.Error("Topen.response.unexpected", slog.Uint64("f", uint64(f)), slog.String("error", "expected Ropen from server"))
		}
		return
	}
}

func (c *BasicClient) Create(f Fid, name string, perm Mode, mode OpenMode) (q Qid, iounit uint32, err error) {
	if c.Logger != nil {
		c.Logger.Debug("Create", slog.Uint64("f", uint64(f)), slog.String("name", name), slog.String("perm", perm.String()), slog.String("mode", mode.String()))
	}
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
		if c.Logger != nil {
			c.Logger.Error("Tcreate.request.failed", slog.Uint64("f", uint64(f)), slog.String("error", err.Error()))
		}
		return
	}

	switch r := msg.(type) {
	case Rcreate:
		if c.Logger != nil {
			c.Logger.Debug("Rcreate")
		}
		q = r.Qid().Clone()
		iounit = r.Iounit()
		return
	case Rerror:
		err = c.asError(r)
		if c.Logger != nil {
			c.Logger.Error("Tcreate.response.error", slog.Uint64("f", uint64(f)), slog.Any("error", err))
		}
		return
	default:
		if c.Logger != nil {
			c.Logger.Error("Tcreate.response.unexpected", slog.Uint64("f", uint64(f)), slog.String("error", "expected Rcreate from server"))
		}
		return
	}
}
