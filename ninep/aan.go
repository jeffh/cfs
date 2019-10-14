package ninep

import (
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"syscall"
	"time"
)

var ErrAanBadSlice = errors.New("byte slice is too small")

type aanHeader struct {
	numBytes, msg, ack uint32
}

func (h *aanHeader) pack(b []byte) error {
	if len(b) < 12 {
		return ErrAanBadSlice
	}
	bo.PutUint32(b, h.numBytes)
	bo.PutUint32(b[4:], h.msg)
	bo.PutUint32(b[8:], h.ack)
	return nil
}

func (h *aanHeader) unpack(b []byte) error {
	if len(b) < 12 {
		return ErrAanBadSlice
	}
	h.numBytes = bo.Uint32(b)
	h.msg = bo.Uint32(b[4:])
	h.ack = bo.Uint32(b[8:])
	return nil
}

type aanPayload struct {
	n   int
	b   []byte
	err error

	reply chan aanPayload
}

type aanConn struct {
	m      sync.RWMutex
	inbox  chan aanPayload // requests to read
	outbox chan aanPayload // requests to write
	reconn chan chan error
	rwc    net.Conn

	Timeout          time.Duration
	network, address string

	ln *aanListener

	Loggable
}

// Like the plan9 aan command (always available network).
//
// This converts a network connection to an ordered, available connection.
// This only provides reliability to network outages, and not server restarts.
func DialAan(network, address string) (net.Conn, error) {
	c := &aanConn{
		network:  network,
		address:  address,
		inbox:    make(chan aanPayload),
		outbox:   make(chan aanPayload),
		reconn:   make(chan chan error),
		Loggable: StdLoggable("[aan] "),
	}

	err := c.reconnect()
	if err != nil {
		return nil, err
	}

	go c.processInbox()
	go c.processOutbox()
	go c.processReconnects()
	return c, err
}

type aanListener struct {
	l net.Listener

	m     sync.Mutex
	state map[string]*aanConn
}

func (l *aanListener) forget(addr string) {
	l.m.Lock()
	delete(l.state, addr)
	l.m.Unlock()
}

func (l *aanListener) Accept() (net.Conn, error) {
	conn, err := l.l.Accept()
	if err != nil {
		return nil, err
	}
	l.m.Lock()
	raddr := conn.RemoteAddr().String()
	aconn, ok := l.state[raddr]
	l.m.Unlock()
	if !ok {
		aconn = &aanConn{
			inbox:    make(chan aanPayload),
			outbox:   make(chan aanPayload),
			reconn:   make(chan chan error),
			ln:       l,
			rwc:      conn,
			Loggable: StdLoggable("[aan] "),
		}
		go aconn.processInbox()
		go aconn.processOutbox()
	} else {
		go aconn.notifyReconnected()
	}
	l.m.Lock()
	l.state[raddr] = aconn
	l.m.Unlock()
	return aconn, nil
}
func (l *aanListener) Close() error   { return l.l.Close() }
func (l *aanListener) Addr() net.Addr { return l.l.Addr() }

func ListenAan(network, address string) (net.Listener, error) {
	ln, err := net.Listen(network, address)
	if err != nil {
		return nil, err
	}
	state := make(map[string]*aanConn)
	return &aanListener{l: ln, state: state}, err
}

func (c *aanConn) isServer() bool { return c.ln != nil }

func (c *aanConn) processInbox() {
	reconnReply := make(chan error)

	for {
		p, ok := <-c.inbox
		if !ok {
			return // quit
		}
		p.err = nil
		p.n = 0

		b := p.b
		for p.err == nil && len(b) != 0 {
			c.m.RLock()
			c.rwc.SetReadDeadline(time.Now().Add(20 * time.Millisecond))
			n, err := c.rwc.Read(b)
			c.m.RUnlock()
			b = b[n:]
			if IsTimeoutErr(err) || IsTemporaryErr(err) {
				c.m.RLock()
				c.reconn <- reconnReply
				c.m.RUnlock()
				err = <-reconnReply

				if err == nil {
					continue
				}
			}
			p.n += n
			p.err = err
		}
		c.Tracef("aan: read(%d): %d %v %#v", len(p.b), p.n, p.b, p.err)
		p.reply <- p
	}
}
func (c *aanConn) processOutbox() {
	reconnReply := make(chan error)

	for {
		p, ok := <-c.outbox
		if !ok {
			return // quit
		}
		p.err = nil
		p.n = 0

		b := p.b
		for p.err == nil && len(b) != 0 {
			c.m.RLock()
			c.rwc.SetWriteDeadline(time.Now().Add(20 * time.Millisecond))
			n, err := c.rwc.Write(b)
			c.m.RUnlock()
			b = b[n:]
			if IsTimeoutErr(err) || IsTemporaryErr(err) {
				c.m.RLock()
				c.reconn <- reconnReply
				c.m.RUnlock()
				err = <-reconnReply

				if err == nil {
					continue
				}
			}
			p.n += n
			p.err = err
		}
		c.Tracef("aan: write(%d): %d %v %s", len(p.b), p.n, p.b, p.err)
		p.reply <- p
	}
}

func (c *aanConn) notifyReconnected() {
	c.Tracef("ann: connection restablished")
	for {
		select {
		case reply, ok := <-c.reconn:
			if !ok {
				return
			}
			reply <- nil
		default:
			return
		}
	}
}

func (c *aanConn) processReconnects() {
	for {
		reply, ok := <-c.reconn
		if !ok {
			return
		}
		err := c.reconnect()
		if err != nil {
			c.Close()
			return
		}
		reply <- err
	}
}

func (c *aanConn) reconnect() error {
	c.m.Lock()
	defer c.m.Unlock()
	if c.rwc != nil {
		c.rwc.Close()
		c.rwc = nil
	}

	dialer := net.Dialer{}
	ctx, cancel := context.WithTimeout(context.Background(), c.timeout())
	defer cancel()
	for {
		rwc, err := dialer.DialContext(ctx, c.network, c.address)
		if err == nil {
			c.Tracef("ann: connected: %s %s", c.network, c.address)
			c.rwc = rwc
			err = c.synchronize()
			if err == nil {
				return nil
			}
		}

		if err == syscall.ECONNREFUSED {
			c.Tracef("aan: fatal error: %s", err)
			c.Errorf("aan: fatal error: %s", err)
			return err
		}

		if err, ok := err.(*net.AddrError); ok {
			if err.Timeout() {
				c.Tracef("aan: timeout connect error: %s", err)
				continue
			}
			if err.Temporary() {
				c.Tracef("aan: temporary connect error: %s", err)
				continue
			}
		}

		if err == context.DeadlineExceeded || err == context.Canceled {
			c.Errorf("aan: stop attempting connects: %s", err)
			return err
		}

		if err != nil {
			c.Errorf("aan: ignoring error: %s", err)
		}
	}
}

func (c *aanConn) synchronize() error {
	return nil
	/*
		hdrBuf := [aanHeaderSize]byte{}
		hdr := hdrBuf[:]

		timeout := 5 * time.Second

		for {
			c.Tracef("synchronize")
			c.rwc.SetDeadline(time.Now().Add(timeout))
			_, err := io.ReadFull(c.rwc, hdr)

			if err, ok := err.(*net.AddrError); ok {
				if err.Timeout() {
					c.Tracef("aan: timeout error: %s", err)
					return err
				}
				if err.Temporary() {
					c.Tracef("aan: temporary error: %s", err)
					continue
				}
			}

			if err != nil {
				return err
			}

			break
		}

		buf.Hdr.numBytes = bo.Uint32(hdr[:4])
		buf.Hdr.msgN = bo.Uint32(hdr[4:8])
		buf.Hdr.acked = bo.Uint32(hdr[8:12])

		for {
			c.rwc.SetDeadline(time.Now().Add(timeout))
			_, err := io.ReadFull(c.rwc, buf.Buf[:])

			if err, ok := err.(*net.AddrError); ok {
				if err.Timeout() {
					c.Tracef("aan: timeout error: %s", err)
					return err
				}
				if err.Temporary() {
					c.Tracef("aan: temporary error: %s", err)
					continue
				}
			}

			if err != nil {
				return err
			}

			break
		}

		return nil
	*/
}

func (c *aanConn) timeout() time.Duration {
	if c.Timeout == 0 {
		return 24 * time.Hour
	}
	return c.Timeout
}

func (c *aanConn) Read(b []byte) (n int, err error) {
	reply := make(chan aanPayload)
	p := aanPayload{b: b, reply: reply}
	c.m.RLock()
	defer c.m.RUnlock()
	if c.rwc == nil {
		return 0, io.EOF
	}
	c.inbox <- p
	p = <-reply
	n, err = p.n, p.err
	return
}
func (c *aanConn) Write(b []byte) (n int, err error) {
	reply := make(chan aanPayload)
	p := aanPayload{b: b, reply: reply}
	c.m.RLock()
	defer c.m.RUnlock()
	if c.rwc == nil {
		return 0, io.EOF
	}
	c.outbox <- p
	p = <-reply
	n, err = p.n, p.err
	return
}
func (c *aanConn) Close() error {
	var err error
	c.m.Lock()
	if c.isServer() {
		c.ln.forget(c.rwc.RemoteAddr().String())
	}
	if c.rwc != nil {
		err = c.rwc.Close()
		c.rwc = nil
	}
	// close(c.inbox)
	// close(c.outbox)
	// close(c.reconn)
	c.m.Unlock()
	return err
}
func (c *aanConn) LocalAddr() net.Addr                { return c.rwc.LocalAddr() }
func (c *aanConn) RemoteAddr() net.Addr               { return c.rwc.RemoteAddr() }
func (c *aanConn) SetDeadline(t time.Time) error      { return c.rwc.SetDeadline(t) }
func (c *aanConn) SetReadDeadline(t time.Time) error  { return c.rwc.SetReadDeadline(t) }
func (c *aanConn) SetWriteDeadline(t time.Time) error { return c.rwc.SetWriteDeadline(t) }
