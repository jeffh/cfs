package ninep

// Always Available Network (AAN)
//
// Not a complete implementation. Needs the following:
//
// [ ] A synchronize message (^uint32(0)) that allows unacked messages to be resent.
// [ ] Support SetDeadline, SetReadDeadline, and SetWriteDeadline
// [ ] Exponential backoff for reconnects

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

var ErrAanConnClosed = errors.New("connection already closed")

const (
	aanSyncMsg = ^uint32(0)
)

type aanHeader []byte

func newAanHeader() aanHeader            { return aanHeader(make([]byte, 4*3)) }
func (h aanHeader) numBytes() uint32     { return bo.Uint32(h) }
func (h aanHeader) setNumBytes(v uint32) { bo.PutUint32(h, v) }
func (h aanHeader) msg() uint32          { return bo.Uint32(h[4:]) }
func (h aanHeader) setMsg(v uint32)      { bo.PutUint32(h[4:], v) }
func (h aanHeader) ack() uint32          { return bo.Uint32(h[8:]) }
func (h aanHeader) setAck(v uint32)      { bo.PutUint32(h[8:], v) }

func (h aanHeader) isEOF() bool { return h.numBytes() == 0 }

type aanPayload struct {
	n   int
	b   []byte
	err error

	reply chan aanPayload
}

type aanConn struct {
	m       sync.RWMutex
	inbox   chan aanPayload // requests to read
	outbox  chan aanPayload // requests to write
	unacked [][]byte        // written requests unacked
	reconn  chan chan error // GROT
	pauseCh chan bool
	rwc     net.Conn

	nwritten uint32
	acks     uint32

	Timeout          time.Duration
	network, address string

	ln *aanListener

	Loggable
}

// Like the plan9 aan command (always available network). But not fully
// compatible with the plan9 version.
//
// This converts a network connection to an ordered, available connection.
// This only provides reliability to network outages, and not server restarts.
func DialAan(network, address string) (net.Conn, error) {
	c := &aanConn{
		network:  network,
		address:  address,
		inbox:    make(chan aanPayload),
		outbox:   make(chan aanPayload),
		unacked:  make([][]byte, 0, 32),
		reconn:   make(chan chan error),
		Loggable: StdLoggable("[aan] "),
	}

	err := c.reconnect()
	if err != nil {
		return nil, err
	}

	go c.ioLoop()
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
retry:
	conn, err := l.l.Accept()
	if err != nil {
		return nil, err
	}
	// TODO: negotiate version
	raddr := conn.RemoteAddr().String()
	if addr, _, err := net.SplitHostPort(raddr); err == nil {
		raddr = addr // not the best, one session per host!
	}

	l.m.Lock()
	aconn, ok := l.state[raddr]
	l.m.Unlock()
	fmt.Printf("aan: accept: %s -> existing=%v\n", raddr, ok)
	if !ok {
		aconn = &aanConn{
			inbox:    make(chan aanPayload),
			outbox:   make(chan aanPayload),
			unacked:  make([][]byte, 0, 32),
			reconn:   make(chan chan error),
			pauseCh:  make(chan bool, 1),
			ln:       l,
			rwc:      conn,
			Loggable: StdLoggable("[aan] "),
		}
		go aconn.ioLoop()
	} else {
		aconn.m.Lock()
		aconn.rwc = conn
		aconn.m.Unlock()
		aconn.unpause()
		// this should be "invisible" to consumer since that looks like a
		// connection that still hasn't closed
		goto retry
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
func (c *aanConn) unpause()       { c.pauseCh <- true }
func (c *aanConn) pause() bool {
	select {
	case _, ok := <-c.pauseCh:
		return ok
	case <-time.After(24 * time.Hour):
		return false
	}
}

func (c *aanConn) ioLoop() {
	const readTimeout = 5 * time.Second
	const writeTimeout = 5 * time.Second

	read := func(c *aanConn, b []byte) (int, error) {
		c.m.RLock()
		rwc := c.rwc
		c.m.RUnlock()
		if c.rwc == nil {
			return 0, ErrAanConnClosed
		}
		c.rwc.SetReadDeadline(time.Now().Add(readTimeout))
		n, err := rwc.Read(b)
		c.Tracef("aan: read(%d) %v, %v", len(b), n, err)
		return n, err
	}

	write := func(c *aanConn, b []byte) (int, error) {
		c.m.RLock()
		rwc := c.rwc
		c.m.RUnlock()
		if rwc == nil {
			return 0, ErrAanConnClosed
		}
		c.rwc.SetWriteDeadline(time.Now().Add(writeTimeout))
		n, err := rwc.Write(b)
		c.Tracef("aan: write(%v) %v, %v", b, n, err)
		return n, err
	}

	reconnect := func(c *aanConn, err error, hdr aanHeader) error {
		c.disconnect()
		if c.isServer() {
			c.Tracef("aan: connection lost: %v", err)
			// client must initiate the reconnect
			if c.pause() {
				c.Tracef("aan: connection resumed")
			} else {
				return io.ErrUnexpectedEOF
			}
		} else {
			err = c.reconnect()
			if err != nil {
				c.Tracef("aan: reconnect failed: %v", err)
				c.Close()
				return err
			}
		}

		// rewrite all unacked messages
		for _, msg := range c.unacked {
			write(c, msg)
		}
		c.Tracef("aan: reconnected (%d unacked msgs)", len(c.unacked))

		return nil
	}

	// reading state
	rhdr := newAanHeader()
	rem := []byte(nil)

	// writing state
	whdr := newAanHeader()
	syncHdr := newAanHeader()
	syncHdr.setMsg(aanSyncMsg)
	atomic.StoreUint32(&c.nwritten, 1)

	// FIXME(jeff): Reading and Writing need to be on separate goroutines
	//              So we can park a read / write without having to set a timeout.
	// After splitting, we need to account for the following:
	// - Have a heartbeat message that can timeout on inactivity
	// - Figure out how to survive wrapping of acknowledge msg counter
	// - avoid deadlocks when good connectivity

	for {
		select {
		case p, ok := <-c.inbox:
			if !ok {
				return // quit
			}
			p.err = nil
			p.n = 0

			pb := p.b
			var (
				n   int
				err error
				b   []byte

				acks uint32
				size int
			)

			if len(rem) > 0 {
				size = copy(pb, rem)
				rem = rem[size:]
				pb = pb[size:]

				if len(pb) == 0 {
					goto returnMsg
				}
			}

		retryHeaderRead:
			n, err = read(c, rhdr)
			if IsAanRecoverableErr(err) {
				err = reconnect(c, err, whdr)
				if err == nil {
					goto retryHeaderRead
				}
			}

			acks = atomic.LoadUint32(&c.acks)
			if rhdr.isEOF() {
				c.Tracef("aan: EOF header")
				p.n = 0
				p.err = io.EOF
				goto returnMsg
			} else if rhdr.msg() <= acks {
				c.Tracef("aan: skipping dup msg of %d (< %d)", rhdr.msg(), acks)
				goto retryHeaderRead // we already saw this message
			}

			// TODO(jeff): use fixed buf size, but support subsequent reads
			rem = make([]byte, rhdr.numBytes())
			b = rem
			for p.err == nil && len(b) != 0 {
				n, err = read(c, b)
				b = b[n:]
				if IsAanRecoverableErr(err) {
					err = reconnect(c, err, whdr)
				}
				p.err = err
			}
			size = copy(pb, rem)
			rem = rem[size:]
			pb = pb[size:]
			for atomic.AddUint32(&c.acks, 1) == aanSyncMsg {
			}

		returnMsg:
			p.n = len(p.b) - len(pb)
			c.Tracef("aan: read(%d): %d %v %#v", len(p.b), p.n, p.b, p.err)
			p.reply <- p

			// clear acked messages
			endIndex := -1
			for i, msg := range c.unacked {
				if aanHeader(msg).msg() > rhdr.ack() {
					endIndex = i
					break
				}
			}
			if endIndex >= 0 {
				c.Tracef("aan: remote acked %d messages", endIndex)
				size := copy(c.unacked, c.unacked[endIndex:])
				c.unacked = c.unacked[:size]
			} else {
				c.Tracef("aan: remote acked %d messages", len(c.unacked))
				c.unacked = c.unacked[:0]
			}

		case p, ok := <-c.outbox:
			if !ok {
				return // quit
			}
			p.err = nil
			p.n = 0

			var (
				n   int
				err error
				b   []byte
			)

			whdr.setNumBytes(uint32(len(p.b)))
			whdr.setMsg(atomic.LoadUint32(&c.nwritten))
		retryHeaderWrite:
			whdr.setAck(atomic.LoadUint32(&c.acks))
			_, err = write(c, whdr)
			if IsAanRecoverableErr(err) {
				err = reconnect(c, err, whdr)
				if err == nil {
					goto retryHeaderWrite
				}
			}

			b = p.b
			for p.err == nil && len(b) != 0 {
				n, err = write(c, b)
				b = b[n:]
				p.n += n
				if IsAanRecoverableErr(err) {
					err = reconnect(c, err, whdr)
				}
				p.err = err
			}
			for atomic.AddUint32(&c.nwritten, 1) == aanSyncMsg {
			}

			{
				msg := make([]byte, len(whdr)+len(p.b))
				off := copy(msg, whdr)
				copy(msg[off:], p.b)
				c.unacked = append(c.unacked, msg)
			}
			c.Tracef("aan: write (unacked: %d)", len(c.unacked))

			// c.Tracef("aan: write(%d): %d %v %s", len(p.b), p.n, p.b, p.err)
			p.reply <- p
		}
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
		if tcp, ok := rwc.(*net.TCPConn); ok {
			if err := tcp.SetKeepAlive(true); err != nil {
				rwc.Close()
				return err
			}
			if err := tcp.SetKeepAlivePeriod(1 * time.Second); err != nil {
				rwc.Close()
				return err
			}
		}
		if err == nil {
			c.Tracef("ann: connected: %s %s", c.network, c.address)
			c.rwc = rwc
			err = c.synchronize()
			if err == nil {
				return nil
			}
		}

		if strings.Index(err.Error(), "connection refused") != -1 {
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

func (c *aanConn) unsafeWriteClose() error {
	hdr := newAanHeader()
	hdr.setNumBytes(0)
	hdr.setMsg(math.MaxUint32)
	hdr.setAck(atomic.LoadUint32(&c.acks))
	c.rwc.SetWriteDeadline(time.Now().Add(50 * time.Millisecond))
	_, err := c.rwc.Write(hdr)
	return err
}

func (c *aanConn) synchronize() error {
	// TODO: GROT
	// based on the code in aan.c, this seems like a copy(chan, chan) behavior
	// that we don't need since we're being more wasteful and separating our
	// read + writes into separate goroutines.
	return nil
}

func (c *aanConn) timeout() time.Duration {
	if c.Timeout == 0 {
		return 24 * time.Hour
	}
	return c.Timeout
}

func (c *aanConn) Read(b []byte) (n int, err error) {
	c.Tracef("aan: read req(%d)", len(b))
	if len(b) == 0 {
		return 0, nil
	}
	reply := make(chan aanPayload, 1)
	p := aanPayload{b: b, reply: reply}
	c.m.RLock()
	rwc := c.rwc
	c.m.RUnlock()
	if rwc == nil {
		return 0, io.EOF
	}
	c.inbox <- p
	p = <-reply
	n, err = p.n, p.err
	c.Tracef("aan: read req(%d) -> %v %v", len(b), n, err)
	return
}
func (c *aanConn) Write(b []byte) (n int, err error) {
	c.Tracef("aan: write req(%d)", len(b))
	if len(b) == 0 {
		return 0, nil
	}
	reply := make(chan aanPayload, 1)
	p := aanPayload{b: b, reply: reply}
	c.m.RLock()
	rwc := c.rwc
	c.m.RUnlock()
	if rwc == nil {
		return 0, io.EOF
	}
	c.outbox <- p
	p = <-reply
	n, err = p.n, p.err
	c.Tracef("aan: write req(%d) -> %v %v", len(b), n, err)
	return
}
func (c *aanConn) disconnect() error {
	c.m.Lock()
	err := c.rwc.Close()
	c.m.Unlock()
	return err
}
func (c *aanConn) Close() error {
	var err error
	c.m.Lock()
	if c.isServer() {
		c.ln.forget(c.rwc.RemoteAddr().String())
	}
	if c.rwc != nil {
		c.unsafeWriteClose()
		err = c.rwc.Close()
		c.rwc = nil
		close(c.inbox)
		close(c.outbox)
		close(c.reconn)
	}
	c.m.Unlock()
	c.Tracef("aan: close")
	return err
}
func (c *aanConn) LocalAddr() net.Addr                { return c.rwc.LocalAddr() }
func (c *aanConn) RemoteAddr() net.Addr               { return c.rwc.RemoteAddr() }
func (c *aanConn) SetDeadline(t time.Time) error      { return c.rwc.SetDeadline(t) }
func (c *aanConn) SetReadDeadline(t time.Time) error  { return nil }
func (c *aanConn) SetWriteDeadline(t time.Time) error { return c.rwc.SetWriteDeadline(t) }

// func (c *aanConn) SetReadDeadline(t time.Time) error  { return c.rwc.SetReadDeadline(t) }
