package ninep

import (
	"context"
	"crypto/tls"
	"math"
	"net"
	"strings"
	"time"
)

type Logger interface {
	Printf(format string, values ...interface{})
}

type Replier interface {
	Rversion(msgSize uint32, version string)
	Rattach(qid Qid)
	Ropen(q Qid, iounit uint32)
	RreadBuffer() []byte
	Rread(data []byte)
	Rwalk(wqids []Qid)
	Rstat(s Stat)
	Rclunk()
	Rerror(format string, values ...interface{})

	Disconnect()
}

type Handler interface {
	Handle9P(ctx context.Context, req Message, w Replier)
}

/////////////////////////////////////////////////////////////

type Server struct {
	TLSConfig *tls.Config

	Handler Handler

	ReadTimeout  time.Duration
	WriteTimeout time.Duration
	IdleTimeout  time.Duration

	MaxMsgSize uint32

	ErrorLog, TraceLog Logger
}

func (s *Server) tracef(f string, values ...interface{}) {
	if s.TraceLog != nil {
		s.TraceLog.Printf(f, values...)
	}
}

func (s *Server) errorf(f string, values ...interface{}) {
	if s.ErrorLog != nil {
		s.ErrorLog.Printf(f, values...)
	}
}

func (s *Server) ServeTLS(l net.Listener, certFile, keyFile string) error {
	config := s.TLSConfig
	if config == nil {
		config = new(tls.Config)
	}

	configHasCert := len(config.Certificates) > 0 || config.GetCertificate != nil
	if !configHasCert || certFile != "" || keyFile != "" {
		var err error
		config.Certificates = make([]tls.Certificate, 1)
		config.Certificates[0], err = tls.LoadX509KeyPair(certFile, keyFile)
		if err != nil {
			return err
		}
	}

	tlsListener := tls.NewListener(l, config)
	return s.Serve(tlsListener)
}

func (s *Server) Serve(l net.Listener) error {
	s.tracef("listening on %s", l.Addr())
	retries := 0
	const maxWait = 2 * time.Second
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	for {
		conn, err := l.Accept()
		if err != nil {
			if IsTemporaryErr(err) {
				retries++
				wait := time.Duration(math.Min(math.Pow(float64(10*time.Millisecond), float64(retries)), float64(maxWait)))
				s.tracef("accept error: %s; retrying in %v", err, wait)
				time.Sleep(wait)
			} else {
				return err
			}
		}

		s.tracef("accepted connection from %s", conn.RemoteAddr())
		sess := &serverSession{
			rwc:        conn,
			handler:    s.Handler,
			maxMsgSize: DEFAULT_MAX_MESSAGE_SIZE,
			ctx:        ctx,
			errorLog:   s.ErrorLog,
			traceLog:   s.TraceLog,
		}
		go sess.serve()
	}
}

func (s *Server) ListenAndServe(addr string) error {
	if addr == "" {
		addr = ":9pfs"
	}
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	return s.Serve(ln)
}

func (s *Server) ListenAndServeTLS(addr string, certFile, keyFile string) error {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	return s.ServeTLS(ln, certFile, keyFile)
}

/////////////////////////////////////////////////////////////

// TODO: support multiplexing
type serverSession struct {
	rwc net.Conn
	txn transaction

	handler Handler
	ctx     context.Context

	maxMsgSize uint32

	errorLog, traceLog Logger
}

func (s *serverSession) tracef(f string, values ...interface{}) {
	if s.traceLog != nil {
		s.traceLog.Printf(f, values...)
	}
}

func (s *serverSession) errorf(f string, values ...interface{}) {
	if s.errorLog != nil {
		s.errorLog.Printf(f, values...)
	}
}

func (s *serverSession) acceptTversion() bool {
	preferredSize := s.maxMsgSize
	version := VERSION_9P

	for {
		err := s.txn.readRequest(s.rwc)
		if err != nil {
			s.errorf("failed to negotiate version: error when reading: %s", err)
			return false
		}

		var request Tversion
		{
			var ok bool
			request, ok = s.txn.Request().(Tversion)
			if !ok {
				s.errorf("failed to negotiate version: unexpected message type: %d", s.txn.requestType())
				s.txn.Rerror("unknown")
				return false
			}
		}

		var size uint32
		if request.MsgSize() > preferredSize {
			size = preferredSize
		} else {
			size = request.MsgSize()
		}

		if request.Tag() != NO_TAG {
			s.errorf("Client sent bad tag (got: %d, wanted: NO_TAG/%d)", request.Tag(), NO_TAG)
			return false
		}

		if request.MsgSize() < MIN_MESSAGE_SIZE {
			s.errorf("Client returned below minimum message size than supported (got: %d, min: %d)", request.MsgSize(), MIN_MESSAGE_SIZE)
			return false
		}

		ok := false
		if !strings.HasPrefix(request.Version(), VERSION_9P) {
			s.txn.Rversion(size, "unknown")
			s.tracef("negotiate version: unrecognized protocol version: got %#v, wanted %#v", request.Version(), version)
		} else {
			s.txn.Rversion(size, version)
			ok = true
		}

		err = s.txn.writeReply(s.rwc)
		if err != nil {
			s.errorf("failed to negotiate version: %s", err)
			return false
		}

		if ok {
			return true
		}
	}
}

func (s *serverSession) hasCancelled() bool {
	select {
	case <-s.ctx.Done():
		return true
	default:
		return false
	}
}

// this runs in a new goroutine
func (s *serverSession) serve() {
	defer s.rwc.Close()

	s.txn = newTransaction(s.maxMsgSize)

	if !s.acceptTversion() {
		return
	}

	// we can reallocate it to be smaller
	s.txn = newTransaction(s.maxMsgSize)

	for run := true; run; {
		if s.hasCancelled() {
			s.tracef("received shutdown signal")
			run = false
			break
		}
		err := s.txn.readRequest(s.rwc)
		if err != nil {
			if IsTemporaryErr(err) {
				s.errorf("(temporary) failed to read message: %s", err)
				continue
			} else {
				s.errorf("failed to read message: %s", err)
				break
			}
		}

		if s.hasCancelled() {
			s.tracef("received shutdown signal, erroring request from %s", s.rwc.RemoteAddr())
			s.txn.Rerror("shutting down")
			s.txn.writeReply(s.rwc) // we don't care, we're going away
			run = false
			break
		}
		if !s.handle() {
			run = false
		}

		if s.txn.handled {
			err = s.txn.writeReply(s.rwc)
			if err != nil {
				if IsTemporaryErr(err) {
					s.errorf("(temporary) failed to read message: %s", err)
					continue
				} else {
					s.errorf("failed to write message: %s", err)
					break
				}
			}
		}
	}

	s.tracef("closing connection from %s", s.rwc.RemoteAddr())
}

func (s *serverSession) handle() bool {
	switch m := s.txn.Request().(type) {
	case MsgBase:
		s.txn.Rerror("unknown")
		return false
	default:
		s.handler.Handle9P(s.ctx, m, &s.txn)
		if !s.txn.handled {
			s.txn.Rerror("not implemented")
		}
		return !s.txn.disconnect
	}
}
