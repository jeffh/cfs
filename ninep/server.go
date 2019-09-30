package ninep

import (
	"context"
	"crypto/tls"
	"fmt"
	"math"
	"net"
	"strings"
	"sync"
	"sync/atomic"
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
	Rwstat()
	Rclunk()
	Rremove()
	Rcreate(q Qid, iounit uint32)
	Rwrite(count uint32)
	Rerror(err error)
	Rerrorf(format string, values ...interface{})

	Disconnect()
	RemoteAddr() string
}

type Handler interface {
	Disconnected(remoteAddr string)
	Connected(remoteAddr string)
	Handle9P(ctx context.Context, req Message, w Replier)
}

/////////////////////////////////////////////////////////////

const (
	DefaultInitialTimeout = 1 * time.Second
	DefaultReadTimeout    = 5 * time.Second //5 * time.Minute
	DefaultWriteTimeout   = 15 * time.Second

	// max number of requests a session can make
	// needs to be balanced with max number of active connections
	DefaultMaxInflightRequestsPerSession = 30
)

type Server struct {
	Loggable
	Handler Handler

	InitialTimeout                time.Duration // timeout initial 9P handshake (version exchange)
	ReadTimeout                   time.Duration // timeout reading data from clients
	WriteTimeout                  time.Duration // timeout writing data to clients
	MaxInflightRequestsPerSession int

	TLSConfig *tls.Config

	MaxMsgSize uint32
}

// Provides an easy way to create a server (you can still construct the Server
// struct manually if you want).
func NewServer(fs FileSystem, errLogger, traceLogger Logger) *Server {
	loggable := Loggable{
		ErrorLog: errLogger,
		TraceLog: traceLogger,
	}
	return &Server{
		Handler: &DefaultHandler{
			Fs:       fs,
			Loggable: loggable,
		},
		Loggable: loggable,
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
	s.Tracef("listening on %s", l.Addr())

	if s.InitialTimeout == 0 {
		s.InitialTimeout = DefaultInitialTimeout
	}
	if s.ReadTimeout == 0 {
		s.ReadTimeout = DefaultReadTimeout
	}
	if s.WriteTimeout == 0 {
		s.WriteTimeout = DefaultWriteTimeout
	}
	if s.MaxInflightRequestsPerSession == 0 {
		s.MaxInflightRequestsPerSession = DefaultMaxInflightRequestsPerSession
	}

	retries := 0
	const maxWait = 1 * time.Minute
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	for {
		conn, err := l.Accept()
		if err != nil {
			if IsTemporaryErr(err) {
				retries++
				wait := time.Duration(math.Min(math.Pow(float64(10*time.Millisecond), float64(retries)), float64(maxWait)))
				s.Tracef("accept error: %s; retrying in %v", err, wait)
				time.Sleep(wait)
				continue
			} else {
				return err
			}
		}

		s.Tracef("accepted connection from %s", conn.RemoteAddr())
		sc := &serverConn{
			rwc:        conn,
			maxMsgSize: DEFAULT_MAX_MESSAGE_SIZE,
			ctx:        ctx,
			srv:        s,
			handler:    s.Handler,
		}
		go sc.serve()
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

type serverConn struct {
	rwc net.Conn

	srv  *Server
	txns chan *transaction

	mut      sync.Mutex
	liveTags map[Tag]context.CancelFunc

	handler Handler
	ctx     context.Context

	maxMsgSize uint32
	stop       int32
}

func (s *serverConn) tracef(f string, values ...interface{}) {
	if s.srv.TraceLog != nil {
		s.srv.TraceLog.Printf(f, values...)
	}
}

func (s *serverConn) errorf(f string, values ...interface{}) {
	if s.srv.ErrorLog != nil {
		s.srv.ErrorLog.Printf(f, values...)
	}
}

func (s *serverConn) prepareDeadlines() {
	now := time.Now()
	s.rwc.SetReadDeadline(now.Add(s.srv.ReadTimeout))
	s.rwc.SetWriteDeadline(now.Add(s.srv.WriteTimeout))
}

func (s *serverConn) acceptTversion(txn *transaction) bool {
	preferredSize := s.maxMsgSize
	version := VERSION_9P

	now := time.Now()
	s.rwc.SetReadDeadline(now.Add(s.srv.InitialTimeout))
	s.rwc.SetWriteDeadline(now.Add(s.srv.InitialTimeout))
	for {
		err := txn.readRequest(s.rwc)
		if err != nil {
			s.errorf("failed to negotiate version: error when reading: %s", err)
			return false
		}

		var request Tversion
		{
			var ok bool
			request, ok = txn.Request().(Tversion)
			if !ok {
				s.errorf("failed to negotiate version: unexpected message type: %d", txn.requestType())
				txn.Rerrorf("unknown")
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
		if !strings.HasPrefix(request.Version(), VERSION_9P2000) {
			txn.Rversion(size, "unknown")
			s.tracef("negotiate version: unrecognized protocol version: got %#v, wanted %#v", request.Version(), version)
		} else {
			txn.Rversion(size, version)
			ok = true
		}

		err = txn.writeReply(s.rwc)
		if err != nil {
			s.errorf("failed to negotiate version: %s", err)
			return false
		}

		if ok {
			return true
		}
	}
}

func (s *serverConn) requestStop() {
	atomic.AddInt32(&s.stop, 1)
}

func (s *serverConn) assocTag(t Tag, c context.CancelFunc) bool {
	s.mut.Lock()
	_, found := s.liveTags[t]
	if !found {
		s.liveTags[t] = c
	}
	s.mut.Unlock()
	// return !found
	return true
}

func (s *serverConn) dissocTag(t Tag) {
	s.mut.Lock()
	cancel, ok := s.liveTags[t]
	delete(s.liveTags, t)
	s.mut.Unlock()
	if ok {
		cancel()
	}
}

// this runs in a new goroutine
func (s *serverConn) serve() {
	defer s.rwc.Close()

	// here's our pool of transactions we can write
	{
		max := s.srv.MaxInflightRequestsPerSession
		if max <= 0 {
			panic(fmt.Errorf("MaxInflightRequestsPerSession must be positive, got: %d", max))
		}
		s.liveTags = make(map[Tag]context.CancelFunc)
		s.txns = make(chan *transaction, max)
		go func() {
			for i := 0; i < max; i++ {
				t := createTransaction(s.maxMsgSize)
				s.txns <- &t
			}
		}()
	}

	{
		verTxn := createTransaction(s.maxMsgSize)
		if !s.acceptTversion(&verTxn) {
			return
		}
	}

	remoteAddr := s.rwc.RemoteAddr().String()
	ctx, cancel := context.WithCancel(s.ctx)
	var err error
	{
		s.handler.Connected(remoteAddr)

		for s.stop = 0; s.stop == 0; {
			txn := <-s.txns
			txn.remoteAddr = remoteAddr
			s.prepareDeadlines()
			err = txn.readRequest(s.rwc)
			if err != nil {
				txn.reset()
				s.txns <- txn
				break
			}

			select {
			case <-ctx.Done():
				s.tracef("closing connection, erroring request from %s", s.rwc.RemoteAddr())
				txn.Rerrorf("closing connection")
				txn.writeReply(s.rwc) // we don't care, we're going away
				txn.reset()
				s.txns <- txn
				break
			default:
				// NOTE: technically, we never verify that Tags are unique, but
				// we're going to write that off as a bug in the client. We'll happily,
				// reply with the same Tag and the client will be confused which
				// response refers to which request they asked.
				//
				// NOTE: according to golang docs, we're safe to have multiple
				// goroutines write to the same net.Conn
				go s.dispatch(ctx, txn)
			}
		}
	}
	cancel()

	s.tracef("closing connection from %s: %s", remoteAddr, err)
	s.handler.Disconnected(remoteAddr)
}

func (s *serverConn) dispatch(ctx context.Context, txn *transaction) {
	req := txn.Request()
	tag := req.Tag()
	ctx, cancel := context.WithCancel(ctx)
	ok := s.assocTag(tag, cancel)

	if !ok {
		defer cancel()
	}

	shouldStop := false

	// dispatch
	if ok {
		switch m := req.(type) {
		case MsgBase:
			s.errorf("Unknown message of type: %d", MsgBase(req.Bytes()).Type())
			txn.Rerrorf("unknown msg")

		case Tflush:
			s.tracef("Cancel request tag %d", tag)
			oldTag := m.OldTag()
			defer s.dissocTag(oldTag)
			txn.Rflush()

		default:
			s.handler.Handle9P(ctx, m, txn)
			if !txn.handled {
				txn.Rerrorf("not implemented")
			}
			shouldStop = txn.disconnect
		}
	} else {
		s.errorf("tag already in use: %d :: %#v", tag, s.liveTags)
		txn.Rerrorf("Tag already in use: %d", tag)
	}

	// handle dispatch result
	select {
	case <-ctx.Done(): // we can't dissocTag above this otherwise this branch will always resolve
		txn.Rerrorf("canceling message")
		s.dissocTag(req.Tag())

	default:
		// since we're on multiple threads, we want to remove the tag ASAP, but
		// ctx.Done() above will always be followed.
		s.dissocTag(req.Tag())
		if txn.handled {
			s.prepareDeadlines()
			err := txn.writeReply(s.rwc)
			if err != nil {
				s.errorf("failed to write message: %s", err)
				shouldStop = true
			}
		} else if !txn.handled {
			s.errorf("failed to handle message: %#v", req)
		}
	}

	if shouldStop {
		s.requestStop()
	}

	if e, ok := txn.Reply().(Rerror); ok {
		s.errorf("return error: %s", e.Ename())
	} else {
		s.tracef("return %s", txn.requestType())
	}

	txn.reset()
	s.txns <- txn // return back to pool
}

///////////////////////////////////////////////////////

type File struct {
	Name     string
	User     string
	Flag     OpenMode
	Mode     Mode
	H        FileHandle
	RefCount int32
}

func (f *File) IncRef() {
	atomic.AddInt32(&f.RefCount, 1)
}

func (f *File) DecRef() bool {
	return atomic.AddInt32(&f.RefCount, -1) == 0
}

type Session struct {
	fids FidTracker
	qids *QidPool

	m            sync.Mutex
	qidsToHandle map[uint64]FileHandle
}

func (s *Session) FileForFid(f Fid) (fil File, found bool) {
	fil, ok := s.fids.Get(f)
	return fil, ok
}
func (s *Session) DeleteFid(f Fid)          { s.fids.Delete(f) }
func (s *Session) PutFid(f Fid, h File) Fid { return s.fids.Put(f, h) }
func (s *Session) PutQid(name string, t QidType, version uint32) Qid {
	return s.qids.Put(name, t, version)
}
func (s *Session) TouchQid(name string, t QidType) Qid {
	return s.qids.Touch(name, t, 1)
}
func (s *Session) PutFileHandle(q Qid, h FileHandle) { s.qidsToHandle[q.Path()] = h }
func (s *Session) Qid(name string) (Qid, bool)       { return s.qids.Get(name) }
func (s *Session) FileHandle(q Qid) (FileHandle, bool) {
	s.m.Lock()
	h, ok := s.qidsToHandle[q.Path()]
	s.m.Unlock()
	return h, ok
}

func (s *Session) DeleteQid(name string) {
	s.qids.Delete(name)
}

func (s *Session) DeleteFileHandle(q Qid) {
	s.m.Lock()
	delete(s.qidsToHandle, q.Path())
	s.m.Unlock()
}

func (s *Session) Close() {
	s.m.Lock()
	for _, h := range s.qidsToHandle {
		h.Close()
	}
	s.m.Unlock()
}

type SessionTracker struct {
	m    sync.Mutex
	sess map[string]*Session

	qids *QidPool
}

func (st *SessionTracker) unsafeInit() {
	if st.sess == nil {
		st.sess = make(map[string]*Session)
		st.qids = &QidPool{pool: make(map[string]Qid)}
	}
}

func (st *SessionTracker) Add(addr string) *Session {
	st.m.Lock()
	st.unsafeInit()
	s, ok := st.sess[addr]
	if !ok {
		s = &Session{
			fids:         FidTracker{fids: make(map[Fid]File)},
			qids:         st.qids,
			qidsToHandle: make(map[uint64]FileHandle),
		}
		st.sess[addr] = s
	}
	st.m.Unlock()
	return s
}

func (st *SessionTracker) Lookup(addr string) *Session {
	st.m.Lock()
	st.unsafeInit()
	s, ok := st.sess[addr]
	if !ok {
		s = nil
	}
	st.m.Unlock()
	return s
}

func (st *SessionTracker) Remove(addr string) {
	st.m.Lock()
	st.unsafeInit()
	s, ok := st.sess[addr]
	if ok {
		delete(st.sess, addr)
	}
	st.m.Unlock()
	if ok {
		s.Close()
	}
}

///////////////////////////////////////////////////////

type FidTracker struct {
	m    sync.Mutex
	fids map[Fid]File
}

func (t *FidTracker) Get(f Fid) (h File, found bool) {
	t.m.Lock()
	h, found = t.fids[f]
	t.m.Unlock()
	return
}

func (t *FidTracker) Put(f Fid, h File) Fid {
	t.m.Lock()
	t.fids[f] = h
	t.m.Unlock()
	return f
}

func (t *FidTracker) Delete(f Fid) {
	t.m.Lock()
	delete(t.fids, f)
	t.m.Unlock()
}

func (t *FidTracker) Clear() {
	t.m.Lock()
	for fid := range t.fids {
		delete(t.fids, fid)
	}
	t.m.Unlock()
}

///////////////////////////////////////////////////////

type QidPool struct {
	m        sync.Mutex
	pool     map[string]Qid
	nextPath uint64
}

func (p *QidPool) Get(name string) (q Qid, found bool) {
	p.m.Lock()
	q, found = p.pool[name]
	p.m.Unlock()
	return
}

func (p *QidPool) Touch(name string, t QidType, verDelta uint32) Qid {
	var qid Qid
	p.m.Lock()
	if existing, ok := p.pool[name]; ok {
		qid = existing
		if verDelta != NoQidVersion {
			qid.SetVersion(qid.Version() + verDelta)
			p.pool[name] = qid
		}
	} else {
		qid = NewQid().Fill(t, 0, p.nextPath)
		p.nextPath++
		p.pool[name] = qid
	}
	p.m.Unlock()
	return qid
}

func (p *QidPool) Put(name string, t QidType, version uint32) Qid {
	var qid Qid
	p.m.Lock()
	if existing, ok := p.pool[name]; ok {
		qid = existing
		if version != NoQidVersion {
			qid.SetVersion(version)
			p.pool[name] = qid
		}
	} else {
		if version == NoQidVersion {
			version = 0
		}
		qid = NewQid().Fill(t, version, p.nextPath)
		p.nextPath++
		p.pool[name] = qid
	}
	p.m.Unlock()
	return qid
}

func (p *QidPool) Delete(name string) {
	p.m.Lock()
	delete(p.pool, name)
	p.m.Unlock()
}

func (p *QidPool) Clear() {
	p.m.Lock()
	for k := range p.pool {
		delete(p.pool, k)
	}
	p.m.Unlock()
}
