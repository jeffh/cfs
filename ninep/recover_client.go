package ninep

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

const (
	defaultReadTimeout  time.Duration = 2 * time.Second
	defaultWriteTimeout time.Duration = 2 * time.Second

	// fids reserved by recover client
	recoverAfid   Fid = 0
	recoverMntFid Fid = 1
)

type fidState struct {
	m         sync.Mutex
	serverFid Fid
	path      []string
	qtype     QidType
	flag      OpenMode
	mode      Mode
	opened    bool
	// TODO: someday: we should remember dir offsets for reconnects
}

// A 9P client that supports low-level operations and higher-level functionality
type RecoverClient struct {
	BasicClient
	User, Mount string

	ReadTimeout  time.Duration // how long to wait for reads before attempting to reconnect
	WriteTimeout time.Duration // how long to wait for writes before attempting to reconnect

	nextClientFid uint32
	nextServerFid uint32

	m          sync.Mutex
	fids       map[Fid]fidState // clientFid -> serverFid
	serverFids []Fid            // allocated server fids
	addr       string
	tlsCfg     *tls.Config
	usesAuth   bool
	connectTLS bool
	closing    bool
}

var _ FileSystemProxyClient = (*RecoverClient)(nil)

func (c *RecoverClient) readTimeout() time.Duration {
	if c.ReadTimeout == 0 {
		return defaultReadTimeout
	}
	return c.ReadTimeout
}

func (c *RecoverClient) writeTimeout() time.Duration {
	if c.WriteTimeout == 0 {
		return defaultWriteTimeout
	}
	return c.WriteTimeout
}

func (c *RecoverClient) translateFid(client Fid) (server Fid, err error) {
	var ok bool
	c.m.Lock()
	server, ok = c.unsafeTranslateFid(client)
	c.m.Unlock()
	if !ok {
		err = ErrUnrecognizedFid
	}
	return
}

func (c *RecoverClient) unsafeDiscardFid(client Fid) {
	delete(c.fids, client)
}

func (c *RecoverClient) discardFid(client Fid) {
	c.m.Lock()
	delete(c.fids, client)
	c.m.Unlock()
}

func (c *RecoverClient) unsafeTranslateFid(client Fid) (server Fid, found bool) {
	var state fidState
	state, found = c.fids[client]
	if found {
		server = state.serverFid
	}
	return
}

func (c *RecoverClient) unsafeCreateFidTranslation(client Fid) (server Fid, ok bool) {
	_, found := c.fids[client]
	if !found {
		server = c.unsafeAllocServerFid()
		c.fids[client] = fidState{
			serverFid: server,
		}
		ok = true
	}
	return
}

func (c *RecoverClient) unsafeAllocClientFid() Fid {
	for { // TODO: abandon after a certain amount of time
		fid := Fid(atomic.AddUint32(&c.nextClientFid, 1))
		_, ok := c.fids[fid]
		if ok {
			return fid
		}
	}
}
func (c *RecoverClient) unsafeAllocServerFid() Fid {
try:
	for { // TODO: abandon after a certain amount of time
		fid := Fid(atomic.AddUint32(&c.nextClientFid, 1))
		// 0 and 1 are reserved (0 = auth, 1 = mount)
		if fid == recoverAfid || fid == recoverMntFid {
			continue try
		}
		for _, f := range c.serverFids {
			if f == fid {
				continue try
			}
		}
		return fid
	}
}

func (c *RecoverClient) retryConnection() error {
	const (
		numRetries = 3
	)

	c.m.Lock()
	defer c.m.Unlock()

	// TODO: manage timeouts

	bc := &c.BasicClient
	bc.rwc.SetDeadline(time.Time{})
	if err := bc.Close(); err != nil {
		c.Errorf("retryConnection.close: %s\n", err)
	}
	var err error
	for i := 0; i < numRetries; i++ {
		c.Tracef("retry connection (attempt=%d)", i)
		// TODO: we need to timeout connect attempts as well
		if c.connectTLS {
			err = bc.ConnectTLS(c.addr, c.tlsCfg)
		} else {
			err = bc.Connect(c.addr)
		}
		if err == nil {
			break
		}
	}
	if err != nil {
		return err
	}

	afid := NO_FID
	mtfid := Fid(1)
	if c.usesAuth {
		afid = 0
		// TODO: we should retry auth attempts
		for i := 0; i < numRetries; i++ {
			_, err = bc.Auth(afid, c.User, c.Mount)
			if IsTimeoutErr(err) || IsTemporaryErr(err) {
				continue // retry
			}
			if err != nil {
				return err
			}
		}
	}
	if err != nil {
		return err
	}
	for i := 0; i < numRetries; i++ {
		_, err = bc.Attach(mtfid, afid, c.User, c.Mount)
		if err == nil {
			break
		}
		fmt.Printf("Attach error: %s\n")
	}
	if err != nil {
		return err
	}

	// since we're a new 9p session, "push" our internal state to the remote
	// server again

	for _, state := range c.fids {
		state.m.Lock()
		if state.serverFid != recoverAfid && state.serverFid != recoverMntFid {
			for i := 0; i < numRetries; i++ {
				_, err = bc.Walk(recoverMntFid, state.serverFid, state.path)
				if err != nil {
					bc.Clunk(state.serverFid) // just because it's easier right now
					continue                  // retry
				}
				if state.opened {
					_, _, err = bc.Open(state.serverFid, state.flag)
					if err != nil {
						e := bc.Clunk(state.serverFid) // just because it's easier right now
						if e != nil {
							return err
						}
						continue // retry
					}
				}
			}
		}
		state.m.Unlock()

		if err != nil {
			// failed
			return err
		}
	}

	return err
}

var _ clientSocketStrategy = (*RecoverClient)(nil)

func (rc *RecoverClient) WriteRequest(c *BasicClient, t *cltRequest) error {
	const numTries = 3
	tries := 0
	for {
		now := time.Now()
		c.rwc.SetWriteDeadline(now.Add(rc.writeTimeout()))
		err := t.writeRequest(c.rwc)
		if err == nil {
			res := <-c.responsePool
			if res == nil {
				panic(fmt.Errorf("res is nil: %v\n", res))
			}
			c.pendingResponses <- res
		}

		if e, ok := err.(net.Error); ok {
			if e.Temporary() {
				continue
			}
			if e.Timeout() || isRetryable(e) {
				if tries < numTries {
					tries++
				} else {
					retryErr := rc.retryConnection()
					if retryErr != nil {
						return err
					}
					tries = 0
				}
				continue
			}
			return err
		} else if isRetryable(e) {
			if tries < numTries {
				tries++
			} else {
				retryErr := rc.retryConnection()
				if retryErr != nil {
					return err
				}
				tries = 0
			}
			continue
		} else {
			return err
		}
	}
}

func (rc *RecoverClient) ReadLoop(ctx context.Context, c *BasicClient) {
	retry := make(chan bool, c.MaxSimultaneousRequests)
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case _, ok := <-retry:
				if !ok {
					return
				}
				for {
					select {
					case <-retry:
						continue
					default:
						break
					}
				}
				retryErr := rc.retryConnection()
				if retryErr != nil {
					c.Errorf("Error read timeout: %s", retryErr)
					rc.Close()
					return
				}
			}
		}
	}()

readLoop:
	for {
		select {
		case <-ctx.Done():
			c.abortTransactions(ctx.Err())
			return
		case res, ok := <-c.pendingResponses:
			if !ok {
				// I guess we're done reading....
				c.Errorf("pending read queue closed")
				return
			}
			res.reset()
		readAttempt:
			for {
				c.rwc.SetReadDeadline(time.Now().Add(rc.readTimeout()))
				err := res.readReply(c.rwc)

				if isClosedErr(err) || err == io.EOF {
					c.abortTransactions(err)
					return
				}
				if IsTimeoutErr(err) {
					retry <- true
					continue readAttempt
				}
				if IsTemporaryErr(err) {
					continue readAttempt
				}
				if err != nil {
					c.Errorf("Error reading from server: %s", err)
					c.abortTransactions(err)
					return
				} else {
					break readAttempt
				}
			}

			txn, ok := c.getTransaction(res.reqTag())
			if !ok {
				c.Errorf("Server returned unrecognized tag: %d", res.reqTag())
				continue readLoop
			}
			c.Tracef("Server tag: %d", res.Reply().Tag())
			txn.res = res
			c.putTransaction(res.reqTag(), txn)
			txn.ch <- cltChResponse{res: res}
			close(txn.ch)
		}
	}
}

// Returns an interface that conforms to the file system interface
// Can be used once Connect*() are called and successful
//
// Uses User and Mount fields in RecoverClient
func (c *RecoverClient) Fs() (*FileSystemProxy, error) {
	c.m.Lock()
	cltF := c.unsafeAllocClientFid()
	c.m.Unlock()
	afid := NO_FID
	f := recoverMntFid
	if c.Authorizee != nil {
		afid = recoverAfid
		_, err := c.BasicClient.Auth(afid, c.User, c.Mount)
		if err == nil {
			err = c.Authorizee.Prove(context.Background(), c.User, c.Mount)
			if err != nil {
				c.Errorf("Failed to authorize, because of bad credentials: %s", err)
				return nil, err
			}
		} else {
			c.Errorf("Failed to authorize, continuing: %s", err)
			err = nil
		}
	}
	root, err := c.BasicClient.Attach(f, afid, c.User, c.Mount)
	if err != nil {
		return nil, err
	} else {
		mode := Mode(0)
		if root.Type().IsDir() {
			mode |= M_DIR
		}
		c.fids[cltF] = fidState{
			serverFid: f,
			qtype:     root.Type(),
			path:      []string{""},
			mode:      mode,
		}
	}
	return &FileSystemProxy{c: c, rootF: cltF, rootQ: root}, nil
}

func (c *RecoverClient) init(usingTLS bool, addr string, tlscfg *tls.Config) {
	c.m.Lock()
	c.BasicClient.strategy = c
	c.BasicClient.MinMsgSize = c.BasicClient.MaxMsgSize
	c.fids = make(map[Fid]fidState)
	c.fids[recoverMntFid] = fidState{}
	c.closing = false
	c.connectTLS = usingTLS
	c.addr = addr
	c.tlsCfg = tlscfg
	c.m.Unlock()
}

func (c *RecoverClient) ConnectTLS(addr string, tlsCfg *tls.Config) error {
	c.init(true, addr, tlsCfg)
	return c.BasicClient.ConnectTLS(addr, tlsCfg)
}

func (c *RecoverClient) Connect(addr string) error {
	c.init(false, addr, nil)
	return c.BasicClient.Connect(addr)
}

func (c *RecoverClient) Close() error {
	c.m.Lock()
	ok := !c.closing
	c.closing = true
	c.m.Unlock()
	if ok {
		return c.BasicClient.Close()
	}
	return nil
}

func (c *RecoverClient) Auth(afid Fid) (Qid, error) {
	c.m.Lock()
	srvAfid, ok := c.unsafeCreateFidTranslation(afid)
	c.m.Unlock()

	if !ok {
		return nil, ErrFidExists
	}

	qid, err := c.BasicClient.Auth(srvAfid, c.User, c.Mount)
	// record fid state
	if err == nil {
		c.m.Lock()
		if !c.closing {
			c.fids[afid] = fidState{
				serverFid: srvAfid,
				qtype:     qid.Type(),
				path:      []string{""},
				mode:      M_AUTH,
			}
		}
		c.m.Unlock()
	} else {
		c.discardFid(afid)
	}
	return qid, err
}

// func (c *RecoverClient) Attach(fid, afid Fid) (Qid, error) {
func (c *RecoverClient) Attach(fid Fid) (Qid, error) {
	c.m.Lock()
	srvFid, ok := c.unsafeCreateFidTranslation(fid)
	c.m.Unlock()
	if !ok {
		return nil, ErrFidExists
	}

	qids, err := c.BasicClient.Walk(recoverMntFid, srvFid, []string{})
	qid := qids[len(qids)-1]
	// record fid state
	if err == nil {
		c.m.Lock()
		mode := Mode(0)
		if qid.Type().IsDir() {
			mode |= M_DIR
		}
		if !c.closing {
			c.fids[fid] = fidState{
				serverFid: srvFid,
				qtype:     qid.Type(),
				path:      []string{""},
				mode:      mode,
			}
		}
		c.m.Unlock()
	}
	return qid, err
}

func (c *RecoverClient) Walk(f, newF Fid, path []string) ([]Qid, error) {
	var srvNewF Fid

	c.m.Lock()
	srvF, ok := c.unsafeTranslateFid(f)
	if !ok {
		c.m.Unlock()
		return nil, ErrUnrecognizedFid
	}
	// either reuse existing fid or create a new one
	if f != newF {
		srvNewF = c.unsafeAllocServerFid()
		if !ok {
			c.m.Unlock()
			return nil, ErrFidExists
		}
	} else {
		srvNewF = srvF
	}
	c.m.Unlock()

	qids, err := c.BasicClient.Walk(srvF, srvNewF, path)
	if err == nil {
		c.m.Lock()
		if !c.closing {
			mode := Mode(0)
			state := c.fids[f]
			var qt QidType
			if len(qids) > 0 {
				qid := qids[len(qids)-1]
				qt = qid.Type()
				if qid.Type().IsDir() {
					mode |= M_DIR
				}
			} else {
				mode = state.mode
			}
			finalPath := make([]string, len(state.path)+len(path))
			copy(finalPath, state.path)
			copy(finalPath[len(state.path):], path)
			c.fids[newF] = fidState{
				serverFid: srvNewF,
				qtype:     qt,
				path:      finalPath,
				mode:      mode,
			}
		}
		c.m.Unlock()
	} else if f != newF {
		// if we errored, forget the new fid, if we allocated it
		c.discardFid(newF)
	}
	return qids, err
}

func (c *RecoverClient) Stat(f Fid) (Stat, error) {
	srvF, err := c.translateFid(f)
	if err != nil {
		return Stat{}, err
	}
	return c.BasicClient.Stat(srvF)
}

func (c *RecoverClient) WriteStat(f Fid, s Stat) error {
	srvF, err := c.translateFid(f)
	if err != nil {
		return err
	}
	return c.BasicClient.WriteStat(srvF, s)
}

func (c *RecoverClient) Read(f Fid, p []byte, offset uint64) (int, error) {
	srvF, err := c.translateFid(f)
	if err != nil {
		return 0, err
	}
	return c.BasicClient.Read(srvF, p, offset)
}

func (c *RecoverClient) Clunk(f Fid) error {
	c.m.Lock()
	state, ok := c.fids[f]
	if !ok {
		c.m.Unlock()
		return nil
	}
	c.m.Unlock()

	// we need to honor delete-on-close behavior
	var err error
	if state.flag&ORCLOSE != 0 {
		// As per spec: Remove implies Clunk
		err = c.BasicClient.Remove(state.serverFid)
	} else {
		err = c.BasicClient.Clunk(state.serverFid)
	}
	c.discardFid(f)
	return err
}

func (c *RecoverClient) Remove(f Fid) error {
	srvF, err := c.translateFid(f)
	if err != nil {
		return err
	}
	return c.BasicClient.Remove(srvF)
}

// Like WriteMsg, but conforms to golang's io.Writer interface (max num bytes possible, else error)
func (c *RecoverClient) Write(f Fid, data []byte, offset uint64) (int, error) {
	srvF, err := c.translateFid(f)
	if err != nil {
		return 0, err
	}
	return c.BasicClient.Write(srvF, data, offset)
}

// The 9p protocol-level write. Will only write as large as negotiated message buffers allow
func (c *RecoverClient) WriteMsg(f Fid, data []byte, offset uint64) (uint32, error) {
	srvF, err := c.translateFid(f)
	if err != nil {
		return 0, err
	}
	return c.BasicClient.WriteMsg(srvF, data, offset)
}

func (c *RecoverClient) Open(f Fid, m OpenMode) (q Qid, iounit uint32, err error) {
	srvF, err := c.translateFid(f)
	if err != nil {
		return nil, 0, err
	}
	// no efficient to fetch again
	c.m.Lock()
	state := c.fids[f]
	c.m.Unlock()

	// TODO: it would be nice to consider how to support exclusive files
	if state.qtype&QT_EXCL != 0 {
		return nil, 0, ErrUnsupported
	}

	q, iounit, err = c.BasicClient.Open(srvF, m)
	if err == nil {
		// update fid state
		c.m.Lock()
		state := c.fids[f]
		state.flag = m
		state.qtype = q.Type()
		c.fids[f] = state
		c.m.Unlock()
	}
	return
}

func (c *RecoverClient) Create(f Fid, name string, perm Mode, mode OpenMode) (q Qid, iounit uint32, err error) {
	// TODO: it would be nice to consider how to support exclusive files
	if perm&M_EXCL != 0 {
		return nil, 0, ErrUnsupported
	}

	srvF, err := c.translateFid(f)
	if err != nil {
		return nil, 0, err
	}

	q, iounit, err = c.BasicClient.Create(srvF, name, perm, mode)
	if err == nil {
		// update fid state
		c.m.Lock()
		state := c.fids[f]
		state.path = append(state.path, name)
		state.flag = mode
		state.mode = perm
		c.fids[f] = state
		c.m.Unlock()
	}
	return
}
