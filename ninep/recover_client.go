package ninep

import (
	"context"
	"crypto/tls"
	"sync"
	"sync/atomic"
)

type fidState struct {
	m         sync.Mutex
	serverFid Fid
	path      []string
	flag      OpenMode
	mode      Mode
}

// A 9P client that supports low-level operations and higher-level functionality
type RecoverClient struct {
	BasicClient
	User, Mount string

	nextClientFid uint32
	nextServerFid uint32

	m          sync.Mutex
	fids       map[Fid]fidState // clientFid -> serverFid
	serverFids []Fid            // allocated server fids
	closing    bool
}

var _ FileSystemProxyClient = (*RecoverClient)(nil)

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
		for _, f := range c.serverFids {
			if f == fid {
				continue try
			}
		}
		return fid
	}
}

var _ clientSocketStrategy = (*RecoverClient)(nil)

func (_ *RecoverClient) WriteRequest(c *BasicClient, t *cltRequest) error {
	return t.writeRequest(c.rwc)
}

func (_ *RecoverClient) ReadLoop(ctx context.Context, c *BasicClient) {
	for {
		select {
		case <-ctx.Done():
			c.abortTransactions(ctx.Err())
			return
		case res := <-c.responsePool:
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

// Returns an interface that conforms to the file system interface
// Can be used once Connect*() are called and successful
//
// Uses User and Mount fields in RecoverClient
func (c *RecoverClient) Fs() (*FileSystemProxy, error) {
	afid := NO_FID
	f := Fid(1)
	if c.Authorizee != nil {
		afid = Fid(0)
		_, err := c.Auth(afid)
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
	root, err := c.Attach(f, afid)
	if err != nil {
		return nil, err
	}
	return &FileSystemProxy{c: c, rootF: f, rootQ: root}, nil
}

func (c *RecoverClient) init() {
	c.BasicClient.strategy = c
	c.BasicClient.MinMsgSize = c.BasicClient.MaxMsgSize
	c.fids = make(map[Fid]fidState)
	c.closing = false
}

func (c *RecoverClient) ConnectTLS(addr string, tlsCfg *tls.Config) error {
	c.init()
	return c.BasicClient.ConnectTLS(addr, tlsCfg)
}

func (c *RecoverClient) Connect(addr string) error {
	c.init()
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

func (c *RecoverClient) Attach(fid, afid Fid) (Qid, error) {
	var srvAfid Fid

	c.m.Lock()
	srvFid, ok := c.unsafeCreateFidTranslation(fid)
	if !ok {
		c.m.Unlock()
		return nil, ErrFidExists
	}
	if afid == NO_FID {
		srvAfid = NO_FID
	} else {
		srvAfid, ok = c.unsafeTranslateFid(afid)
		if !ok {
			c.m.Unlock()
			return nil, ErrUnrecognizedFid
		}
	}
	c.m.Unlock()

	qid, err := c.BasicClient.Attach(srvFid, srvAfid, c.User, c.Mount)
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
			if len(qids) > 0 {
				qid := qids[len(qids)-1]
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
	if state.mode&M_EXCL != 0 {
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
	q, iounit, err = c.BasicClient.Open(srvF, m)
	if err == nil {
		// update fid state
		c.m.Lock()
		state := c.fids[f]
		state.flag = m
		c.fids[f] = state
		c.m.Unlock()
	}
	return
}

func (c *RecoverClient) Create(f Fid, name string, perm Mode, mode OpenMode) (q Qid, iounit uint32, err error) {
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
