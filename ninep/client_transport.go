package ninep

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

type ClientTransport interface {
	Connect(d Dialer, network, addr, usr, mnt string, A Authorizee, L *slog.Logger) error
	Disconnect() error

	MaxMessageSize() uint32

	// TODO: RENAME: this is allocating a transaction for preparing a request (not response)
	// Assumes .reset() was called by implementor if reused
	AllocTransaction() (*cltTransaction, bool)
	ReleaseTransaction(t Tag)
	Request(txn *cltTransaction) (Message, error) // this fills in a txn's response
}

////////////////////////////////////////////////////////////////////////////////////////////
// Serial Client Transport
// non-concurrent socket read & write, no retry on failures

type SerialClientTransport struct {
	m          sync.Mutex
	rwc        net.Conn
	txn        cltTransaction
	maxMsgSize uint32
}

var _ ClientTransport = (*SerialClientTransport)(nil)

func (t *SerialClientTransport) MaxMessageSize() uint32 {
	t.m.Lock()
	defer t.m.Unlock()
	return t.maxMsgSize
}

func (t *SerialClientTransport) Connect(d Dialer, network, addr, usr, mnt string, A Authorizee, L *slog.Logger) error {
	t.m.Lock()
	defer t.m.Unlock()
	var err error
	if network == "" {
		network = "tcp"
	}
	t.rwc, err = d.Dial(network, addr)
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

func (t *SerialClientTransport) Disconnect() error { return t.rwc.Close() }
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
// Parallel Client Transport
// concurrent socket read & write, no retry on failures

type ParallelClientTransport struct {
	mut       sync.Mutex
	tagToTxns map[Tag]cltTransaction

	m                       sync.Mutex
	rwc                     net.Conn
	requestPool             chan *cltRequest
	responsePool            chan *cltResponse
	pendingResponses        chan *cltResponse
	readCancel              context.CancelFunc
	maxMsgSize              uint32
	MaxSimultaneousRequests uint
	Logger                  *slog.Logger
}

var _ ClientTransport = (*ParallelClientTransport)(nil)

func (t *ParallelClientTransport) MaxMessageSize() uint32 { return t.maxMsgSize }

func (t *ParallelClientTransport) Connect(d Dialer, network, addr, usr, mnt string, A Authorizee, L *slog.Logger) error {
	t.m.Lock()
	defer t.m.Unlock()

	t.Logger = L

	var err error
	t.rwc, err = d.Dial(network, addr)
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
	if t.MaxSimultaneousRequests == 0 {
		t.MaxSimultaneousRequests = 1
	}

	t.requestPool = make(chan *cltRequest, t.MaxSimultaneousRequests)
	t.responsePool = make(chan *cltResponse, t.MaxSimultaneousRequests)
	t.pendingResponses = make(chan *cltResponse, t.MaxSimultaneousRequests)

	go func() {
		for i := uint(0); i < t.MaxSimultaneousRequests; i++ {
			r := createClientRequest(Tag(i), t.maxMsgSize)
			t.requestPool <- &r
		}
		for i := uint(0); i < t.MaxSimultaneousRequests; i++ {
			r := createClientResponse(t.maxMsgSize)
			t.responsePool <- &r
		}
	}()

	ctx, cancel := context.WithCancel(context.Background())
	t.readCancel = cancel
	go t.readLoop(ctx)

	return nil
}

func (t *ParallelClientTransport) abortTransactions(err error) {
	t.mut.Lock()
	for _, txn := range t.tagToTxns {
		select {
		case txn.ch <- cltChResponse{err: err}:
		case <-time.After(1 * time.Second):
		}
		close(txn.ch)
	}
	t.tagToTxns = make(map[Tag]cltTransaction)
	t.mut.Unlock()
	return
}

func (c *ParallelClientTransport) getTransaction(t Tag) (cltTransaction, bool) {
	c.mut.Lock()
	txn, ok := c.tagToTxns[t]
	c.mut.Unlock()
	return txn, ok
}

func (c *ParallelClientTransport) putTransaction(t Tag, txn cltTransaction) {
	c.mut.Lock()
	c.tagToTxns[t] = txn
	c.mut.Unlock()
}

func (t *ParallelClientTransport) readLoop(ctx context.Context) {
	t.m.Lock()
	pendingResponses := t.pendingResponses
	t.m.Unlock()
	for {
		select {
		case <-ctx.Done():
			t.abortTransactions(ctx.Err())
			return
		case res, ok := <-pendingResponses:
			if !ok {
				t.m.Lock()
				pendingResponses = t.pendingResponses
				t.m.Unlock()
				continue
			}
			res.reset()
			err := res.readReply(t.rwc)
			if err != nil {
				if t.Logger != nil {
					t.Logger.Error("ParallelClientTransport.readLoop.failed", slog.Any("error", err))
				}
				t.abortTransactions(err)
				return
			}

			txn, ok := t.getTransaction(res.reqTag())
			if !ok {
				if t.Logger != nil {
					t.Logger.Error("ParallelClientTransport.readLoop.failed.unrecognizedTag", slog.Uint64("tag", uint64(res.reqTag())))
				}
				continue
			}
			if t.Logger != nil {
				t.Logger.Debug("ParallelClientTransport.readLoop.serverTag", slog.Uint64("tag", uint64(res.Reply().Tag())))
			}
			txn.res = res
			t.putTransaction(res.reqTag(), txn)
			txn.ch <- cltChResponse{res: res}
			close(txn.ch)
		}
	}
}

func (t *ParallelClientTransport) Disconnect() error {
	t.m.Lock()
	defer t.m.Unlock()
	if t.readCancel != nil {
		t.readCancel()
	}
	return t.rwc.Close()
}
func (c *ParallelClientTransport) AllocTransaction() (*cltTransaction, bool) {
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
	return &txn, ok
}
func (c *ParallelClientTransport) ReleaseTransaction(t Tag) {
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
func (t *ParallelClientTransport) Request(txn *cltTransaction) (Message, error) {
	t.m.Lock()
	defer t.m.Unlock()
	return txn.sendAndReceive(t.rwc)
}

////////////////////////////////////////////////////////////////////////////////////////////
// Serial *Retry* Client Transport
// non-concurrent socket read & write, attempt retries & reconnect on IO/network failures
// TODO: handle auth

type SerialRetryClientTransport struct {
	m          sync.Mutex
	rwc        net.Conn
	txn        cltTransaction
	maxMsgSize uint32

	nextServerFid uint32

	mut  sync.Mutex
	fids map[Fid]*fidState // clientFid -> serverFid

	d          Dialer
	network    string
	addr       string
	usr        string
	mnt        string
	authorizee Authorizee
	Logger     *slog.Logger
}

var _ ClientTransport = (*SerialRetryClientTransport)(nil)

func (t *SerialRetryClientTransport) MaxMessageSize() uint32 {
	t.m.Lock()
	defer t.m.Unlock()
	return t.maxMsgSize
}

func (t *SerialRetryClientTransport) Connect(d Dialer, network, addr, usr, mnt string, A Authorizee, L *slog.Logger) error {
	t.mut.Lock()
	t.fids = make(map[Fid]*fidState)
	t.nextServerFid = 0
	t.mut.Unlock()

	t.m.Lock()
	defer t.m.Unlock()
	t.d = d
	if network == "" {
		network = "tcp"
	}
	t.network = network
	t.addr = addr
	t.usr = usr
	t.mnt = mnt
	t.authorizee = A
	t.Logger = L

	return t.unsafeConnect(d, network, addr, usr, mnt, A, L)
}

func (t *SerialRetryClientTransport) unsafeConnect(d Dialer, network, addr, usr, mnt string, A Authorizee, L *slog.Logger) error {
	var err error
	t.rwc, err = d.Dial(network, addr)
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

	req := txn.req.Request()
	tmp := txn.req.clone()
	orig := tmp.Request()
	newMappings := make(map[Fid]Fid)
	RemapFids(req, func(a Fid) Fid {
		if a == NO_FID {
			return a
		}
		value, found := t.fids[a]
		if found {
			return value.mappedFid
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
		if isTemporaryErr(err) {
			continue
		}

		// state to update even if there's an error
		switch req.(type) {
		case Tclunk:
			fid := orig.(Tclunk).Fid()
			if t.Logger != nil {
				t.Logger.Debug("SerialRetryClientTransport.Request.Tclunk", slog.Uint64("fid", uint64(fid)))
			}
			// always clunk
			t.mut.Lock()
			delete(t.fids, fid)
			t.mut.Unlock()
		case Tremove:
			fid := orig.(Tremove).Fid()
			if t.Logger != nil {
				t.Logger.Debug("SerialRetryClientTransport.Request.Tremove", slog.Uint64("fid", uint64(fid)))
			}
			// always clunk
			t.mut.Lock()
			delete(t.fids, fid)
			t.mut.Unlock()
		default: // do nothing
		}

		if err == nil {
			// state to update only on success
			switch m := msg.(type) {
			case Rauth:
				r := req.(Tauth)
				t.mut.Lock()
				t.fids[orig.(Tauth).Afid()] = &fidState{
					qtype:     m.Aqid().Type(),
					mode:      M_AUTH,
					mappedFid: r.Afid(),
					serverFid: r.Afid(),
					uname:     r.Uname(),
					aname:     r.Aname(),
					COMMENT:   "AUTH",
				}
				t.mut.Unlock()
				if t.Logger != nil {
					t.Logger.Debug("SerialRetryClientTransport.Request.Rauth", slog.Uint64("fid", uint64(r.Afid())))
				}
			case Rattach:
				r := req.(Tattach)
				t.mut.Lock()
				t.fids[orig.(Tattach).Fid()] = &fidState{
					qtype:      m.Qid().Type(),
					mode:       M_MOUNT,
					mappedFid:  r.Fid(),
					serverFid:  r.Fid(),
					serverAfid: r.Afid(),
					uname:      r.Uname(),
					aname:      r.Aname(),
					COMMENT:    "ATTACH",
				}
				t.mut.Unlock()
				if t.Logger != nil {
					t.Logger.Debug("SerialRetryClientTransport.Request.Rattach", slog.Uint64("fid", uint64(r.Fid())), slog.Uint64("afid", uint64(r.Afid())))
				}
			case Rwalk:
				r := req.(Twalk)
				var qt QidType
				if m.NumWqid() == r.NumWname() {
					qt = m.Wqid(int(m.NumWqid() - 1)).Type()
				}
				t.mut.Lock()
				t.fids[orig.(Twalk).NewFid()] = &fidState{
					qtype:        qt,
					mappedFid:    r.NewFid(),
					serverFid:    r.Fid(),
					serverNewFid: r.NewFid(),
				}
				if t.Logger != nil {
					t.Logger.Debug("SerialRetryClientTransport.Request.Rwalk", slog.Uint64("fid", uint64(r.Fid())), slog.Uint64("newFid", uint64(r.NewFid())))
				}
				t.mut.Unlock()
			case Ropen:
				r := req.(Topen)
				t.mut.Lock()
				t.fids[orig.(Topen).Fid()] = &fidState{
					qtype:     m.Qid().Type(),
					flag:      r.Mode(),
					mappedFid: r.Fid(),
					serverFid: r.Fid(),
					opened:    true,
				}
				if t.Logger != nil {
					t.Logger.Debug("SerialRetryClientTransport.Request.Ropen", slog.Uint64("fid", uint64(r.Fid())), slog.String("mode", string(r.Mode())))
				}
				t.mut.Unlock()
			case Rcreate:
				r := req.(Tcreate)
				t.mut.Lock()
				t.fids[orig.(Tcreate).Fid()] = &fidState{
					qtype:        m.Qid().Type(),
					path:         []string{r.Name()},
					flag:         r.Mode(),
					mappedFid:    r.Fid(),
					serverNewFid: r.Fid(),
					serverFid:    r.Fid(),
					opened:       true,
				}
				if t.Logger != nil {
					t.Logger.Debug("SerialRetryClientTransport.Request.Rcreate", slog.Uint64("fid", uint64(r.Fid())))
				}
				t.mut.Unlock()
			default:
				// do nothing
			}

			return msg, nil
		}

		if isClosedSocket(err) || isTimeoutErr(err) {
			txn = txn.clone()
			err = t.rwc.Close()
			if err != nil {
				return nil, err
			}
			err = t.unsafeConnect(t.d, t.network, t.addr, t.usr, t.mnt, t.authorizee, t.Logger)
			if err != nil {
				return nil, err
			}

			stateTxn := createClientTransaction(1, t.maxMsgSize)

			// since we're a new 9p session, "push" our internal state to the remote
			// server again, using the following operations:
			//
			// - Tauth/Tattach to remount
			// - Twalk to recreate any fids
			// - Topen to reopen any fids
			// - Topen any created files (via Tcreate)
			// - Forget any fids that got Tclunk or Tremove
			t.mut.Lock()

			// the more correct ordering would be based on usage, but this is good enough
			sortedFids := make(fidSlice, 0, len(t.fids))
			for fid := range t.fids {
				sortedFids = append(sortedFids, fid)
			}
			sort.Sort(sortedFids)

			for _, fid := range sortedFids {
				state, ok := t.fids[fid]
				if !ok {
					if t.Logger != nil {
						t.Logger.Debug("SerialRetryClientTransport.Request.Restore.notFound", slog.Uint64("fid", uint64(fid)))
					}
					t.mut.Unlock()
					continue
				}

				stateTxn.reset()
				if t.Logger != nil {
					t.Logger.Debug("SerialRetryClientTransport.Request.Restore", slog.Any("state", state))
				}

				state.m.Lock()
				if state.mode&M_AUTH != 0 {
					if t.Logger != nil {
						t.Logger.Debug("SerialRetryClientTransport.Request.Restore.Tauth", slog.Uint64("afid", uint64(state.serverAfid)))
					}
					stateTxn.req.Tauth(state.serverAfid, state.uname, state.aname)
					m, err := stateTxn.sendAndReceive(t.rwc)
					if err != nil {
						t.mut.Unlock()
						return nil, fmt.Errorf("Restore: Failed to reattach mount: %w", err)
					}
					_, ok := m.(Rauth)
					if !ok {
						state.m.Unlock()
						t.mut.Unlock()
						return nil, fmt.Errorf("Restore: Expected Rauth from server, got %s", stateTxn.res.responseType())
					}
					if t.authorizee != nil {
						err = t.authorizee.Prove(context.Background(), state.uname, state.aname)
						if err != nil {
							state.m.Unlock()
							t.mut.Unlock()
							return nil, fmt.Errorf("Restore: Error from Authorizee: %w", err)
						}
					}
				} else if state.mode&M_MOUNT != 0 {
					if t.Logger != nil {
						t.Logger.Debug("SerialRetryClientTransport.Request.Restore.Tattach", slog.Uint64("fid", uint64(state.serverFid)), slog.Uint64("afid", uint64(state.serverAfid)))
					}
					stateTxn.req.Tattach(state.serverFid, state.serverAfid, state.uname, state.aname)
					m, err := stateTxn.sendAndReceive(t.rwc)
					if err != nil {
						state.m.Unlock()
						t.mut.Unlock()
						return nil, fmt.Errorf("Restore: Failed to reattach mount: %w", err)
					}
					_, ok := m.(Rattach)
					if !ok {
						state.m.Unlock()
						t.mut.Unlock()
						return nil, fmt.Errorf("Restore: Expected Rattach from server, got %s", stateTxn.res.responseType())
					}
				} else {
					for i := 0; i < numRetries; i++ {
						if t.Logger != nil {
							t.Logger.Debug("SerialRetryClientTransport.Request.Restore.Twalk", slog.Uint64("fid", uint64(state.serverFid)), slog.Uint64("afid", uint64(state.serverAfid)))
						}
						stateTxn.req.Twalk(state.serverFid, state.serverNewFid, state.path)
						m, err := stateTxn.sendAndReceive(t.rwc)
						if err != nil {
							// clunk, b/c its easier right now
							stateTxn.reset()
							stateTxn.req.Tclunk(state.serverFid)
							_, _ = stateTxn.sendAndReceive(t.rwc) // ignoring error
							state.m.Unlock()
							t.mut.Unlock()
							continue // retry
						}
						_, ok := m.(Rwalk)
						if !ok {
							state.m.Unlock()
							t.mut.Unlock()
							return nil, fmt.Errorf("Restore: Expected Rwalk from server, got %s", stateTxn.res.responseType())
						}
						if state.opened {
							// file was created in previous session
							if len(state.path) > 0 {
								stateTxn.reset()
								if t.Logger != nil {
									t.Logger.Debug("SerialRetryClientTransport.Request.Restore.Tcreate", slog.Uint64("fid", uint64(state.serverFid)), slog.String("mode", string(state.flag)))
								}
								stateTxn.req.Twalk(state.serverFid, state.serverNewFid, state.path)
								m, err = stateTxn.sendAndReceive(t.rwc)
								if err != nil {
									// clunk, b/c its easier right now
									stateTxn.reset()
									stateTxn.req.Tclunk(state.serverFid)
									_, e := stateTxn.sendAndReceive(t.rwc) // ignoring error
									if e != nil {
										state.m.Unlock()
										t.mut.Unlock()
										return nil, err
									}
									state.m.Unlock()
									t.mut.Unlock()
									continue // retry
								}

								_, ok := m.(Ropen)
								if !ok {
									state.m.Unlock()
									t.mut.Unlock()
									return nil, fmt.Errorf("Restore: Failed to re-open file: expected Ropen, got %s", stateTxn.res.responseType())
								}
							}
							stateTxn.reset()
							if t.Logger != nil {
								t.Logger.Debug("SerialRetryClientTransport.Request.Restore.Topen", slog.Uint64("fid", uint64(state.serverFid)), slog.String("mode", string(state.flag)))
							}
							stateTxn.req.Topen(state.serverFid, state.flag)
							m, err = stateTxn.sendAndReceive(t.rwc)
							if err != nil {
								// clunk, b/c its easier right now
								stateTxn.reset()
								stateTxn.req.Tclunk(state.serverFid)
								_, e := stateTxn.sendAndReceive(t.rwc) // ignoring error
								state.m.Unlock()
								t.mut.Unlock()
								if e != nil {
									return nil, err
								}
								continue // retry
							}

							_, ok := m.(Ropen)
							if !ok {
								state.m.Unlock()
								t.mut.Unlock()
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

			t.mut.Unlock()
			continue // retry
		}
		return msg, err
	}
}
