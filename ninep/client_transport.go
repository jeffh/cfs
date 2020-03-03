package ninep

import (
	"fmt"
	"net"
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

////////////////////////////////////////////////////////////////////////////////////////////
// Serial Client Transport

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
// Serial *Retry* Client Transport

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

		switch r := req.(type) {
		case Tclunk:
			fid := r.Fid()
			t.Tracef("Save: Tclunk(%d, ..., ...)", fid)
			// always clunk
			t.mut.Lock()
			delete(t.fids, fid)
			t.mut.Unlock()
		case Tremove:
			fid := r.Fid()
			t.Tracef("Save: Tremove(%d, ..., ...)", fid)
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
					qtype:        qid.Type(),
					mode:         M_MOUNT,
					serverFid:    r.Fid(),
					serverNewFid: r.NewFid(),
				}
				t.Tracef("Save: Rwalk(%d, %d, ..., ...)", r.Fid(), r.NewFid())
				t.mut.Unlock()
			case Ropen:
				r := req.(Topen)
				t.mut.Lock()
				t.fids[orig.(Topen).Fid()] = fidState{
					qtype:     m.Qid().Type(),
					flag:      r.Mode(),
					serverFid: r.Fid(),
					opened:    true,
				}
				t.Tracef("Save: Ropen(%d, %d, ..., ...)", r.Fid())
				t.mut.Unlock()
			case Rcreate:
				r := req.(Tcreate)
				t.mut.Lock()
				t.fids[orig.(Tcreate).Fid()] = fidState{
					qtype:        m.Qid().Type(),
					path:         []string{r.Name()},
					flag:         r.Mode(),
					serverNewFid: r.Fid(),
					serverFid:    r.Fid(),
					opened:       true,
				}
				t.Tracef("Save: Rcreate(%d, ..., ...)", r.Fid())
				t.mut.Unlock()
			default:
				// do nothing
			}

			return msg, nil
		}

		if IsTemporaryErr(err) {
			continue
		}

		if IsClosedSocket(err) || IsTimeoutErr(err) {
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
			// server again, using the following operations:
			//
			// - Tauth/Tattach to remount
			// - Twalk to recreate any fids
			// - Topen to reopen any fids
			// - Topen any created files (via Tcreate)
			// - Forget any fids that got Tclunk or Tremove
			t.mut.Lock()
			defer t.mut.Unlock()
			for _, state := range t.fids {
				stateTxn.reset()
				t.Tracef("Restore: %#v", state)

				state.m.Lock()
				if state.mode&M_MOUNT != 0 {
					t.Tracef("Restore: Tattach(%d, %d, ..., ...)", state.serverFid, state.serverAfid)
					stateTxn.req.Tattach(state.serverFid, state.serverAfid, state.uname, state.aname)
					m, err := stateTxn.sendAndReceive(t.rwc)
					if err != nil {
						return nil, fmt.Errorf("Restore: Failed to reattach mount: %w", err)
					}
					_, ok := m.(Rattach)
					if !ok {
						state.m.Unlock()
						return nil, fmt.Errorf("Restore: Expected Rattach from server, got %s", stateTxn.res.responseType())
					}
				} else {
					for i := 0; i < numRetries; i++ {
						t.Tracef("Restore: Twalk(%d, %d, ..., ...)", state.serverFid, state.serverAfid)
						stateTxn.req.Twalk(state.serverFid, state.serverNewFid, state.path)
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
							return nil, fmt.Errorf("Restore: Expected Rwalk from server, got %s", stateTxn.res.responseType())
						}
						if state.opened {
							// file was created in previous session
							if len(state.path) > 0 {
								stateTxn.reset()
								t.Tracef("Restore: Tcreate(%d, %s, ..., ...) by walking", state.serverFid, state.flag)
								stateTxn.req.Twalk(state.serverFid, state.serverNewFid, state.path)
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
									return nil, fmt.Errorf("Restore: Failed to re-open file: expected Ropen, got %s", stateTxn.res.responseType())
								}
							}
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
		}
		return msg, err
	}
}
