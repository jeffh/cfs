package ninep

import (
	"fmt"
	"io"
)

func zero(b []byte) {
	for i := range b {
		b[i] = 0
	}
}

type srvTransaction struct {
	inMsg  []byte
	outMsg []byte

	remoteAddr string

	sem        chan struct{}
	handled    bool
	disconnect bool
}

func createServerTransaction(maxMsgSize uint32) srvTransaction {
	return srvTransaction{
		inMsg:  make([]byte, int(maxMsgSize)),
		outMsg: make([]byte, int(maxMsgSize)),
		sem:    make(chan struct{}, 1),
	}
}

func (t *srvTransaction) wait() {
	<-t.sem
}

func (t *srvTransaction) signalHandled(h bool) {
	t.handled = h
	t.sem <- struct{}{}
}

func (t *srvTransaction) Disconnect() {
	t.disconnect = true
	t.signalHandled(true)
}

func (t *srvTransaction) RemoteAddr() string { return t.remoteAddr }

func (t *srvTransaction) readRequest(rdr io.Reader) error {
	// read size
	_, err := readUpTo(rdr, t.inMsg[:4])
	if err != nil {
		return err
	}

	size := MsgBase(t.inMsg).Size()

	if size > uint32(len(t.inMsg)) {
		return fmt.Errorf("srv: Message too large (%d > %d)", size, len(t.inMsg))
	}

	if size < 4 {
		return fmt.Errorf("srv: Message too small (%d < 4)", size)
	}

	_, err = readUpTo(rdr, t.inMsg[4:size])
	if err != nil {
		return err
	}

	return nil
}

func (t *srvTransaction) writeReply(wr io.Writer) error {
	b := MsgBase(t.outMsg).Bytes()
	for len(b) > 0 {
		n, err := wr.Write(b)
		b = b[n:]
		if isTemporaryErr(err) {
			continue
		} else if err != nil {
			return err
		}
	}
	return nil
}

func (t *srvTransaction) reset() {
	zero(t.inMsg)
	zero(t.outMsg)
	t.handled = false
	t.disconnect = false
}

func (t *srvTransaction) requestType() msgType {
	mb := MsgBase(t.inMsg)
	return mb.msgType()
}

func (t *srvTransaction) Request() Message {
	mb := MsgBase(t.inMsg)
	// fmt.Printf("recv: %s\n", mb.msgType())
	switch mb.msgType() {
	case msgTversion:
		return Tversion(mb)
	case msgTauth:
		return Tauth(mb)
	case msgTattach:
		return Tattach(mb)
	case msgTflush:
		return Tflush(mb)
	case msgTwalk:
		return Twalk(mb)
	case msgTopen:
		return Topen(mb)
	case msgTcreate:
		return Tcreate(mb)
	case msgTread:
		return Tread(mb)
	case msgTwrite:
		return Twrite(mb)
	case msgTclunk:
		return Tclunk(mb)
	case msgTremove:
		return Tremove(mb)
	case msgTstat:
		return Tstat(mb)
	case msgTwstat:
		return Twstat(mb)
	default:
		return mb
	}
}

func (t *srvTransaction) Reply() Message {
	mb := MsgBase(t.outMsg)
	switch mb.msgType() {
	case msgRversion:
		return Rversion(mb)
	case msgRauth:
		return Rauth(mb)
	case msgRattach:
		return Rattach(mb)
	case msgRflush:
		return Rflush(mb)
	case msgRwalk:
		return Rwalk(mb)
	case msgRopen:
		return Ropen(mb)
	case msgRcreate:
		return Rcreate(mb)
	case msgRread:
		return Rread(mb)
	case msgRwrite:
		return Rwrite(mb)
	case msgRclunk:
		return Rclunk(mb)
	case msgRremove:
		return Rremove(mb)
	case msgRstat:
		return Rstat(mb)
	case msgRwstat:
		return Rwstat(mb)
	case msgRerror:
		return Rerror(mb)
	default:
		return mb
	}
}

func (t *srvTransaction) reqTag() Tag {
	return MsgBase(t.inMsg).Tag()
}

func (t *srvTransaction) Rversion(msgSize uint32, version string) {
	Rversion(t.outMsg).fill(t.reqTag(), msgSize, version)
	t.signalHandled(true)
}

func (t *srvTransaction) Rauth(q Qid) {
	Rattach(t.outMsg).fill(t.reqTag(), q)
	t.signalHandled(true)
}

func (t *srvTransaction) Rattach(q Qid) {
	Rattach(t.outMsg).fill(t.reqTag(), q)
	t.signalHandled(true)
}

func (t *srvTransaction) Ropen(q Qid, iounit uint32) {
	Ropen(t.outMsg).fill(t.reqTag(), q, iounit)
	t.signalHandled(true)
}

func (t *srvTransaction) Rwalk(wqids []Qid) {
	Rwalk(t.outMsg).fill(t.reqTag(), wqids)
	t.signalHandled(true)
}

func (t *srvTransaction) Rstat(s Stat) {
	Rstat(t.outMsg).fill(t.reqTag(), s)
	t.signalHandled(true)
}

func (t *srvTransaction) Rwstat() {
	Rwstat(t.outMsg).fill(t.reqTag())
	t.signalHandled(true)
}

func (t *srvTransaction) RreadBuffer() []byte {
	return Rread(t.outMsg).DataNoLimit()
}

// Use RreadBuffer to access the raw data buffer before setting this
func (t *srvTransaction) Rread(count uint32) {
	Rread(t.outMsg).fill(t.reqTag(), count)
	t.signalHandled(true)
}

func (t *srvTransaction) Rclunk() {
	Rclunk(t.outMsg).fill(t.reqTag())
	t.signalHandled(true)
}

func (t *srvTransaction) Rremove() {
	Rremove(t.outMsg).fill(t.reqTag())
	t.signalHandled(true)
}

func (t *srvTransaction) Rcreate(q Qid, iounit uint32) {
	Rcreate(t.outMsg).fill(t.reqTag(), q, iounit)
	t.signalHandled(true)
}

func (t *srvTransaction) Rwrite(count uint32) {
	Rwrite(t.outMsg).fill(t.reqTag(), count)
	t.signalHandled(true)
}

func (t *srvTransaction) Rflush() {
	Rflush(t.outMsg).fill(t.reqTag())
	t.signalHandled(true)
}

func (t *srvTransaction) Rerror(err error) {
	Rerror(t.outMsg).fill(t.reqTag(), underlyingError(err))
	t.signalHandled(true)
}

func (t *srvTransaction) Rerrorf(format string, values ...interface{}) {
	msg := fmt.Sprintf(format, values...)
	Rerror(t.outMsg).fill(t.reqTag(), msg)
	t.signalHandled(true)
}

////////////////////////////////////////////////////////////////////////

type cltRequest struct {
	outMsg []byte
	tag    Tag
}

type cltResponse struct {
	inMsg []byte
}

func createClientRequest(tag Tag, maxMsgSize uint32) cltRequest {
	return cltRequest{
		outMsg: make([]byte, int(maxMsgSize)),
		tag:    tag,
	}
}
func createClientResponse(maxMsgSize uint32) cltResponse {
	return cltResponse{
		inMsg: make([]byte, int(maxMsgSize)),
	}
}

func (t *cltRequest) clone() cltRequest {
	buf := make([]byte, cap(t.outMsg))
	n := copy(buf, t.outMsg)
	buf = buf[:n]
	return cltRequest{
		outMsg: buf,
		tag:    t.tag,
	}
}

func (t *cltResponse) readReply(rdr io.Reader) error {
	// read size
	_, err := readUpTo(rdr, t.inMsg[:4])
	if err != nil {
		return err
	}

	size := MsgBase(t.inMsg).Size()

	if size > uint32(len(t.inMsg)) {
		return fmt.Errorf("clt: Message too large (%d > %d)", size, len(t.inMsg))
	}

	if size < 4 {
		return fmt.Errorf("clt: Message size too short (%d < %d)", size, 4)
	}

	_, err = readUpTo(rdr, t.inMsg[4:size])
	if err != nil {
		return err
	}

	return nil
}

func (t *cltRequest) writeRequest(wr io.Writer) error {
	b := MsgBase(t.outMsg).Bytes()
	for len(b) > 0 {
		n, err := wr.Write(b)
		b = b[n:]
		if isTemporaryErr(err) {
			continue
		} else if err != nil {
			return err
		}
	}
	return nil
}

func (t *cltRequest) reset()  { zero(t.outMsg) }
func (t *cltResponse) reset() { zero(t.inMsg) }

func (t *cltRequest) requestType() msgType {
	mb := MsgBase(t.outMsg)
	return mb.msgType()
}

func (t *cltRequest) Request() Message {
	mb := MsgBase(t.outMsg)
	// fmt.Printf("recv: %s\n", mb.msgType())
	switch mb.msgType() {
	case msgTversion:
		return Tversion(mb)
	case msgTauth:
		return Tauth(mb)
	case msgTattach:
		return Tattach(mb)
	case msgTflush:
		return Tflush(mb)
	case msgTwalk:
		return Twalk(mb)
	case msgTopen:
		return Topen(mb)
	case msgTcreate:
		return Tcreate(mb)
	case msgTread:
		return Tread(mb)
	case msgTwrite:
		return Twrite(mb)
	case msgTclunk:
		return Tclunk(mb)
	case msgTremove:
		return Tremove(mb)
	case msgTstat:
		return Tstat(mb)
	case msgTwstat:
		return Twstat(mb)
	default:
		return mb
	}
}

func (t *cltResponse) responseType() msgType {
	mb := MsgBase(t.inMsg)
	return mb.msgType()
}

func (t *cltResponse) Reply() Message {
	mb := MsgBase(t.inMsg)
	switch mb.msgType() {
	case msgRversion:
		return Rversion(mb)
	case msgRauth:
		return Rauth(mb)
	case msgRattach:
		return Rattach(mb)
	case msgRflush:
		return Rflush(mb)
	case msgRwalk:
		return Rwalk(mb)
	case msgRopen:
		return Ropen(mb)
	case msgRcreate:
		return Rcreate(mb)
	case msgRread:
		return Rread(mb)
	case msgRwrite:
		return Rwrite(mb)
	case msgRclunk:
		return Rclunk(mb)
	case msgRremove:
		return Rremove(mb)
	case msgRstat:
		return Rstat(mb)
	case msgRwstat:
		return Rwstat(mb)
	case msgRerror:
		return Rerror(mb)
	default:
		return mb
	}
}

func (t *cltRequest) reqTag() Tag  { return t.tag }
func (t *cltResponse) reqTag() Tag { return MsgBase(t.inMsg).Tag() }

func (t *cltRequest) Tversion(msgSize uint32, version string) {
	Tversion(t.outMsg).fill(t.reqTag(), msgSize, version)
}

func (t *cltRequest) Tauth(afid Fid, uname, aname string) {
	Tauth(t.outMsg).fill(t.reqTag(), afid, uname, aname)
}

func (t *cltRequest) Tattach(fid, afid Fid, uname, aname string) {
	Tattach(t.outMsg).fill(t.reqTag(), fid, afid, uname, aname)
}

func (t *cltRequest) Topen(f Fid, om OpenMode) {
	Topen(t.outMsg).fill(t.reqTag(), f, om)
}

func (t *cltRequest) Twalk(inF, outF Fid, path []string) {
	Twalk(t.outMsg).fill(t.reqTag(), inF, outF, path)
}

func (t *cltRequest) Tstat(f Fid) {
	Tstat(t.outMsg).fill(t.reqTag(), f)
}

func (t *cltRequest) Twstat(f Fid, s Stat) {
	Twstat(t.outMsg).fill(t.reqTag(), f, s)
}

func (t *cltRequest) Tread(f Fid, offset uint64, count uint32) {
	Tread(t.outMsg).fill(t.reqTag(), f, offset, count)
}

func (t *cltRequest) Tclunk(f Fid) {
	Tclunk(t.outMsg).fill(t.reqTag(), f)
}

func (t *cltRequest) Tremove(f Fid) {
	Tremove(t.outMsg).fill(t.reqTag(), f)
}

func (t *cltRequest) Tcreate(f Fid, name string, perm uint32, om OpenMode) {
	Tcreate(t.outMsg).fill(t.reqTag(), f, name, perm, om)
}

func (t *cltRequest) TwriteBuffer() []byte {
	return Twrite(t.outMsg).DataNoLimit()
}

func (t *cltRequest) Twrite(f Fid, offset uint64, count uint32) {
	Twrite(t.outMsg).fill(t.reqTag(), f, offset, count)
}

func (t *cltRequest) Tflush(tagToCancel Tag) {
	Tflush(t.outMsg).fill(t.reqTag(), tagToCancel)
}
