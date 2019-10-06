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

	handled    bool
	disconnect bool
}

func createServerTransaction(maxMsgSize uint32) srvTransaction {
	return srvTransaction{
		inMsg:  make([]byte, int(maxMsgSize)),
		outMsg: make([]byte, int(maxMsgSize)),
	}
}

func (t *srvTransaction) Disconnect() {
	t.handled = true
	t.disconnect = true
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
		return fmt.Errorf("Message too large (%d > %d)", size, len(t.inMsg))
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
		if IsTemporaryErr(err) {
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

func (t *srvTransaction) requestType() MsgType {
	mb := MsgBase(t.inMsg)
	return mb.Type()
}

func (t *srvTransaction) Request() Message {
	mb := MsgBase(t.inMsg)
	// fmt.Printf("recv: %s\n", mb.Type())
	switch mb.Type() {
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
	switch mb.Type() {
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
	t.handled = true
	Rversion(t.outMsg).fill(t.reqTag(), msgSize, version)
}

func (t *srvTransaction) Rauth(q Qid) {
	t.handled = true
	Rattach(t.outMsg).fill(t.reqTag(), q)
}

func (t *srvTransaction) Rattach(q Qid) {
	t.handled = true
	Rattach(t.outMsg).fill(t.reqTag(), q)
}

func (t *srvTransaction) Ropen(q Qid, iounit uint32) {
	t.handled = true
	Ropen(t.outMsg).fill(t.reqTag(), q, iounit)
}

func (t *srvTransaction) Rwalk(wqids []Qid) {
	t.handled = true
	Rwalk(t.outMsg).fill(t.reqTag(), wqids)
}

func (t *srvTransaction) Rstat(s Stat) {
	t.handled = true
	Rstat(t.outMsg).fill(t.reqTag(), s)
}

func (t *srvTransaction) Rwstat() {
	t.handled = true
	Rwstat(t.outMsg).fill(t.reqTag())
}

func (t *srvTransaction) RreadBuffer() []byte {
	return Rread(t.outMsg).DataNoLimit()
}

func (t *srvTransaction) Rread(data []byte) {
	t.handled = true
	Rread(t.outMsg).fill(t.reqTag(), data)
}

func (t *srvTransaction) Rclunk() {
	t.handled = true
	Rclunk(t.outMsg).fill(t.reqTag())
}

func (t *srvTransaction) Rremove() {
	t.handled = true
	Rremove(t.outMsg).fill(t.reqTag())
}

func (t *srvTransaction) Rcreate(q Qid, iounit uint32) {
	t.handled = true
	Rcreate(t.outMsg).fill(t.reqTag(), q, iounit)
}

func (t *srvTransaction) Rwrite(count uint32) {
	t.handled = true
	Rwrite(t.outMsg).fill(t.reqTag(), count)
}

func (t *srvTransaction) Rflush() {
	t.handled = true
	Rflush(t.outMsg).fill(t.reqTag())
}

func (t *srvTransaction) Rerror(err error) {
	t.handled = true
	Rerror(t.outMsg).fill(t.reqTag(), err.Error())
}

func (t *srvTransaction) Rerrorf(format string, values ...interface{}) {
	t.handled = true
	msg := fmt.Sprintf(format, values...)
	Rerror(t.outMsg).fill(t.reqTag(), msg)
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

func (t *cltResponse) readReply(rdr io.Reader) error {
	// read size
	_, err := readUpTo(rdr, t.inMsg[:4])
	if err != nil {
		return err
	}

	size := MsgBase(t.inMsg).Size()

	if size > uint32(len(t.inMsg)) {
		return fmt.Errorf("Message too large (%d > %d)", size, len(t.inMsg))
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
		if IsTemporaryErr(err) {
			continue
		} else if err != nil {
			return err
		}
	}
	return nil
}

func (t *cltRequest) reset()  { zero(t.outMsg) }
func (t *cltResponse) reset() { zero(t.inMsg) }

func (t *cltRequest) requestType() MsgType {
	mb := MsgBase(t.outMsg)
	return mb.Type()
}

func (t *cltRequest) Request() Message {
	mb := MsgBase(t.outMsg)
	// fmt.Printf("recv: %s\n", mb.Type())
	switch mb.Type() {
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

func (t *cltResponse) Reply() Message {
	mb := MsgBase(t.inMsg)
	switch mb.Type() {
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
