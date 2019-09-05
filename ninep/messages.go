package ninep

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
)

var (
	ErrBadFormat = errors.New("Unrecognized 9P protocol")
)

func readUpTo(r io.Reader, p []byte) (int, error) {
	var err error
	n := 0
	b := p

	for len(b) > 0 {
		m, e := r.Read(b)
		n += m
		err = e
		if m == len(b) || e != nil {
			break
		}
		b = b[m:]
	}
	return n, err
}

func ReadMessage(r io.Reader) (Message, error) {
	b := [4]byte{}
	n, err := readUpTo(r, b[:])
	if n != 4 {
		return ErrBadFormat
	}
	if err != nil {
		return err
	}
	size := MsgBase(b).Size()

	msg := make(MsgBase, int(size))
	copy(msg[:4], b)

	n, err = readUpTo(r, msg[4:])
	if n != size-4 {
		return ErrBadFormat
	}
	return msg, err
}

func WriteMessage(w io.Writer, m Message) (int, error) {
	return w.Write([]byte(m))
}

type Message interface {
	Type() MsgType
}

////////////////////////////////////////////////////////////////////////////////

const (
	NO_TAG         Tag    = ^uint16(0)
	NO_FID         Fid    = ^uint32(0)
	VERSION_9P2000 string = "9P2000"

	DEFAULT_MAX_MESSAGE_SIZE = 4096
	MIN_MESSAGE_SIZE         = 128
)

type MsgType byte

const (
	Tversion MsgType = 100
	Rversion         = 101
	Tauth            = 102
	Rauth            = 103
	Rerror           = 107
	Tflush           = 108
	Rflush           = 109
	Twalk            = 110
	Rwalk            = 111
	Topen            = 112
	Ropen            = 113
	Tcreate          = 114
	Rcreate          = 115
	Tread            = 116
	Rread            = 117
	Twrite           = 118
	Rwrite           = 119
	Tclunk           = 120
	Rclunk           = 121
	Tremove          = 122
	Rremove          = 123
	Tstat            = 124
	Rstat            = 125
	Twstat           = 126
	Rwstat           = 127
)

type Mode byte

const (
	O_READ  Mode = 0
	O_WRITE      = 1
	O_RDWR       = 2
	O_EXEC       = 3

	O_TRUNC  = 0x10
	O_RCLOSE = 0x40
)

var bo = binary.LittleEndian

/////////////////////////////////////

type Tag uint16

/////////////////////////////////////

type MsgBase struct{ Raw []byte }

func (r MsgBase) Size() uint32     { return bo.Uint32(r.Raw[:4]) }
func (r MsgBase) SetSize(v uint32) { bo.PutUint32(r.Raw[:4], v) }
func (r MsgBase) ComputeSize()     { r.SetSize(uint32(len(r.Raw))) }

func (r MsgBase) Type() MsgType     { return MsgType(r.Raw[4]) }
func (r MsgBase) SetType(t MsgType) { r.Raw[4] = byte(t) }

func (r MsgBase) Tag() Tag     { return Tag(bo.Uint16(r.Raw[5:7])) }
func (r MsgBase) SetTag(v Tag) { bo.PutUint16(r.Raw[5:7], uint16(v)) }

const msgOffset = 7

/////////////////////////////////////
type msgString []byte

func (s msgString) Len() uint16       { return bo.Uint16(s[0:2]) }
func (s msgString) SetLen(v uint16)   { bo.PutUint16(s[0:2], v) }
func (s msgString) Bytes() []byte     { return s[2 : s.Len()+2] }
func (s msgString) SetBytes(b []byte) { copy(s[2:s.Len()+2], b) }
func (s msgString) SetBytesAndLen(b []byte) {
	if len(b) > math.MaxUint16 {
		panic(fmt.Errorf("invalid string size: %d", len(b)))
	}
	copy(s[2:s.Len()+2], b)
	s.SetLen(uint16(len(b)))
}
func (s msgString) String() string           { return string(s.Bytes()) }
func (s msgString) SetString(v string)       { s.SetBytes([]byte(v)) }
func (s msgString) SetStringAndLen(v string) { s.SetBytesAndLen([]byte(v)) }
func (s msgString) NextOffset() int          { return int(s.Len()) + 2 }

/////////////////////////////////////

// shorthand for replys that are only acks
type RepAck MsgBase

/////////////////////////////////////

type Fid uint32 // always size 4

/////////////////////////////////////

type QidType byte

const (
	QT_FILE    QidType = 0x00
	QT_LINK            = 0x01
	QT_SYMLINK         = 0x02
	QT_TMP             = 0x04
	QT_AUTH            = 0x08
	QT_MOUNT           = 0x10
	QT_EXCL            = 0x20
	QT_DIR             = 0x80
)

type Qid []byte // always size 13

// qid.type[1] the type of the file (directory, etc.), represented as a bit vector corresponding to the high 8 bits of the file's mode word.
// qid.vers[4] version number for given path
// qid.path[8] the file server's unique identification for the file

func (q Qid) Type() QidType     { return QidType(q[0]) }
func (q Qid) SetType(t QidType) { q[0] = byte(t) }

func (q Qid) Version() uint32     { return bo.Uint32(q[1:5]) }
func (q Qid) SetVersion(v uint32) { bo.PutUint32(q[1:5], v) }

func (q Qid) Path() uint64     { return bo.Uint64(q[5 : 5+8]) }
func (q Qid) SetPath(v uint64) { bo.PutUint64(q[5:5+8], v) }

/////////////////////////////////////
/*
DESCRIPTION

The stat transaction inquires about the file identified by fid. The reply will contain a machine-independent directory entry, stat, laid out as follows:

	size[2] total byte count of the following data
	type[2] for kernel use
	dev[4] for kernel use
	qid.type[1] the type of the file (directory, etc.), represented as a bit vector corresponding to the high 8 bits of the file's mode word.
	qid.vers[4] version number for given path
	qid.path[8] the file server's unique identification for the file
	mode[4] permissions and flags
	atime[4] last access time
	mtime[4] last modification time
	length[8] length of file in bytes
	name[s] file name; must be / if the file is the root directory of the server
	uid[s] owner name
	gid[s] group name
	muid[s] name of the user who last modified the file

*/
type Stat []byte

func (s Stat) Size() uint16     { return bo.Uint16(s[0:2]) }
func (s Stat) SetSize(v uint16) { bo.PutUint16(s[0:2], v) }

func (s Stat) Type() uint16     { return bo.Uint16(s[2:4]) }
func (s Stat) SetType(v uint16) { bo.PutUint16(s[2:4], v) }

func (s Stat) Dev() uint16     { return bo.Uint16(s[4:6]) }
func (s Stat) SetDev(v uint16) { bo.PutUint16(s[4:6], v) }

func (s Stat) Qid() Qid     { return Qid(s[6 : 6+13]) }
func (s Stat) SetQid(v Qid) { copy(s[6:6+13], v) }

func (s Stat) Mode() uint32     { return bo.Uint32(s[6+13 : 6+13+4]) }
func (s Stat) SetMode(v uint32) { bo.PutUint32(s[6+13:6+13+4], v) }

func (s Stat) Atime() uint32     { return bo.Uint32(s[23 : 23+4]) }
func (s Stat) SetAtime(v uint32) { bo.PutUint32(s[23:23+4], v) }

func (s Stat) Mtime() uint32     { return bo.Uint32(s[27 : 27+4]) }
func (s Stat) SetMtime(v uint32) { bo.PutUint32(s[27:27+4], v) }

func (s Stat) Length() uint64     { return bo.Uint64(s[31 : 31+8]) }
func (s Stat) SetLength(v uint64) { bo.PutUint64(s[31:31+8], v) }

func (s Stat) name() msgString       { return msgString(s[39:]) }
func (s Stat) NameBytes() []byte     { return s.name().Bytes() }
func (s Stat) Name() string          { return s.name().String() }
func (s Stat) SetNameBytes(v []byte) { s.name().SetBytesAndLen(v) }
func (s Stat) SetName(v string)      { s.name().SetStringAndLen(v) }

func (s Stat) uid() msgString       { return msgString(s[s.name().NextOffset():]) }
func (s Stat) UidBytes() []byte     { return s.uid().Bytes() }
func (s Stat) Uid() string          { return s.uid().String() }
func (s Stat) SetUidBytes(v []byte) { s.uid().SetBytesAndLen(v) }
func (s Stat) SetUid(v string)      { s.uid().SetStringAndLen(v) }

func (s Stat) gid() msgString       { return msgString(s[s.uid().NextOffset():]) }
func (s Stat) GidBytes() []byte     { return s.gid().Bytes() }
func (s Stat) Gid() string          { return s.gid().String() }
func (s Stat) SetGidBytes(v []byte) { s.gid().SetBytesAndLen(v) }
func (s Stat) SetGid(v string)      { s.gid().SetStringAndLen(v) }

func (s Stat) muid() msgString       { return msgString(s[s.gid().NextOffset():]) }
func (s Stat) MuidBytes() []byte     { return s.muid().Bytes() }
func (s Stat) Muid() string          { return s.muid().String() }
func (s Stat) SetMuidBytes(v []byte) { s.muid().SetBytesAndLen(v) }
func (s Stat) SetMuid(v string)      { s.muid().SetStringAndLen(v) }

func fillMsgBase(m MsgBase, mt MsgType, t Tag) {
	v.SetTag(t)
	v.SetType(mt)
	v.ComputeSize()
}

/////////////////////////////////////
// size[4] Tversion tag[2] msize[4] version[s]
type ReqVersion struct{ MsgBase }

func MakeReqVersion(maxMessageSize uint32, version string) ReqVersion {
	v := make(ReqVersion, msgOffset+4+2+len([]byte(version)))
	fillMsgBase(v, Tversion, NO_TAG)
	v.SetMsgSize(maxMessageSize)
	v.SetVersion(version)
	return v
}

func (r ReqVersion) MsgSize() uint32     { return bo.Uint32(r.MsgBase.Raw[msgOffset : msgOffset+4]) }
func (r ReqVersion) SetMsgSize(v uint32) { bo.PutUint32(r.MsgBase.Raw[msgOffset:msgOffset+4], v) }

func (r ReqVersion) version() msgString       { return msgString(r.MsgBase.Raw[msgOffset+4:]) }
func (r ReqVersion) VersionBytes() []byte     { return r.version().Bytes() }
func (r ReqVersion) Version() string          { return r.version().String() }
func (r ReqVersion) SetVersionBytes(v []byte) { r.version().SetBytesAndLen(v) }
func (r ReqVersion) SetVersion(v string)      { r.version().SetStringAndLen(v) }

/////////////////////////////////////
// size[4] Rversion tag[2] msize[4] version[s]
type RepVersion struct{ MsgBase }

func MakeRepVersion(maxMessageSize uint32, version string) ReqVersion {
	v := make(ReqVersion, msgOffset+4+2+len([]byte(version)))
	fillMsgBase(v, Tversion, NO_TAG)
	v.SetMsgSize(maxMessageSize)
	v.SetVersion(version)
	return v
}

func (r RepVersion) MsgSize() uint32     { return bo.Uint32(r.MsgBase.Raw[msgOffset : msgOffset+4]) }
func (r RepVersion) SetMsgSize(v uint32) { bo.PutUint32(r.MsgBase.Raw[msgOffset:msgOffset+4], v) }

func (r RepVersion) version() msgString       { return msgString(r.MsgBase.Raw[msgOffset+4:]) }
func (r RepVersion) VersionBytes() []byte     { return r.version().Bytes() }
func (r RepVersion) Version() string          { return r.version().String() }
func (r RepVersion) SetVersionBytes(v []byte) { r.version().SetBytesAndLen(v) }
func (r RepVersion) SetVersion(v string)      { r.version().SetStringAndLen(v) }

/////////////////////////////////////
//size[4] Tauth tag[2] afid[4] name[s] aname[s]
type ReqAuth struct{ MsgBase }

func (r ReqAuth) Afid() Fid     { return Fid(bo.Uint32(r.MsgBase.Raw[msgOffset : msgOffset+4])) }
func (r ReqAuth) SetAfid(v Fid) { bo.PutUint32(r.MsgBase.Raw[msgOffset:msgOffset+4], uint32(v)) }

func (r ReqAuth) uname() msgString       { return msgString(r.MsgBase.Raw[msgOffset+4:]) }
func (r ReqAuth) UnameBytes() []byte     { return r.uname().Bytes() }
func (r ReqAuth) Uname() string          { return r.uname().String() }
func (r ReqAuth) SetUnameBytes(v []byte) { r.uname().SetBytesAndLen(v) }
func (r ReqAuth) SetUname(v string)      { r.uname().SetStringAndLen(v) }

func (r ReqAuth) aname() msgString       { return msgString(r.MsgBase.Raw[r.uname().NextOffset():]) }
func (r ReqAuth) AnameBytes() []byte     { return r.aname().Bytes() }
func (r ReqAuth) Aname() string          { return r.aname().String() }
func (r ReqAuth) SetAnameBytes(v []byte) { r.aname().SetBytesAndLen(v) }
func (r ReqAuth) SetAname(v string)      { r.aname().SetStringAndLen(v) }

/////////////////////////////////////
// size[4] Rauth tag[2] aqid[13]
type RepAuth struct{ MsgBase }

func (r RepAuth) Aqid() Qid     { return Qid(r.MsgBase.Raw[msgOffset : msgOffset+13]) }
func (r RepAuth) SetAqid(v Qid) { copy(r.MsgBase.Raw[msgOffset:msgOffset+13], v) }

/////////////////////////////////////
// size[4] Rerror tag[2] ename[s]
type RepError struct{ MsgBase }

func MakeRepError(tag Tag, msg string) RepError {
	m := make(RepError, msgOffset+2+2+len([]byte(msg)))
	m.SetEname(msg)
	fillMsgBase(m, Rerror, tag)
	return m
}

func (r RepError) ename() msgString       { return msgString(r.MsgBase.Raw[msgOffset:]) }
func (r RepError) EnameBytes() []byte     { return r.ename().Bytes() }
func (r RepError) Ename() string          { return r.ename().String() }
func (r RepError) SetEnameBytes(v []byte) { r.ename().SetBytesAndLen(v) }
func (r RepError) SetEname(v string)      { r.ename().SetStringAndLen(v) }

/////////////////////////////////////
// size[4] Tclunk tag[2] fid[4]
type ReqClunk struct{ MsgBase }

func (r ReqClunk) Fid() Fid     { return Fid(bo.Uint32(r.MsgBase.Raw[msgOffset : msgOffset+4])) }
func (r ReqClunk) SetFid(v Fid) { bo.PutUint32(r.MsgBase.Raw[msgOffset:msgOffset+4], uint32(v)) }

/////////////////////////////////////
// size[4] Rclunk tag[2]
type RepClunk RepAck

/////////////////////////////////////
// size[4] Tflush tag[2] oldtag[2]
type ReqFlush struct{ MsgBase }

func (r ReqFlush) OldTag() Tag     { return Tag(bo.Uint16(r.MsgBase.Raw[msgOffset : msgOffset+2])) }
func (r ReqFlush) OldSetTag(v Tag) { bo.PutUint16(r.MsgBase.Raw[msgOffset:msgOffset+2], uint16(v)) }

/////////////////////////////////////
// size[4] Rflush tag[2]
type RepFlush RepAck

/////////////////////////////////////
// size[4] Tattach tag[2] fid[4] afid[4] uname[s] aname[s]
type ReqAttach struct{ MsgBase }

func (r ReqAttach) Fid() Fid     { return Fid(bo.Uint32(r.MsgBase.Raw[msgOffset : msgOffset+4])) }
func (r ReqAttach) SetFid(v Fid) { bo.PutUint32(r.MsgBase.Raw[msgOffset:msgOffset+4], uint32(v)) }

func (r ReqAttach) Afid() Fid     { return Fid(bo.Uint32(r.MsgBase.Raw[msgOffset+4 : msgOffset+8])) }
func (r ReqAttach) SetAfid(v Fid) { bo.PutUint32(r.MsgBase.Raw[msgOffset+4:msgOffset+8], uint32(v)) }

func (r ReqAttach) uname() msgString       { return msgString(r.MsgBase.Raw[msgOffset+8:]) }
func (r ReqAttach) UnameBytes() []byte     { return r.uname().Bytes() }
func (r ReqAttach) Uname() string          { return r.uname().String() }
func (r ReqAttach) SetUnameBytes(v []byte) { r.uname().SetBytesAndLen(v) }
func (r ReqAttach) SetUname(v string)      { r.uname().SetStringAndLen(v) }

func (r ReqAttach) aname() msgString       { return msgString(r.MsgBase.Raw[r.uname().NextOffset():]) }
func (r ReqAttach) AnameBytes() []byte     { return r.aname().Bytes() }
func (r ReqAttach) Aname() string          { return r.aname().String() }
func (r ReqAttach) SetAnameBytes(v []byte) { r.aname().SetBytesAndLen(v) }
func (r ReqAttach) SetAname(v string)      { r.aname().SetStringAndLen(v) }

/////////////////////////////////////
// size[4] Rattach tag[2] qid[13]
type RepAttach struct{ MsgBase }

func (r RepAttach) Qid() Qid     { return Qid(r.MsgBase.Raw[msgOffset : msgOffset+13]) }
func (r RepAttach) SetQid(v Qid) { copy(r.MsgBase.Raw[msgOffset:msgOffset+13], v) }

/////////////////////////////////////
// size[4] Topen tag[2] fid[4] mode[1]
type ReqOpen struct{ MsgBase }

func (r ReqOpen) Fid() Fid     { return Fid(bo.Uint32(r.MsgBase.Raw[msgOffset : msgOffset+4])) }
func (r ReqOpen) SetFid(v Fid) { bo.PutUint32(r.MsgBase.Raw[msgOffset:msgOffset+4], uint32(v)) }

func (r ReqOpen) Mode() Mode     { return Mode(r.MsgBase.Raw[msgOffset+4]) }
func (r ReqOpen) SetMode(v Mode) { r.MsgBase.Raw[msgOffset+4] = byte(v) }

/////////////////////////////////////
// size[4] Ropen tag[2] qid[13] iounit[4]
type RepOpen struct{ MsgBase }

func (r RepAuth) Qid() Qid     { return Qid(r.MsgBase.Raw[msgOffset : msgOffset+13]) }
func (r RepAuth) SetQid(v Qid) { copy(r.MsgBase.Raw[msgOffset:msgOffset+13], v) }

func (r ReqOpen) Iounit() uint32     { return bo.Uint32(r.MsgBase.Raw[msgOffset+13 : msgOffset+13+4]) }
func (r ReqOpen) SetIounit(v uint32) { bo.PutUint32(r.MsgBase.Raw[msgOffset+13:msgOffset+13+4], v) }

/////////////////////////////////////
// size[4] Tcreate tag[2] fid[4] name[s] perm[4] mode[1]
type ReqCreate struct{ MsgBase }

func (r ReqCreate) Fid() Fid     { return Fid(bo.Uint32(r.MsgBase.Raw[msgOffset : msgOffset+4])) }
func (r ReqCreate) SetFid(v Fid) { bo.PutUint32(r.MsgBase.Raw[msgOffset:msgOffset+4], uint32(v)) }

func (r ReqCreate) name() msgString       { return msgString(r.MsgBase.Raw[msgOffset+4:]) }
func (r ReqCreate) NameBytes() []byte     { return r.name().Bytes() }
func (r ReqCreate) Name() string          { return r.name().String() }
func (r ReqCreate) SetNameBytes(v []byte) { r.name().SetBytesAndLen(v) }
func (r ReqCreate) SetName(v string)      { r.name().SetStringAndLen(v) }

func (r ReqCreate) Perm() uint32 { o := r.name().NextOffset(); return bo.Uint32(r.MsgBase.Raw[o : o+4]) }
func (r ReqCreate) SetPerm(v uint32) {
	o := r.name().NextOffset()
	bo.PutUint32(r.MsgBase.Raw[o:o+4], v)
}

func (r ReqCreate) Mode() Mode     { return Mode(r.MsgBase.Raw[r.name().NextOffset()+4]) }
func (r ReqCreate) SetMode(v Mode) { r.MsgBase.Raw[r.name().NextOffset()+4] = byte(v) }

/////////////////////////////////////
// size[4] Rcreate tag[2] qid[13] iounit[4]
type RepCreate struct{ MsgBase }

func (r RepCreate) Qid() Qid     { return Qid(r.MsgBase.Raw[msgOffset : msgOffset+13]) }
func (r RepCreate) SetQid(v Qid) { copy(r.MsgBase.Raw[msgOffset:msgOffset+13], v) }

func (r ReqCreate) Iounit() uint32     { return bo.Uint32(r.MsgBase.Raw[msgOffset+13 : msgOffset+13+4]) }
func (r ReqCreate) SetIounit(v uint32) { bo.PutUint32(r.MsgBase.Raw[msgOffset+13:msgOffset+13+4], v) }

/////////////////////////////////////
// size[4] Tread tag[2] fid[4] offset[8] count[4]
type ReqRead struct{ MsgBase }

func (r ReqRead) Fid() Fid     { return Fid(bo.Uint32(r.MsgBase.Raw[msgOffset : msgOffset+4])) }
func (r ReqRead) SetFid(v Fid) { bo.PutUint32(r.MsgBase.Raw[msgOffset:msgOffset+4], uint32(v)) }

func (r ReqRead) Offset() uint64     { return bo.Uint64(r.MsgBase.Raw[msgOffset+4 : msgOffset+12]) }
func (r ReqRead) SetOffset(v uint64) { bo.PutUint64(r.MsgBase.Raw[msgOffset+4:msgOffset+12], v) }

func (r ReqRead) Count() uint32     { return bo.Uint32(r.MsgBase.Raw[msgOffset+12 : msgOffset+16]) }
func (r ReqRead) SetCount(v uint32) { bo.PutUint32(r.MsgBase.Raw[msgOffset+12:msgOffset+16], v) }

/////////////////////////////////////
// size[4] Rread tag[2] count[4] data[count]
type RepRead struct{ MsgBase }

func (r RepRead) Count() uint32     { return bo.Uint32(r.MsgBase.Raw[msgOffset : msgOffset+4]) }
func (r RepRead) SetCount(v uint32) { bo.PutUint32(r.MsgBase.Raw[msgOffset:msgOffset+4], v) }

func (r RepRead) Data() []byte            { return r.MsgBase.Raw[msgOffset+4:] }
func (r RepRead) SetDataNoCount(b []byte) { copy(r.MsgBase.Raw[msgOffset+4:], b) }
func (r RepRead) SetData(b []byte) {
	if len(b) > math.MaxUint32 {
		panic(fmt.Errorf("data is larger than allowed: %d", len(b)))
	}
	r.SetDataNoCount(b)
	r.SetCount(uint32(len(b)))
}

/////////////////////////////////////
// size[4] Twrite tag[2] fid[4] offset[8] count[4] data[count]
type ReqWrite struct{ MsgBase }

func (r ReqWrite) Fid() Fid     { return Fid(bo.Uint32(r.MsgBase.Raw[msgOffset : msgOffset+4])) }
func (r ReqWrite) SetFid(v Fid) { bo.PutUint32(r.MsgBase.Raw[msgOffset:msgOffset+4], uint32(v)) }

func (r ReqWrite) Offset() uint64     { return bo.Uint64(r.MsgBase.Raw[msgOffset+4 : msgOffset+12]) }
func (r ReqWrite) SetOffset(v uint64) { bo.PutUint64(r.MsgBase.Raw[msgOffset+4:msgOffset+12], v) }

func (r ReqWrite) Count() uint32     { return bo.Uint32(r.MsgBase.Raw[msgOffset+12 : msgOffset+16]) }
func (r ReqWrite) SetCount(v uint32) { bo.PutUint32(r.MsgBase.Raw[msgOffset+12:msgOffset+16], v) }

func (r ReqWrite) Data() []byte            { return r.MsgBase.Raw[msgOffset+16:] }
func (r ReqWrite) SetDataNoCount(b []byte) { copy(r.Data(), b) }
func (r ReqWrite) SetData(b []byte) {
	if len(b) > math.MaxUint32 {
		panic(fmt.Errorf("data is larger than allowed: %d", len(b)))
	}
	r.SetDataNoCount(b)
	r.SetCount(uint32(len(b)))
}

/////////////////////////////////////
// size[4] Rwrite tag[2] count[4]
type RepWrite struct{ MsgBase }

func (r RepWrite) Count() uint32     { return bo.Uint32(r.MsgBase.Raw[msgOffset : msgOffset+4]) }
func (r RepWrite) SetCount(v uint32) { bo.PutUint32(r.MsgBase.Raw[msgOffset:msgOffset+4], v) }

/////////////////////////////////////
// size[4] Tremove tag[2] fid[4]
type ReqRemove struct{ MsgBase }

func (r ReqRemove) Fid() Fid     { return Fid(bo.Uint32(r.MsgBase.Raw[msgOffset : msgOffset+4])) }
func (r ReqRemove) SetFid(v Fid) { bo.PutUint32(r.MsgBase.Raw[msgOffset:msgOffset+4], uint32(v)) }

/////////////////////////////////////
// size[4] Rremove tag[2]
type RepRemove RepAck

/////////////////////////////////////
// size[4] Tstat tag[2] fid[4]
type ReqStat struct{ MsgBase }

func (r ReqStat) Fid() Fid     { return Fid(bo.Uint32(r.MsgBase.Raw[msgOffset : msgOffset+4])) }
func (r ReqStat) SetFid(v Fid) { bo.PutUint32(r.MsgBase.Raw[msgOffset:msgOffset+4], uint32(v)) }

/////////////////////////////////////
// size[4] Rstat tag[2] stat[n]
type RepStat struct{ MsgBase }

func (r RepStat) Stat() Stat     { return Stat(r.MsgBase.Raw[msgOffset:]) }
func (r RepStat) SetStat(s Stat) { copy(r.MsgBase.Raw[msgOffset:], s) }

/////////////////////////////////////
// size[4] Twstat tag[2] fid[4] stat[n]
type ReqWstat struct{ MsgBase }

func (r ReqWstat) Fid() Fid     { return Fid(bo.Uint32(r.MsgBase.Raw[msgOffset : msgOffset+4])) }
func (r ReqWstat) SetFid(v Fid) { bo.PutUint32(r.MsgBase.Raw[msgOffset:msgOffset+4], uint32(v)) }

func (r ReqWstat) Stat() Stat     { return Stat(r.MsgBase.Raw[msgOffset+4:]) }
func (r ReqWstat) SetStat(s Stat) { copy(r.MsgBase.Raw[msgOffset+4:], s) }

/////////////////////////////////////
// size[4] Rwstat tag[2]
type RepWstat RepAck
