package ninep

import (
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"os"
	"strings"
	"time"
)

const (
	NoTouchU64  = ^uint64(0)
	NoTouchU32  = ^uint32(0)
	NoTouchU16  = ^uint16(0)
	NoTouchMode = ^Mode(0)

	NoQidVersion = NoTouchU32
)

type Message interface {
	Tag() Tag
	Bytes() []byte
}

////////////////////////////////////////////////////////////////////////////////

type Fidable interface {
	Fid() Fid
	SetFid(v Fid)
}

type NewFidable interface {
	NewFid() Fid
	SetNewFid(v Fid)
}

type Afidable interface {
	Afid() Fid
	SetAfid(v Fid)
}

func RemapFids(m Message, mapper func(a Fid) Fid) Message {
	if msg, ok := m.(Fidable); ok {
		f := mapper(msg.Fid())
		msg.SetFid(f)
	}
	if msg, ok := m.(NewFidable); ok {
		msg.SetNewFid(mapper(msg.NewFid()))
	}
	if msg, ok := m.(Afidable); ok {
		msg.SetAfid(mapper(msg.Afid()))
	}
	return m
}

////////////////////////////////////////////////////////////////////////////////

const (
	NO_TAG         Tag    = ^Tag(0)
	NO_FID         Fid    = ^Fid(0)
	VERSION_9P2000 string = "9P2000"
	VERSION_9P     string = "9P"

	MIN_MESSAGE_SIZE = uint32(128)
)

// The default maximum size of 9p message blocks. Should never be below MIN_MESSAGE_SIZE
var DEFAULT_MAX_MESSAGE_SIZE uint32

func init() {
	s := uint32(os.Getpagesize() * 2)
	if s > math.MaxUint32 {
		s = math.MaxUint32
	}
	if uint32(s) < MIN_MESSAGE_SIZE {
		s = MIN_MESSAGE_SIZE
	}
	DEFAULT_MAX_MESSAGE_SIZE = s
}

type MsgType byte

// Based on
// http://plan9.bell-labs.com/sources/plan9/sys/include/fcall.h
const (
	msgTversion MsgType = iota + 100 // size[4] Tversion tag[2] msize[4] version[s]
	msgRversion                      // size[4] Rversion tag[2] msize[4] version[s]
	msgTauth                         // size[4] Tauth tag[2] afid[4] uname[s] aname[s]
	msgRauth                         // size[4] Rauth tag[2] aqid[13]
	msgTattach                       // size[4] Tattach tag[2] fid[4] afid[4] uname[s] aname[s]
	msgRattach                       // size[4] Rattach tag[2] qid[13]
	msgTerror                        // illegal
	msgRerror                        // size[4] Rerror tag[2] ename[s]
	msgTflush                        // size[4] Tflush tag[2] oldtag[2]
	msgRflush                        // size[4] Rflush tag[2]
	msgTwalk                         // size[4] Twalk tag[2] fid[4] newfid[4] nwname[2] nwname*(wname[s])
	msgRwalk                         // size[4] Rwalk tag[2] nwqid[2] nwqid*(wqid[13])
	msgTopen                         // size[4] Topen tag[2] fid[4] mode[1]
	msgRopen                         // size[4] Ropen tag[2] qid[13] iounit[4]
	msgTcreate                       // size[4] Tcreate tag[2] fid[4] name[s] perm[4] mode[1]
	msgRcreate                       // size[4] Rcreate tag[2] qid[13] iounit[4]
	msgTread                         // size[4] Tread tag[2] fid[4] offset[8] count[4]
	msgRread                         // size[4] Rread tag[2] count[4] data[count]
	msgTwrite                        // size[4] Twrite tag[2] fid[4] offset[8] count[4] data[count]
	msgRwrite                        // size[4] Rwrite tag[2] count[4]
	msgTclunk                        // size[4] Tclunk tag[2] fid[4]
	msgRclunk                        // size[4] Rclunk tag[2]
	msgTremove                       // size[4] Tremove tag[2] fid[4]
	msgRremove                       // size[4] Rremove tag[2]
	msgTstat                         // size[4] Tstat tag[2] fid[4]
	msgRstat                         // size[4] Rstat tag[2] stat[n]
	msgTwstat                        // size[4] Twstat tag[2] fid[4] stat[n]
	msgRwstat                        // size[4] Rwstat tag[2]
)

func (t MsgType) String() string {
	switch t {
	case msgTversion:
		return "msgTversion"
	case msgRversion:
		return "msgRversion"
	case msgTauth:
		return "msgTauth"
	case msgRauth:
		return "msgRauth"
	case msgTattach:
		return "msgTattach"
	case msgRattach:
		return "msgRattach"
	case msgTerror:
		return "msgTerror"
	case msgRerror:
		return "msgRerror"
	case msgTflush:
		return "msgTflush"
	case msgRflush:
		return "msgRflush"
	case msgTwalk:
		return "msgTwalk"
	case msgRwalk:
		return "msgRwalk"
	case msgTopen:
		return "msgTopen"
	case msgRopen:
		return "msgRopen"
	case msgTcreate:
		return "msgTcreate"
	case msgRcreate:
		return "msgRcreate"
	case msgTread:
		return "msgTread"
	case msgRread:
		return "msgRread"
	case msgTwrite:
		return "msgTwrite"
	case msgRwrite:
		return "msgRwrite"
	case msgTclunk:
		return "msgTclunk"
	case msgRclunk:
		return "msgRclunk"
	case msgTremove:
		return "msgTremove"
	case msgRremove:
		return "msgRremove"
	case msgTstat:
		return "msgTstat"
	case msgRstat:
		return "msgRstat"
	case msgTwstat:
		return "msgTwstat"
	case msgRwstat:
		return "msgRwstat"
	}
	panic(fmt.Errorf("Unexpected message: %d", t))
}

type OpenMode byte

const (
	OREAD   = 0
	OWRITE  = 1
	ORDWR   = 2
	OEXEC   = 3 // execute, == read but check execute permission
	OTRUNC  = 0x10
	OCEXEC  = 0x20 // close on exec
	ORCLOSE = 0x40 // remove on close

	OMODE = 3
)

func (m OpenMode) IsReadOnly() bool  { return m&OMODE == OREAD }
func (m OpenMode) IsWriteOnly() bool { return m&OMODE == OWRITE }
func (m OpenMode) IsReadWrite() bool { return m&OMODE == ORDWR }

// IsReadOnly() || IsReadWrite()
func (m OpenMode) IsReadable() bool {
	return m.IsReadOnly() || m.IsReadWrite()
}

// IsWriteOnly() || IsReadWrite()
func (m OpenMode) IsWriteable() bool {
	return m.IsWriteOnly() || m.IsReadWrite()
}

func (m OpenMode) String() string {
	res := []string{}
	if m.IsReadOnly() {
		res = append(res, "OREAD")
	} else if m.IsWriteOnly() {
		res = append(res, "OWRITE")
	} else if m.IsReadWrite() {
		res = append(res, "ORDWR")
	}
	if m&OTRUNC != 0 {
		res = append(res, "OTRUNC")
	}
	if m&OCEXEC != 0 {
		res = append(res, "OCEXEC")
	}
	if m&ORCLOSE != 0 {
		res = append(res, "ORCLOSE")
	}
	return strings.Join(res, "|")
}

func (m OpenMode) ToOsFlag() int {
	var flags int
	if m.IsReadOnly() {
		flags |= os.O_RDONLY
	} else if m.IsWriteOnly() {
		flags |= os.O_WRONLY
	} else if m.IsReadWrite() {
		flags |= os.O_RDWR
	}
	if m&OTRUNC != 0 {
		flags |= os.O_TRUNC
	}
	return flags
}

type Mode uint32

const (
	M_DIR    = 0x80000000 // mode bit for directories
	M_APPEND = 0x40000000 // mode bit for append only files
	M_EXCL   = 0x20000000 // mode bit for exclusive use files
	M_MOUNT  = 0x10000000 // mode bit for mounted channel
	M_AUTH   = 0x08000000 // mode bit for authentication file
	M_TMP    = 0x04000000 // mode bit for non-backed-up file
	M_READ   = 0x4        // mode bit for read permission
	M_WRITE  = 0x2        // mode bit for write permission
	M_EXEC   = 0x1        // mode bit for execute permission

	// Mask for the type bits
	M_TYPE = M_DIR | M_APPEND | M_EXCL | M_MOUNT | M_TMP

	// Mask for the permissions bits
	M_PERM = 0777
)

func (m Mode) IsDir() bool { return m&M_DIR != 1 }

func (m Mode) String() string {
	res := []string{}
	perm := os.FileMode(m & M_PERM)
	res = append(res, perm.String())
	if m&M_DIR != 0 {
		res = append(res, "M_DIR")
	}
	if m&M_APPEND != 0 {
		res = append(res, "M_APPEND")
	}
	if m&M_EXCL != 0 {
		res = append(res, "M_EXCL")
	}
	if m&M_MOUNT != 0 {
		res = append(res, "M_MOUNT")
	}
	if m&M_AUTH != 0 {
		res = append(res, "M_AUTH")
	}
	if m&M_TMP != 0 {
		res = append(res, "M_TMP")
	}
	return strings.Join(res, "|")
}

// Convert to OS file flag with given OpenMode ORed in.
// Applies flags that are part of Mode (used for file creation)
func (m Mode) ToOsFlag(o OpenMode) int {
	flag := o.ToOsFlag()
	if m&M_EXCL != 0 {
		flag |= os.O_EXCL
	}
	if m&M_APPEND != 0 {
		flag |= os.O_APPEND
	}
	return flag
}

func (m Mode) ToOsMode() os.FileMode {
	var mode os.FileMode
	if m&M_DIR != 0 {
		mode = os.ModeDir
	}
	if m&M_APPEND != 0 {
		mode |= os.ModeAppend
	}
	if m&M_EXCL != 0 {
		mode |= os.ModeExclusive
	}
	if m&M_TMP != 0 {
		mode |= os.ModeTemporary
	}
	mode |= (os.FileMode(m) & os.ModePerm)
	return mode
}

func (m Mode) QidType() QidType {
	return QidType((m & M_TYPE) >> 24)
}

func versionFromFileInfo(info os.FileInfo) uint32 {
	if i, ok := info.(FileInfoVersion); ok {
		return i.Version()
	}
	return NoQidVersion
}

func ModeFromFileInfo(info os.FileInfo) Mode {
	if in, ok := info.(FileInfoMode9P); ok {
		return in.Mode9P()
	}
	return ModeFromOS(info.Mode())
}

func ModeFromOS(mode os.FileMode) Mode {
	var perm Mode
	if mode&os.ModeDir != 0 {
		perm |= M_DIR
	}
	if mode&os.ModeAppend != 0 {
		perm |= M_APPEND
	}
	if mode&os.ModeExclusive != 0 {
		perm |= M_EXCL
	}
	if mode&os.ModeTemporary != 0 {
		perm |= M_TMP
	}
	return perm | Mode(mode.Perm())
}

var bo = binary.LittleEndian

/////////////////////////////////////

type Tag uint16

/////////////////////////////////////

type MsgBase []byte

func (r MsgBase) fill(mt MsgType, t Tag, size uint32) {
	bo.PutUint32(r[:4], size)       // Size
	r[4] = byte(mt)                 // MsgType
	bo.PutUint16(r[5:7], uint16(t)) // Tag
}

func (r MsgBase) Bytes() []byte { return r[:int(r.Size())] }
func (r MsgBase) Size() uint32  { return bo.Uint32(r[:4]) }
func (r MsgBase) Type() MsgType { return MsgType(r[4]) }
func (r MsgBase) Tag() Tag      { return Tag(bo.Uint16(r[5:7])) }

const msgOffset = 7

/////////////////////////////////////
type msgString []byte

const maxStringLen = math.MaxUint16

func (s msgString) Len() uint16     { return bo.Uint16(s[0:2]) }
func (s msgString) SetLen(v uint16) { bo.PutUint16(s[0:2], v) }
func (s msgString) Bytes() []byte {
	return s[2 : s.Len()+2]
}
func (s msgString) SetBytes(b []byte) { copy(s[2:s.Len()+2], b) }
func (s msgString) SetBytesAndLen(b []byte) {
	s.SetLen(uint16(len(b)))
	copy(s[2:len(b)+2], b)
}
func (s msgString) String() string     { return string(s.Bytes()) }
func (s msgString) SetString(v string) { s.SetBytes([]byte(v)) }
func (s msgString) SetStringAndLen(v string) int {
	s.SetBytesAndLen([]byte(v))
	return 2 + len((v))
}
func (s msgString) Nbytes() int { return int(s.Len()) + 2 }

/////////////////////////////////////

type Fid uint32 // always size 4

const MAX_FID = math.MaxUint32 - 2

func (f Fid) String() string {
	return fmt.Sprintf("Fid(%d)", f)
}

/////////////////////////////////////

type FidSlice []Fid

func (s FidSlice) Len() int           { return len(s) }
func (s FidSlice) Less(i, j int) bool { return s[i] < s[j] }
func (s FidSlice) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }

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

func (qt QidType) IsDir() bool       { return qt&QT_DIR != 0 }
func (qt QidType) IsSymLink() bool   { return qt&QT_SYMLINK != 0 }
func (qt QidType) IsAuth() bool      { return qt&QT_AUTH != 0 }
func (qt QidType) IsMount() bool     { return qt&QT_MOUNT != 0 }
func (qt QidType) IsExclusive() bool { return qt&QT_EXCL != 0 }
func (qt QidType) IsTemporary() bool { return qt&QT_TMP != 0 }

func (qt QidType) Mode() Mode {
	var m Mode
	if qt == QT_FILE {
	}
	return m
}

func (qt QidType) String() string {
	parts := []string{}
	if qt == QT_FILE {
		parts = append(parts, "QT_FILE")
	}
	if qt&QT_LINK != 0 {
		parts = append(parts, "QT_LINK")
	}
	if qt&QT_SYMLINK != 0 {
		parts = append(parts, "QT_SYMLINK")
	}
	if qt&QT_TMP != 0 {
		parts = append(parts, "QT_TMP")
	}
	if qt&QT_AUTH != 0 {
		parts = append(parts, "QT_AUTH")
	}
	if qt&QT_MOUNT != 0 {
		parts = append(parts, "QT_MOUNT")
	}
	if qt&QT_EXCL != 0 {
		parts = append(parts, "QT_EXCL")
	}
	if qt&QT_DIR != 0 {
		parts = append(parts, "QT_DIR")
	}
	return strings.Join(parts, "|")
}

const QidSize = 13

type Qid []byte // always size 13

var NoTouchQid Qid

func init() {
	NoTouchQid = NewQid()
	for i := range NoTouchQid {
		NoTouchQid[i] = 0xff
	}
}

func NewQid() Qid {
	return Qid(make([]byte, QidSize))
}

func (q Qid) Fill(t QidType, version uint32, path uint64) Qid {
	q[0] = byte(t)
	bo.PutUint32(q[1:5], version)
	bo.PutUint64(q[5:13], path)
	return q
}

// qid.type[1] the type of the file (directory, etc.), represented as a bit vector corresponding to the high 8 bits of the file's mode word.
// qid.vers[4] version number for given path
// qid.path[8] the file server's unique identification for the file

func (q Qid) Bytes() []byte       { return q[:QidSize] }
func (q Qid) Type() QidType       { return QidType(q[0]) }
func (q Qid) Version() uint32     { return bo.Uint32(q[1:5]) }
func (q Qid) SetVersion(v uint32) { bo.PutUint32(q[1:5], v) }
func (q Qid) Path() uint64        { return bo.Uint64(q[5 : 5+8]) }
func (q Qid) SetPath(v uint64)    { bo.PutUint64(q[5:5+8], v) } // not recommended to use unless you know the impact of this
func (q Qid) IsNoTouch() bool {
	for _, v := range q.Bytes() {
		if v != 0xff {
			return false
		}
	}
	return true
}

func (q Qid) Clone() Qid {
	qid := make(Qid, len(q))
	copy(qid, q)
	return qid
}

func (q Qid) String() string {
	return fmt.Sprintf("Qid{ type: %s, version: %v, path: %v }", q.Type(), q.Version(), q.Path())
}

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

const (
	minStatSize = 2 + 2 + 4 + 13 + 4 + 4 + 4 + 8 + 4*2
	maxStatSize = minStatSize + maxStringLen*4
)

func statSize(name, uid, gid, muid string) int {
	return minStatSize + len(name) + len(uid) + len(gid) + len(muid)
}

func (s Stat) fill(name, uid, gid, muid string) {
	if len(name) > maxStringLen {
		panic(fmt.Errorf("name is too large (%d > %d)", len(name), maxStringLen))
	}
	if len(uid) > maxStringLen {
		panic(fmt.Errorf("uid is too large (%d > %d)", len(uid), maxStringLen))
	}
	if len(gid) > maxStringLen {
		panic(fmt.Errorf("gid is too large (%d > %d)", len(gid), maxStringLen))
	}
	if len(muid) > maxStringLen {
		panic(fmt.Errorf("muid is too large (%d > %d)", len(muid), maxStringLen))
	}
	size := statSize(name, uid, gid, muid)
	if size > len(s) {
		panic(fmt.Errorf("Stat is not large enough for size (%d >= %d)", size, len(s)))
	}
	s.SetSize(uint16(size - 2))
	b := s[41:]
	{
		bo.PutUint16(b, uint16(len(name)))
		b = b[2:]
		b = b[copy(b, name):]
	}
	{
		bo.PutUint16(b, uint16(len(uid)))
		b = b[2:]
		b = b[copy(b, uid):]
	}
	{
		bo.PutUint16(b, uint16(len(gid)))
		b = b[2:]
		b = b[copy(b, gid):]
	}
	{
		bo.PutUint16(b, uint16(len(muid)))
		b = b[2:]
		b = b[copy(b, muid):]
	}
	// s.name().SetStringAndLen(name)
	// s.uid().SetStringAndLen(uid)
	// s.gid().SetStringAndLen(gid)
	// s.muid().SetStringAndLen(muid)
}

// An way to product a stat from a file info.
//
// Note that Stat is a richer type that os.FileInfo, so the returned Stat may
// be potentially inaccurate.
func StatFromFileInfo(info os.FileInfo) Stat {
	if st, ok := info.Sys().(Stat); ok {
		return st
	}
	qt := QidType(0)
	m := info.Mode()
	if m&os.ModeDir != 0 {
		qt |= QT_DIR
	}
	if m&os.ModeExclusive != 0 {
		qt |= QT_EXCL
	}
	if m&os.ModeSymlink != 0 {
		qt |= QT_SYMLINK
	}
	if m&os.ModeTemporary != 0 {
		qt |= QT_TMP
	}
	qid := NewQid()
	qid.Fill(qt, 0, 0)
	return fileInfoToStat(qid, info)
}

// Useful for creating a WriteStat stat that only wants fsync.
//
// Due to the 9p2000 spec, SyncStat is the "zero-value" of Stat. Non-string
// fields can be modified to only request that field to be modified.
func SyncStat() Stat {
	st := NewStat("", "", "", "")
	st.SetType(NoTouchU16)
	st.SetDev(NoTouchU32)
	st.SetQid(NoTouchQid)
	st.SetMode(NoTouchMode)
	st.SetAtime(NoTouchU32)
	st.SetMtime(NoTouchU32)
	st.SetLength(NoTouchU64)
	return st
}

// Like SyncStat(), but allows setting the file name.
func SyncStatWithName(name string) Stat {
	st := NewStat(name, "", "", "")
	st.SetType(NoTouchU16)
	st.SetDev(NoTouchU32)
	st.SetQid(NoTouchQid)
	st.SetMode(NoTouchMode)
	st.SetAtime(NoTouchU32)
	st.SetMtime(NoTouchU32)
	st.SetLength(NoTouchU64)
	return st
}

func NewStat(name, uid, gid, muid string) Stat {
	// TODO: error if strings are too large
	size := statSize(name, uid, gid, muid)
	s := Stat(make([]byte, size))
	s.fill(name, uid, gid, muid)
	return s
}

func (s Stat) CopyFixedFieldsFrom(o Stat) {
	s.SetSize(o.Size())
	s.SetType(o.Type())
	s.SetDev(o.Dev())
	s.SetQid(o.Qid())
	s.SetMode(o.Mode())
	s.SetAtime(o.Atime())
	s.SetMtime(o.Mtime())
	s.SetLength(o.Length())
}

func (s Stat) Nbytes() int   { return int(s.Size() + 2) }
func (s Stat) Bytes() []byte { return s[:s.Size()+2] }

func (s Stat) String() string {
	return fmt.Sprintf(
		"Stat{\n\tSize: %#v,\n\tQid: %#v,\n\tMode: %#v,\n\tAtime: %#v,\n\tMtime: %#v,\n\tLength: %#v,\n\tName: %#v,\n\tUid: %#v,\n\tGid: %#v,\n\tMuid: %#v,\n\tType: %#v,\n\tDev: %#v,\n} => RAW: %#v",
		s.Size(),
		s.Qid(),
		s.Mode(),
		s.Mtime(),
		s.Atime(),
		s.Length(),
		s.Name(),
		s.Uid(),
		s.Gid(),
		s.Muid(),
		s.Type(),
		s.Dev(),
		s.Bytes(),
	)
}

func (s Stat) Size() uint16     { return bo.Uint16(s[:2]) }
func (s Stat) SetSize(v uint16) { bo.PutUint16(s[:2], v) }

func (s Stat) TypeNoTouch() bool { return s.Type() == NoTouchU16 }
func (s Stat) Type() uint16      { return bo.Uint16(s[2:4]) }
func (s Stat) SetType(v uint16)  { bo.PutUint16(s[2:4], v) }

func (s Stat) DevNoTouch() bool { return s.Dev() == NoTouchU32 }
func (s Stat) Dev() uint32      { return bo.Uint32(s[4:8]) }
func (s Stat) SetDev(v uint32)  { bo.PutUint32(s[4:8], v) }

func (s Stat) Qid() Qid     { return Qid(s[8 : 8+QidSize]) }
func (s Stat) SetQid(v Qid) { copy(s[8:8+QidSize], v.Bytes()) }

func (s Stat) ModeNoTouch() bool { return s.Mode() == NoTouchMode }
func (s Stat) Mode() Mode        { return Mode(bo.Uint32(s[8+QidSize : 8+QidSize+4])) }
func (s Stat) SetMode(v Mode)    { bo.PutUint32(s[8+QidSize:8+QidSize+4], uint32(v)) }

func (s Stat) AtimeNoTouch() bool { return s.Atime() == NoTouchU32 }
func (s Stat) Atime() uint32      { return bo.Uint32(s[8+QidSize+4 : 8+QidSize+4+4]) }
func (s Stat) SetAtime(v uint32)  { bo.PutUint32(s[8+QidSize+4:8+QidSize+4+4], v) }

func (s Stat) MtimeNoTouch() bool { return s.Mtime() == NoTouchU32 }
func (s Stat) Mtime() uint32      { return bo.Uint32(s[8+QidSize+4+4 : 8+QidSize+4+4+4]) }
func (s Stat) SetMtime(v uint32)  { bo.PutUint32(s[8+QidSize+4+4:8+QidSize+4+4+4], v) }

func (s Stat) LengthNoTouch() bool { return s.Length() == NoTouchU64 }
func (s Stat) Length() uint64      { return bo.Uint64(s[8+QidSize+4+4+4 : 8+QidSize+4+4+4+8]) }
func (s Stat) SetLength(v uint64)  { bo.PutUint64(s[8+QidSize+4+4+4:8+QidSize+4+4+4+8], v) }

func (s Stat) name() msgString   { return msgString(s[41:]) }
func (s Stat) NameNoTouch() bool { return len(s.NameBytes()) == 0 }
func (s Stat) NameBytes() []byte { return s.name().Bytes() }
func (s Stat) Name() string      { return s.name().String() }

func (s Stat) uid() msgString   { return msgString(s[41+s.name().Nbytes():]) }
func (s Stat) UidNoTouch() bool { return len(s.UidBytes()) == 0 }
func (s Stat) UidBytes() []byte { return s.uid().Bytes() }
func (s Stat) Uid() string      { return s.uid().String() }

func (s Stat) gid() msgString   { return msgString(s[41+s.name().Nbytes()+s.uid().Nbytes():]) }
func (s Stat) GidNoTouch() bool { return len(s.GidBytes()) == 0 }
func (s Stat) GidBytes() []byte { return s.gid().Bytes() }
func (s Stat) Gid() string      { return s.gid().String() }

func (s Stat) muid() msgString {
	return msgString(s[41+s.name().Nbytes()+s.uid().Nbytes()+s.gid().Nbytes():])
}
func (s Stat) MuidNoTouch() bool { return len(s.MuidBytes()) == 0 }
func (s Stat) MuidBytes() []byte { return s.muid().Bytes() }
func (s Stat) Muid() string      { return s.muid().String() }

func (s Stat) IsZero() bool {
	for _, v := range s.Bytes() {
		if v != 0 {
			return false
		}
	}
	return true
}

func (s Stat) FileInfo() StatFileInfo { return StatFileInfo{s} }
func (s Stat) Clone() Stat {
	st := make(Stat, len(s))
	copy(st, s)
	return st
}

func (s Stat) fileUsers() (uid, gid, muid string, err error) {
	return s.Uid(), s.Gid(), s.Muid(), nil
}

// os.FileInfo interface

type StatFileInfo struct {
	Stat
}

func (s StatFileInfo) Size() int64        { return int64(s.Stat.Length()) }
func (s StatFileInfo) Name() string       { return s.Stat.Name() }
func (s StatFileInfo) Mode() os.FileMode  { return s.Stat.Mode().ToOsMode() }
func (s StatFileInfo) ModTime() time.Time { return time.Unix(int64(s.Stat.Mtime()), 0) }
func (s StatFileInfo) IsDir() bool        { return s.Stat.Mode()&M_DIR != 0 }
func (s StatFileInfo) Sys() interface{}   { return s.Stat }

func FileInfosFromStats(infos []Stat) []os.FileInfo {
	sfi := make([]os.FileInfo, len(infos))
	for i, info := range infos {
		sfi[i] = info.FileInfo()
	}
	return sfi
}

/////////////////////////////////////
// size[4] Tversion tag[2] msize[4] version[s]
type Tversion []byte

func (r Tversion) fill(t Tag, maxMessageSize uint32, version string) {
	verSize := len(version)
	size := uint32(msgOffset + 4 + 2 + verSize)
	MsgBase(r).fill(msgTversion, t, size)
	bo.PutUint32(r[msgOffset:msgOffset+4], maxMessageSize)
	msgString(r[msgOffset+4:]).SetStringAndLen(version)
}

func (r Tversion) Bytes() []byte   { return MsgBase(r).Bytes() }
func (r Tversion) Size() uint32    { return MsgBase(r).Size() }
func (r Tversion) Tag() Tag        { return MsgBase(r).Tag() }
func (r Tversion) MsgSize() uint32 { return bo.Uint32(r[msgOffset : msgOffset+4]) }

func (r Tversion) version() msgString   { return msgString(r[msgOffset+4:]) }
func (r Tversion) VersionBytes() []byte { return r.version().Bytes() }
func (r Tversion) Version() string      { return r.version().String() }

/////////////////////////////////////
// size[4] Rversion tag[2] msize[4] version[s]
type Rversion []byte

func (r Rversion) fill(t Tag, maxMessageSize uint32, version string) {
	MsgBase(r).fill(msgRversion, t, uint32(msgOffset+4+2+len((version))))
	bo.PutUint32(r[msgOffset:msgOffset+4], maxMessageSize)
	msgString(r[msgOffset+4:]).SetStringAndLen(version)
}

func (r Rversion) Bytes() []byte   { return MsgBase(r).Bytes() }
func (r Rversion) Size() uint32    { return MsgBase(r).Size() }
func (r Rversion) Tag() Tag        { return MsgBase(r).Tag() }
func (r Rversion) MsgSize() uint32 { return bo.Uint32(r[msgOffset : msgOffset+4]) }

func (r Rversion) version() msgString   { return msgString(r[msgOffset+4:]) }
func (r Rversion) VersionBytes() []byte { return r.version().Bytes() }
func (r Rversion) Version() string      { return r.version().String() }

/////////////////////////////////////
//size[4] Tauth tag[2] afid[4] name[s] aname[s]
type Tauth []byte

func (r Tauth) fill(t Tag, afid Fid, name, aname string) {
	MsgBase(r).fill(msgTauth, t, uint32(msgOffset+4+2*2+len((name))+len((aname))))
	bo.PutUint32(r[msgOffset:msgOffset+4], uint32(afid))
	next := msgString(r[msgOffset+4:]).SetStringAndLen(name)
	msgString(r[next:]).SetStringAndLen(aname)
}

func (r Tauth) Bytes() []byte { return MsgBase(r).Bytes() }
func (r Tauth) Size() uint32  { return MsgBase(r).Size() }
func (r Tauth) Tag() Tag      { return MsgBase(r).Tag() }
func (r Tauth) Afid() Fid     { return Fid(bo.Uint32(r[msgOffset : msgOffset+4])) }
func (r Tauth) SetAfid(v Fid) { bo.PutUint32(r[msgOffset:msgOffset+4], uint32(v)) }

func (r Tauth) uname() msgString   { return msgString(r[msgOffset+4:]) }
func (r Tauth) UnameBytes() []byte { return r.uname().Bytes() }
func (r Tauth) Uname() string      { return r.uname().String() }

func (r Tauth) aname() msgString   { return msgString(r[msgOffset+4+r.uname().Nbytes():]) }
func (r Tauth) AnameBytes() []byte { return r.aname().Bytes() }
func (r Tauth) Aname() string      { return r.aname().String() }

/////////////////////////////////////
// size[4] Rauth tag[2] aqid[13]
type Rauth []byte

func (r Rauth) fill(t Tag, aqid Qid) {
	MsgBase(r).fill(msgRauth, t, uint32(msgOffset+QidSize))
	copy(r[msgOffset:msgOffset+QidSize], aqid)
}

func (r Rauth) Bytes() []byte { return MsgBase(r).Bytes() }
func (r Rauth) Size() uint32  { return MsgBase(r).Size() }
func (r Rauth) Tag() Tag      { return MsgBase(r).Tag() }
func (r Rauth) Aqid() Qid     { return Qid(r[msgOffset : msgOffset+QidSize]) }

/////////////////////////////////////
// size[4] Rerror tag[2] ename[s]
type Rerror []byte

func (r Rerror) fill(t Tag, msg string) {
	MsgBase(r).fill(msgRerror, t, uint32(msgOffset+2+len((msg))))
	msgString(r[msgOffset:]).SetStringAndLen(msg)
}

func (r Rerror) Bytes() []byte      { return MsgBase(r).Bytes() }
func (r Rerror) Size() uint32       { return MsgBase(r).Size() }
func (r Rerror) Tag() Tag           { return MsgBase(r).Tag() }
func (r Rerror) ename() msgString   { return msgString(r[msgOffset:]) }
func (r Rerror) EnameBytes() []byte { return r.ename().Bytes() }
func (r Rerror) Ename() string      { return r.ename().String() }

var mappedErrors []error = []error{
	os.ErrInvalid,
	os.ErrPermission,
	os.ErrExist,
	os.ErrNotExist,
	os.ErrClosed,
	os.ErrNoDeadline,
	io.EOF,
	io.ErrClosedPipe,
	io.ErrNoProgress,
	io.ErrShortBuffer,
	io.ErrShortWrite,
	io.ErrUnexpectedEOF,

	ErrBadFormat,
	ErrWriteNotAllowed,
	ErrReadNotAllowed,
	ErrSeekNotAllowed,
	ErrUnsupported,
	ErrNotImplemented,
	ErrInvalidAccess,
	ErrChangeUidNotAllowed,
	ErrMissingIterator,
}

func (r Rerror) Error() error {
	var underlyingErr error
	msg := r.Ename()
	// we want to preserve equality of errors to native os-styled errors
	for _, e := range mappedErrors {
		if msg == e.Error() {
			underlyingErr = e
			break
		}
	}
	// else
	return &RerrorType{msg, underlyingErr}
}

type RerrorType struct {
	msg string
	e   error
}

func (e RerrorType) Error() string { return e.msg }
func (e RerrorType) Unwrap() error { return e.e }

/////////////////////////////////////
// size[4] Tclunk tag[2] fid[4]
type Tclunk []byte

func (r Tclunk) fill(t Tag, fid Fid) {
	MsgBase(r).fill(msgTclunk, t, uint32(msgOffset+4))
	bo.PutUint32(r[msgOffset:msgOffset+4], uint32(fid))
}

func (r Tclunk) Bytes() []byte { return MsgBase(r).Bytes() }
func (r Tclunk) Size() uint32  { return MsgBase(r).Size() }
func (r Tclunk) Tag() Tag      { return MsgBase(r).Tag() }
func (r Tclunk) Fid() Fid      { return Fid(bo.Uint32(r[msgOffset : msgOffset+4])) }
func (r Tclunk) SetFid(v Fid)  { bo.PutUint32(r[msgOffset:msgOffset+4], uint32(v)) }

/////////////////////////////////////
// size[4] Rclunk tag[2]
type Rclunk []byte

func (r Rclunk) fill(t Tag) {
	MsgBase(r).fill(msgRclunk, t, uint32(msgOffset))
}

func (r Rclunk) Bytes() []byte { return MsgBase(r).Bytes() }
func (r Rclunk) Size() uint32  { return MsgBase(r).Size() }
func (r Rclunk) Tag() Tag      { return MsgBase(r).Tag() }

/////////////////////////////////////
// size[4] Tflush tag[2] oldtag[2]
type Tflush []byte

func (r Tflush) fill(t Tag, oldTag Tag) {
	MsgBase(r).fill(msgTflush, t, uint32(msgOffset+2))
	bo.PutUint16(r[msgOffset:msgOffset+2], uint16(oldTag))
}

func (r Tflush) Bytes() []byte { return MsgBase(r).Bytes() }
func (r Tflush) Size() uint32  { return MsgBase(r).Size() }
func (r Tflush) Tag() Tag      { return MsgBase(r).Tag() }
func (r Tflush) OldTag() Tag   { return Tag(bo.Uint16(r[msgOffset : msgOffset+2])) }

/////////////////////////////////////
// size[4] Rflush tag[2]
type Rflush []byte

func (r Rflush) fill(t Tag) {
	MsgBase(r).fill(msgRflush, t, uint32(msgOffset))
}

func (r Rflush) Bytes() []byte { return MsgBase(r).Bytes() }
func (r Rflush) Size() uint32  { return MsgBase(r).Size() }
func (r Rflush) Tag() Tag      { return MsgBase(r).Tag() }

/////////////////////////////////////
// size[4] Twalk tag[2] fid[4] newfid[4] nwname[2] nwname*(wname[s])
type Twalk []byte

// From docs:
// "To simplify the implementation of the servers, a maximum of sixteen name
// elements or qids may be packed in a single message. This constant is called
// MAXWELEM in fcall(3). Despite this restriction, the system imposes no limit
// on the number of elements in a file name, only the number that may be
// transmitted in a single message."
const MAXWELEM = 16

func (r Twalk) fill(t Tag, fid, newfid Fid, wnames []string) {
	size := uint32(msgOffset + 4 + 4 + 2 + 2*len(wnames))
	for _, n := range wnames {
		size += uint32(len((n)))
	}
	MsgBase(r).fill(msgTwalk, t, size)
	bo.PutUint32(r[msgOffset:msgOffset+4], uint32(fid))
	bo.PutUint32(r[msgOffset+4:msgOffset+8], uint32(newfid))
	bo.PutUint16(r[msgOffset+8:msgOffset+10], uint16(len(wnames)))
	off := msgOffset + 10
	for _, n := range wnames {
		off += msgString(r[off:]).SetStringAndLen(n)
	}
}

func (r Twalk) Bytes() []byte { return MsgBase(r).Bytes() }
func (r Twalk) Size() uint32  { return MsgBase(r).Size() }
func (r Twalk) Tag() Tag      { return MsgBase(r).Tag() }
func (r Twalk) Fid() Fid      { return Fid(bo.Uint32(r[msgOffset : msgOffset+4])) }
func (r Twalk) SetFid(v Fid)  { bo.PutUint32(r[msgOffset:msgOffset+4], uint32(v)) }

func (r Twalk) NewFid() Fid     { return Fid(bo.Uint32(r[msgOffset+4 : msgOffset+8])) }
func (r Twalk) SetNewFid(v Fid) { bo.PutUint32(r[msgOffset+4:msgOffset+8], uint32(v)) }

func (r Twalk) NumWname() uint16 { return bo.Uint16(r[msgOffset+8 : msgOffset+10]) }

func (r Twalk) wname(i int) msgString {
	if i >= int(r.NumWname()) {
		panic("Out of bounds")
	}
	off := msgOffset + 10
	for j := 0; j < i; j++ {
		off += msgString(r[off:]).Nbytes()
	}
	return msgString(r[off:])
}
func (r Twalk) Wname(i int) string { return r.wname(i).String() }
func (r Twalk) Wnames() []string {
	names := make([]string, 0, 16)
	off := msgOffset + 10
	size := int(r.NumWname())
	for j := 0; j < size; j++ {
		mstr := msgString(r[off:])
		names = append(names, mstr.String())
		off += mstr.Nbytes()
	}
	return names
}

/////////////////////////////////////
// size[4] Rwalk tag[2] nwqid[2] nwqid*(wqid[13])
type Rwalk []byte

func (r Rwalk) fill(t Tag, wqids []Qid) {
	size := uint32(msgOffset + 2 + len(wqids)*QidSize)
	MsgBase(r).fill(msgRwalk, t, size)
	bo.PutUint16(r[msgOffset:msgOffset+2], uint16(len(wqids)))
	off := msgOffset + 2
	for i, wqid := range wqids {
		o := off + i*QidSize
		copy(r[o:o+QidSize], wqid.Bytes())
	}
}

func (r Rwalk) Bytes() []byte   { return MsgBase(r).Bytes() }
func (r Rwalk) Size() uint32    { return MsgBase(r).Size() }
func (r Rwalk) Tag() Tag        { return MsgBase(r).Tag() }
func (r Rwalk) NumWqid() uint16 { return bo.Uint16(r[msgOffset : msgOffset+2]) }
func (r Rwalk) Wqid(i int) Qid {
	off := msgOffset + 2 + i*QidSize
	return Qid(r[off : off+QidSize])
}

/////////////////////////////////////
// size[4] Tattach tag[2] fid[4] afid[4] uname[s] aname[s]
type Tattach []byte

func (r Tattach) fill(t Tag, fid, afid Fid, uname, aname string) {
	size := uint32(msgOffset + 4 + 4 + 2 + len((uname)) + 2 + len((aname)))
	MsgBase(r).fill(msgTattach, t, size)
	bo.PutUint32(r[msgOffset:msgOffset+4], uint32(fid))
	bo.PutUint32(r[msgOffset+4:msgOffset+8], uint32(afid))
	off := msgString(r[msgOffset+8:]).SetStringAndLen(uname)
	msgString(r[msgOffset+8+off:]).SetStringAndLen(aname)
}

func (r Tattach) Bytes() []byte { return MsgBase(r).Bytes() }
func (r Tattach) Size() uint32  { return MsgBase(r).Size() }
func (r Tattach) Tag() Tag      { return MsgBase(r).Tag() }
func (r Tattach) Fid() Fid      { return Fid(bo.Uint32(r[msgOffset : msgOffset+4])) }
func (r Tattach) SetFid(v Fid)  { bo.PutUint32(r[msgOffset:msgOffset+4], uint32(v)) }
func (r Tattach) Afid() Fid     { return Fid(bo.Uint32(r[msgOffset+4 : msgOffset+8])) }
func (r Tattach) SetAfid(v Fid) { bo.PutUint32(r[msgOffset+4:msgOffset+8], uint32(v)) }

func (r Tattach) uname() msgString   { return msgString(r[msgOffset+8:]) }
func (r Tattach) UnameBytes() []byte { return r.uname().Bytes() }
func (r Tattach) Uname() string      { return r.uname().String() }

func (r Tattach) aname() msgString   { return msgString(r[msgOffset+8+r.uname().Nbytes():]) }
func (r Tattach) AnameBytes() []byte { return r.aname().Bytes() }
func (r Tattach) Aname() string      { return r.aname().String() }

/////////////////////////////////////
// size[4] Rattach tag[2] qid[13]
type Rattach []byte

func (r Rattach) fill(t Tag, qid Qid) {
	size := uint32(msgOffset + QidSize)
	MsgBase(r).fill(msgRattach, t, size)
	copy(r[msgOffset:msgOffset+QidSize], qid.Bytes())
}

func (r Rattach) Bytes() []byte { return MsgBase(r).Bytes() }
func (r Rattach) Size() uint32  { return MsgBase(r).Size() }
func (r Rattach) Tag() Tag      { return MsgBase(r).Tag() }
func (r Rattach) Qid() Qid      { return Qid(r[msgOffset : msgOffset+QidSize]) }

/////////////////////////////////////
// size[4] Topen tag[2] fid[4] mode[1]
type Topen []byte

func (r Topen) fill(t Tag, fid Fid, mode OpenMode) {
	size := uint32(msgOffset + 4 + 1)
	MsgBase(r).fill(msgTopen, t, size)
	bo.PutUint32(r[msgOffset:msgOffset+4], uint32(fid))
	r[msgOffset+4] = byte(mode)
}

func (r Topen) Bytes() []byte  { return MsgBase(r).Bytes() }
func (r Topen) Size() uint32   { return MsgBase(r).Size() }
func (r Topen) Tag() Tag       { return MsgBase(r).Tag() }
func (r Topen) Fid() Fid       { return Fid(bo.Uint32(r[msgOffset : msgOffset+4])) }
func (r Topen) SetFid(v Fid)   { bo.PutUint32(r[msgOffset:msgOffset+4], uint32(v)) }
func (r Topen) Mode() OpenMode { return OpenMode(r[msgOffset+4]) }

/////////////////////////////////////
// size[4] Ropen tag[2] qid[13] iounit[4]
type Ropen []byte

func (r Ropen) fill(t Tag, qid Qid, iounit uint32) {
	size := uint32(msgOffset + QidSize + 4)
	MsgBase(r).fill(msgRopen, t, size)
	copy(r[msgOffset:msgOffset+QidSize], qid.Bytes())
	bo.PutUint32(r[msgOffset+QidSize:msgOffset+QidSize+4], iounit)
}

func (r Ropen) Bytes() []byte { return MsgBase(r).Bytes() }
func (r Ropen) Size() uint32  { return MsgBase(r).Size() }
func (r Ropen) Tag() Tag      { return MsgBase(r).Tag() }
func (r Ropen) Qid() Qid      { return Qid(r[msgOffset : msgOffset+QidSize]) }
func (r Ropen) Iounit() uint32 {
	return bo.Uint32(r[msgOffset+QidSize : msgOffset+QidSize+4])
}

/////////////////////////////////////
// size[4] Tcreate tag[2] fid[4] name[s] perm[4] mode[1]
type Tcreate []byte

func (r Tcreate) fill(t Tag, fid Fid, name string, perm uint32, mode OpenMode) {
	size := uint32(msgOffset + 4 + 2 + len((name)) + 4 + 1)
	MsgBase(r).fill(msgTcreate, t, size)
	bo.PutUint32(r[msgOffset:msgOffset+4], uint32(fid))
	off := msgString(r[msgOffset+4:]).SetStringAndLen(name)
	bo.PutUint32(r[msgOffset+4+off:msgOffset+4+off+4], perm)
	r[msgOffset+4+off+4] = byte(mode)
}

func (r Tcreate) Bytes() []byte { return MsgBase(r).Bytes() }
func (r Tcreate) Size() uint32  { return MsgBase(r).Size() }
func (r Tcreate) Tag() Tag      { return MsgBase(r).Tag() }
func (r Tcreate) Fid() Fid      { return Fid(bo.Uint32(r[msgOffset : msgOffset+4])) }
func (r Tcreate) SetFid(v Fid)  { bo.PutUint32(r[msgOffset:msgOffset+4], uint32(v)) }

func (r Tcreate) name() msgString   { return msgString(r[msgOffset+4:]) }
func (r Tcreate) NameBytes() []byte { return r.name().Bytes() }
func (r Tcreate) Name() string      { return r.name().String() }

func (r Tcreate) Perm() Mode {
	o := msgOffset + 4 + r.name().Nbytes()
	return Mode(bo.Uint32(r[o : o+4]))
}
func (r Tcreate) Mode() OpenMode { return OpenMode(r[msgOffset+4+r.name().Nbytes()+4]) }

/////////////////////////////////////
// size[4] Rcreate tag[2] qid[13] iounit[4]
type Rcreate []byte

func (r Rcreate) fill(t Tag, q Qid, iounit uint32) {
	size := uint32(msgOffset + QidSize + 4)
	MsgBase(r).fill(msgRcreate, t, size)
	copy(r[msgOffset:msgOffset+QidSize], q.Bytes())
	bo.PutUint32(r[msgOffset+QidSize:msgOffset+QidSize+4], iounit)
}

func (r Rcreate) Bytes() []byte { return MsgBase(r).Bytes() }
func (r Rcreate) Size() uint32  { return MsgBase(r).Size() }
func (r Rcreate) Tag() Tag      { return MsgBase(r).Tag() }
func (r Rcreate) Qid() Qid      { return Qid(r[msgOffset : msgOffset+QidSize]) }
func (r Rcreate) Iounit() uint32 {
	return bo.Uint32(r[msgOffset+QidSize : msgOffset+QidSize+4])
}

/////////////////////////////////////
// size[4] Tread tag[2] fid[4] offset[8] count[4]
type Tread []byte

func (r Tread) fill(t Tag, fid Fid, offset uint64, count uint32) {
	size := uint32(msgOffset + 4 + 8 + 4)
	MsgBase(r).fill(msgTread, t, size)
	bo.PutUint32(r[msgOffset:msgOffset+4], uint32(fid))
	bo.PutUint64(r[msgOffset+4:msgOffset+12], offset)
	bo.PutUint32(r[msgOffset+12:msgOffset+16], count)
}

func (r Tread) Bytes() []byte { return MsgBase(r).Bytes() }
func (r Tread) Size() uint32  { return MsgBase(r).Size() }
func (r Tread) Tag() Tag      { return MsgBase(r).Tag() }
func (r Tread) Fid() Fid      { return Fid(bo.Uint32(r[msgOffset : msgOffset+4])) }
func (r Tread) SetFid(v Fid)  { bo.PutUint32(r[msgOffset:msgOffset+4], uint32(v)) }

func (r Tread) Offset() uint64     { return bo.Uint64(r[msgOffset+4 : msgOffset+12]) }
func (r Tread) SetOffset(v uint64) { bo.PutUint64(r[msgOffset+4:msgOffset+12], v) }

func (r Tread) Count() uint32     { return bo.Uint32(r[msgOffset+12 : msgOffset+16]) }
func (r Tread) SetCount(v uint32) { bo.PutUint32(r[msgOffset+12:msgOffset+16], v) }

/////////////////////////////////////
// size[4] Rread tag[2] count[4] data[count]
type Rread []byte

func (r Rread) fill(t Tag, count uint32) {
	size := uint32(msgOffset + 4 + count)
	MsgBase(r).fill(msgRread, t, size)
	bo.PutUint32(r[msgOffset:msgOffset+4], count)
	// copy(r[msgOffset+4:], data)
}

func (r Rread) Bytes() []byte       { return MsgBase(r).Bytes() }
func (r Rread) Size() uint32        { return MsgBase(r).Size() }
func (r Rread) Tag() Tag            { return MsgBase(r).Tag() }
func (r Rread) Count() uint32       { return bo.Uint32(r[msgOffset : msgOffset+4]) }
func (r Rread) Data() []byte        { return r[msgOffset+4 : msgOffset+4+r.Count()] }
func (r Rread) DataNoLimit() []byte { return r[msgOffset+4:] }

/////////////////////////////////////
// size[4] Twrite tag[2] fid[4] offset[8] count[4] data[count]
type Twrite []byte

func (r Twrite) fill(t Tag, fid Fid, offset uint64, count uint32) {
	size := uint32(msgOffset + 4 + 8 + 4 + count)
	MsgBase(r).fill(msgTwrite, t, size)
	bo.PutUint32(r[msgOffset:msgOffset+4], uint32(fid))
	bo.PutUint64(r[msgOffset+4:msgOffset+12], offset)
	bo.PutUint32(r[msgOffset+12:msgOffset+16], count)
}

func (r Twrite) Bytes() []byte  { return MsgBase(r).Bytes() }
func (r Twrite) Size() uint32   { return MsgBase(r).Size() }
func (r Twrite) Tag() Tag       { return MsgBase(r).Tag() }
func (r Twrite) Fid() Fid       { return Fid(bo.Uint32(r[msgOffset : msgOffset+4])) }
func (r Twrite) SetFid(v Fid)   { bo.PutUint32(r[msgOffset:msgOffset+4], uint32(v)) }
func (r Twrite) Offset() uint64 { return bo.Uint64(r[msgOffset+4 : msgOffset+12]) }
func (r Twrite) Count() uint32  { return bo.Uint32(r[msgOffset+12 : msgOffset+16]) }
func (r Twrite) Data() []byte   { return r[msgOffset+16 : msgOffset+16+int(r.Count())] }

// Returns slice of bytes to write to (ignores message's count)
func (r Twrite) DataNoLimit() []byte { return r[msgOffset+16:] }

/////////////////////////////////////
// size[4] Rwrite tag[2] count[4]
type Rwrite []byte

func (r Rwrite) fill(t Tag, count uint32) {
	size := uint32(msgOffset + 4)
	MsgBase(r).fill(msgRwrite, t, size)
	bo.PutUint32(r[msgOffset:msgOffset+4], count)
}

func (r Rwrite) Bytes() []byte { return MsgBase(r).Bytes() }
func (r Rwrite) Size() uint32  { return MsgBase(r).Size() }
func (r Rwrite) Tag() Tag      { return MsgBase(r).Tag() }
func (r Rwrite) Count() uint32 { return bo.Uint32(r[msgOffset : msgOffset+4]) }

/////////////////////////////////////
// size[4] Tremove tag[2] fid[4]
type Tremove []byte

func (r Tremove) fill(t Tag, fid Fid) {
	size := uint32(msgOffset + 4)
	MsgBase(r).fill(msgTremove, t, size)
	bo.PutUint32(r[msgOffset:msgOffset+4], uint32(fid))
}

func (r Tremove) Bytes() []byte { return MsgBase(r).Bytes() }
func (r Tremove) Size() uint32  { return MsgBase(r).Size() }
func (r Tremove) Tag() Tag      { return MsgBase(r).Tag() }
func (r Tremove) Fid() Fid      { return Fid(bo.Uint32(r[msgOffset : msgOffset+4])) }
func (r Tremove) SetFid(v Fid)  { bo.PutUint32(r[msgOffset:msgOffset+4], uint32(v)) }

/////////////////////////////////////
// size[4] Rremove tag[2]
type Rremove []byte

func (r Rremove) fill(t Tag) {
	size := uint32(msgOffset)
	MsgBase(r).fill(msgRremove, t, size)
}

func (r Rremove) Bytes() []byte { return MsgBase(r).Bytes() }
func (r Rremove) Size() uint32  { return MsgBase(r).Size() }
func (r Rremove) Tag() Tag      { return MsgBase(r).Tag() }

/////////////////////////////////////
// size[4] Tstat tag[2] fid[4]
type Tstat []byte

func (r Tstat) fill(t Tag, fid Fid) {
	size := uint32(msgOffset + 4)
	MsgBase(r).fill(msgTstat, t, size)
	bo.PutUint32(r[msgOffset:msgOffset+4], uint32(fid))
}

func (r Tstat) Bytes() []byte { return MsgBase(r).Bytes() }
func (r Tstat) Size() uint32  { return MsgBase(r).Size() }
func (r Tstat) Tag() Tag      { return MsgBase(r).Tag() }
func (r Tstat) Fid() Fid      { return Fid(bo.Uint32(r[msgOffset : msgOffset+4])) }
func (r Tstat) SetFid(v Fid)  { bo.PutUint32(r[msgOffset:msgOffset+4], uint32(v)) }

/////////////////////////////////////
// size[4] Rstat tag[2] stat[n]
type Rstat []byte

func (r Rstat) fill(t Tag, s Stat) {
	b := s.Bytes()
	size := uint32(msgOffset + len(b) + 2)
	MsgBase(r).fill(msgRstat, t, size)
	// from docs:
	//   "To make the contents of a directory, such as returned by read(5), easy
	//   to parse, each directory entry begins with a size field. For
	//   consistency, the entries in Twstat and Rstat messages also contain their
	//   size, which means the size appears twice."
	bo.PutUint16(r[msgOffset:msgOffset+2], uint16(len(b)))
	copy(r[msgOffset+2:], b)
}

func (r Rstat) Bytes() []byte { return MsgBase(r).Bytes() }
func (r Rstat) Size() uint32  { return MsgBase(r).Size() }
func (r Rstat) Tag() Tag      { return MsgBase(r).Tag() }
func (r Rstat) N() uint16     { return bo.Uint16(r[msgOffset : msgOffset+2]) }
func (r Rstat) Stat() Stat    { return Stat(r[msgOffset+2:]) }

/////////////////////////////////////
// size[4] Twstat tag[2] fid[4] stat[n]
type Twstat []byte

func (r Twstat) fill(t Tag, fid Fid, s Stat) {
	b := s.Bytes()
	size := uint32(msgOffset + 4 + len(b))
	MsgBase(r).fill(msgTwstat, t, size)
	bo.PutUint32(r[msgOffset:msgOffset+4], uint32(fid))
	// from docs:
	//   "To make the contents of a directory, such as returned by read(5), easy
	//   to parse, each directory entry begins with a size field. For
	//   consistency, the entries in Twstat and Rstat messages also contain their
	//   size, which means the size appears twice."
	bo.PutUint16(r[msgOffset+4:msgOffset+6], uint16(len(b)))
	copy(r[msgOffset+6:], s)
}

func (r Twstat) Bytes() []byte { return MsgBase(r).Bytes() }
func (r Twstat) Size() uint32  { return MsgBase(r).Size() }
func (r Twstat) Tag() Tag      { return MsgBase(r).Tag() }
func (r Twstat) Fid() Fid      { return Fid(bo.Uint32(r[msgOffset : msgOffset+4])) }
func (r Twstat) SetFid(v Fid)  { bo.PutUint32(r[msgOffset:msgOffset+4], uint32(v)) }
func (r Twstat) N() uint16     { return bo.Uint16(r[msgOffset+4 : msgOffset+6]) }
func (r Twstat) Stat() Stat    { return Stat(r[msgOffset+6:]) }

/////////////////////////////////////
// size[4] Rwstat tag[2]
type Rwstat []byte

func (r Rwstat) fill(t Tag) {
	size := uint32(msgOffset)
	MsgBase(r).fill(msgRwstat, t, size)
}

func (r Rwstat) Bytes() []byte { return MsgBase(r).Bytes() }
func (r Rwstat) Size() uint32  { return MsgBase(r).Size() }
func (r Rwstat) Tag() Tag      { return MsgBase(r).Tag() }

///////////////////////////////////////////

func PathSplit(path string) []string {
	if len(path) > 0 && path[0] != '/' {
		path = "/" + path
	}
	if path == "" {
		return []string{""}
	} else if path == "/" {
		return []string{""}
	}
	return strings.Split(path, "/")
}

func IsSubpath(path, parentPath string) bool {
	parLen := len(parentPath)
	if len(path) < parLen {
		return false
	}
	if len(path) == parLen {
		return strings.HasPrefix(path, parentPath)
	}
	for i, ch := range []byte(parentPath) {
		if path[i] != ch {
			return false
		}
	}
	return parentPath[parLen-1] == '/' || path[parLen] == '/'
}
