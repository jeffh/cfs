// Manages the client-server Plan9 File System Protocol (9p)
//
// 9p is a network file system thats known for its small interface while
// provided a unix-like file system. It's relatively simple API encourages more
// novel uses to map onto a file system-like layout.
//
// This package contains both low-level and high-level data structures and
// algorithms related to the 9p protocol.
//
// Besides providing common interfaces for both clients and servers, it also
// handle the encoding and decoding of 9p network messages to be used over a
// socket.
//
// See cfs/fs/* for concrete file system implementations.
package ninep

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io/fs"
	"math"
	"os"
	"strings"
	"time"
)

const (
	NoTouchU64  = ^uint64(0) // Represents a Uint64 value that is unmodified in 9p stat
	NoTouchU32  = ^uint32(0) // Represents a Uint32 value that is unmodified in 9p stat
	NoTouchU16  = ^uint16(0) // Represents a Uint16 value that is unmodified in 9p stat
	NoTouchMode = ^Mode(0)   // Represents a Mode value that is unmodified in 9p stat

	NoQidVersion = NoTouchU32 // Represents a QidVersion that is unmodified
)

// Represents all 9P network protocol messages
type Message interface {
	Tag() Tag
	Bytes() []byte
}

////////////////////////////////////////////////////////////////////////////////

// Changes all Fids of a given message using a mapper function
func RemapFids(m Message, mapper func(a Fid) Fid) Message {
	// Represents messages that talks about a file descriptor
	type Fidable interface {
		Fid() Fid
		SetFid(v Fid)
	}

	if msg, ok := m.(Fidable); ok {
		f := mapper(msg.Fid())
		msg.SetFid(f)
	}

	switch msg := m.(type) {
	case Twalk:
		msg.SetNewFid(mapper(msg.NewFid()))
	case Tauth:
		msg.SetAfid(mapper(msg.Afid()))
	case Tattach:
		msg.SetAfid(mapper(msg.Afid()))
	}
	return m
}

////////////////////////////////////////////////////////////////////////////////

const (
	NO_TAG         Tag    = ^Tag(0)  // Represents an empty tag in the 9p protocol
	NO_FID         Fid    = ^Fid(0)  // Represents an empty fid in the 9p protocol
	VERSION_9P2000 string = "9P2000" // Supported version
	VERSION_9P     string = "9P"     // Base protocol version

	MIN_MESSAGE_SIZE = uint32(128) // The minimum 9p message size in bytes based on 9p protocol
)

// The default maximum size of 9p message blocks
var DEFAULT_MAX_MESSAGE_SIZE uint32

func init() {
	s := uint64(os.Getpagesize())
	if s > math.MaxUint32 {
		s = math.MaxUint32
	}
	if uint32(s) < MIN_MESSAGE_SIZE {
		s = uint64(MIN_MESSAGE_SIZE)
	}
	DEFAULT_MAX_MESSAGE_SIZE = uint32(s)
}

// An opcode that represents each type of 9p message
type msgType byte

// Based on
// http://plan9.bell-labs.com/sources/plan9/sys/include/fcall.h
const (
	msgTversion msgType = iota + 100 // size[4] Tversion tag[2] msize[4] version[s]
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

func (t msgType) String() string {
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
	panic(fmt.Errorf("unexpected message: %d", t))
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

func (m OpenMode) IsTruncate() bool {
	return m&OTRUNC != 0
}

func (m OpenMode) RequestsMutation() bool {
	return m.IsWriteable() || m&(OTRUNC|ORCLOSE) != 0
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

func OpenModeFromOS(flag int) OpenMode {
	var m OpenMode
	if flag&os.O_RDWR != 0 {
		m |= ORDWR
	} else if flag&os.O_WRONLY != 0 {
		m |= OWRITE
	} else if flag&os.O_RDONLY != 0 {
		m |= OREAD
	}

	if flag&os.O_EXCL != 0 {
		m |= OCEXEC
	}

	if flag&os.O_TRUNC != 0 {
		m |= OTRUNC
	}
	return m
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
	perm := fs.FileMode(m & M_PERM)
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
	// M_APPEND is to for creating files that can only be appended to (no overwriting)
	// if m&M_APPEND != 0 {
	// 	flag |= os.O_APPEND
	// }
	return flag
}

func (m Mode) ToFsMode() fs.FileMode {
	var mode fs.FileMode
	if m&M_DIR != 0 {
		mode = fs.ModeDir
	}
	// M_APPEND is to for creating files that can only be appended to (no overwriting)
	// if m&M_APPEND != 0 {
	// 	mode |= fs.ModeAppend
	// }
	if m&M_EXCL != 0 {
		mode |= fs.ModeExclusive
	}
	if m&M_TMP != 0 {
		mode |= fs.ModeTemporary
	}
	mode |= (fs.FileMode(m) & fs.ModePerm)
	return mode
}

func (m Mode) QidType() QidType {
	return QidType((m & M_TYPE) >> 24)
}

func versionFromFileInfo(info fs.FileInfo) uint32 {
	if i, ok := info.(FileInfoVersion); ok {
		return i.Version()
	}
	return NoQidVersion
}

func ModeFromFileInfo(info fs.FileInfo) Mode {
	if in, ok := info.(FileInfoMode9P); ok {
		return in.Mode9P()
	}
	return ModeFromFS(info.Mode())
}

func ModeFromFS(mode fs.FileMode) Mode {
	perm := Mode(mode.Perm())
	if mode&fs.ModeDir != 0 {
		perm |= M_DIR
	}
	if mode&fs.ModeAppend != 0 {
		perm |= M_APPEND
	}
	if mode&fs.ModeExclusive != 0 {
		perm |= M_EXCL
	}
	if mode&fs.ModeTemporary != 0 {
		perm |= M_TMP
	}
	return perm
}

var bo = binary.LittleEndian

/////////////////////////////////////

// Represents a message id. Tags must be unique per active requests, per client.
type Tag uint16

/////////////////////////////////////

// Represents the share data for all 9p messages
type MsgBase []byte

func (r MsgBase) fill(mt msgType, t Tag, size uint32) {
	bo.PutUint32(r[:4], size)       // Size
	r[4] = byte(mt)                 // msgType
	bo.PutUint16(r[5:7], uint16(t)) // Tag
}

func (r MsgBase) Bytes() []byte    { return r[:int(r.Size())] }
func (r MsgBase) Size() uint32     { return bo.Uint32(r[:4]) }
func (r MsgBase) msgType() msgType { return msgType(r[4]) }
func (r MsgBase) Tag() Tag         { return Tag(bo.Uint16(r[5:7])) }

const msgOffset = 7

// ///////////////////////////////////
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

type Fid uint32 // represents a 9p file descriptor

const MAX_FID = math.MaxUint32 - 2 // The maximum value a FID can be

func (f Fid) String() string {
	return fmt.Sprintf("Fid(%d)", f)
}

/////////////////////////////////////

// fidSlice is a slice of fids, for sorting
type fidSlice []Fid

func (s fidSlice) Len() int           { return len(s) }
func (s fidSlice) Less(i, j int) bool { return s[i] < s[j] }
func (s fidSlice) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }

/////////////////////////////////////

// QidType stores metadata of the current file descriptor
type QidType byte

const (
	QT_FILE    QidType = 0x00 // FD refers to a file
	QT_LINK    QidType = 0x01 // FD refers to a link
	QT_SYMLINK QidType = 0x02 // FD refers to a symlink
	QT_TMP     QidType = 0x04 // FD refers to a temporary file that may go away
	QT_AUTH    QidType = 0x08 // FD refers to the special 9p auth file
	QT_MOUNT   QidType = 0x10 // FD refers to a mount point
	QT_EXCL    QidType = 0x20 // FD is exclusively held (locked)
	QT_DIR     QidType = 0x80 // FD refers to a directory
)

func (qt QidType) IsDir() bool       { return qt&QT_DIR != 0 }     // Returns true if FD is a directory
func (qt QidType) IsSymLink() bool   { return qt&QT_SYMLINK != 0 } // Returns true if FD is a SymLink
func (qt QidType) IsAuth() bool      { return qt&QT_AUTH != 0 }    // Returns true if FD is a 9p auth file
func (qt QidType) IsMount() bool     { return qt&QT_MOUNT != 0 }   // Returns true if FD is a mountpoint
func (qt QidType) IsExclusive() bool { return qt&QT_EXCL != 0 }    // Returns true if FD is has an exclusive lock held
func (qt QidType) IsTemporary() bool { return qt&QT_TMP != 0 }     // Returns true if FD is a temporary file or dir

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

const QidSize = 13 // The number of bytes a Qid takes

// Qid is the server's version of a file descriptor.
// Stores unique ids and some metadata about the file requested.
type Qid []byte // always sized to QidSize, not an array to minimize copies

// NoTouchQid is a Qid special value indicating that the field should not be touched when used with WriteStat
var NoTouchQid Qid

func init() {
	NoTouchQid = NewQid()
	for i := range NoTouchQid {
		NoTouchQid[i] = 0xff
	}
}

// NewQid creates a new zeroed Qid.
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

func (q Qid) Bytes() []byte       { return q[:QidSize] }       // Returns the raw bytes of a Qid. Used for writing to a socket
func (q Qid) Type() QidType       { return QidType(q[0]) }     // Returns the metadata available on a Qid
func (q Qid) Version() uint32     { return bo.Uint32(q[1:5]) } // Returns the version of the file. The server usually changes this value when the file changes.
func (q Qid) SetType(t QidType)   { q[0] = byte(t) }
func (q Qid) SetVersion(v uint32) { bo.PutUint32(q[1:5], v) }

// Returns the path id of the file. This is similar to inode numbers. The server should return a different path for different files, event if it's the same file path
//
// An example is when a file gets deleted and created:
//
//  1. Delete file /foo
//  2. Create file /foo
//
// Both files in the example scenario should have different paths, but it's up to the server implementation.
func (q Qid) Path() uint64     { return bo.Uint64(q[5 : 5+8]) }
func (q Qid) SetPath(v uint64) { bo.PutUint64(q[5:5+8], v) } // not recommended to use unless you know the impact of this

// Returns true based on the 9p protocol indicating "This Qid value does not change"
func (q Qid) IsNoTouch() bool {
	for _, v := range q.Bytes() {
		if v != 0xff {
			return false
		}
	}
	return true
}

// Allocates and copies the current Qid
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

// Stat is metadata about a file descriptor. Part of the 9p protocol.
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
		copy(b, muid)
	}
	// s.name().SetStringAndLen(name)
	// s.uid().SetStringAndLen(uid)
	// s.gid().SetStringAndLen(gid)
	// s.muid().SetStringAndLen(muid)
}

// StatFromFileInfo creates a Stat from an fs.FileInfo.
//
// Note that Stat and fs.FileInfo are not perfect equivalents.
func StatFromFileInfo(info fs.FileInfo) Stat {
	if st, ok := info.Sys().(Stat); ok {
		return st
	}
	qt := QidType(0)
	m := info.Mode()
	if m&fs.ModeDir != 0 {
		qt |= QT_DIR
	}
	if m&fs.ModeExclusive != 0 {
		qt |= QT_EXCL
	}
	if m&fs.ModeSymlink != 0 {
		qt |= QT_SYMLINK
	}
	if m&fs.ModeTemporary != 0 {
		qt |= QT_TMP
	}
	qid := NewQid()
	qid.Fill(qt, 0, 0)
	return fileInfoToStat(qid, info)
}
func StatFromFileInfoClone(info fs.FileInfo) Stat {
	if st, ok := info.Sys().(Stat); ok {
		return st.Clone()
	}
	qt := QidType(0)
	m := info.Mode()
	if m&fs.ModeDir != 0 {
		qt |= QT_DIR
	}
	if m&fs.ModeExclusive != 0 {
		qt |= QT_EXCL
	}
	if m&fs.ModeSymlink != 0 {
		qt |= QT_SYMLINK
	}
	if m&fs.ModeTemporary != 0 {
		qt |= QT_TMP
	}
	qid := NewQid()
	qid.Fill(qt, 0, 0)
	return fileInfoToStat(qid, info)
}

// SyncStat creates a zero-value Stat for writing stat updates. This provides a
// good basis for creating updates. If left as is, this is considered an fsync.
//
// Due to the 9p2000 spec, SyncStat is the "zero-value" of Stat. Non-string
// fields can be modified to only request that field to be modified.
//
// Non-string fields are set to NoTouch states.
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

// SyncStatWithName creates a SyncStat with the given name.
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

// NewStat creates a new Stat with the given name, uid, gid, and muid.
// Other fields can be set using setter methods
func NewStat(name, uid, gid, muid string) Stat {
	// TODO: error if strings are too large
	size := statSize(name, uid, gid, muid)
	s := Stat(make([]byte, size))
	s.fill(name, uid, gid, muid)
	return s
}

// CopyFixedFieldsFrom copies the fixed fields from one Stat to another.
// Only strings are excluded from this copy.
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

// CopyWithNewName creates a new Stat with the same fields as s, but with the
// given name.
func (s Stat) CopyWithNewName(n string) Stat {
	st := NewStat(n, s.Uid(), s.Gid(), s.Muid())
	st.CopyFixedFieldsFrom(s)
	return st
}

// Nbytes returns the number of bytes in the Stat.
func (s Stat) Nbytes() int { return int(s.Size() + 2) }

// Bytes returns the bytes in the Stat.
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

// Size returns the size of the Stat. This does not include the size field itself.
func (s Stat) Size() uint16 { return bo.Uint16(s[:2]) }

// SetSize sets the size of the Stat. This does not include the size field itself.
func (s Stat) SetSize(v uint16) { bo.PutUint16(s[:2], v) }

// TypeNoTouch returns true if the Type field is not set for WriteStat.
func (s Stat) TypeNoTouch() bool { return s.Type() == NoTouchU16 }

// Type returns the Type field of the Stat.
func (s Stat) Type() uint16 { return bo.Uint16(s[2:4]) }

// SetType sets the Type field of the Stat.
func (s Stat) SetType(v uint16) { bo.PutUint16(s[2:4], v) }

// DevNoTouch returns true if the Dev field is not set for WriteStat.
func (s Stat) DevNoTouch() bool { return s.Dev() == NoTouchU32 }

// Dev returns the Dev field of the Stat.
func (s Stat) Dev() uint32 { return bo.Uint32(s[4:8]) }

// SetDev sets the Dev field of the Stat.
func (s Stat) SetDev(v uint32) { bo.PutUint32(s[4:8], v) }

// Qid returns the Qid field of the Stat.
func (s Stat) Qid() Qid { return Qid(s[8 : 8+QidSize]) }

// SetQid sets the Qid field of the Stat.
func (s Stat) SetQid(v Qid) { copy(s[8:8+QidSize], v.Bytes()) }

// ModeNoTouch returns true if the Mode field is not set for WriteStat.
func (s Stat) ModeNoTouch() bool { return s.Mode() == NoTouchMode }

// Mode returns the Mode field of the Stat.
func (s Stat) Mode() Mode { return Mode(bo.Uint32(s[8+QidSize : 8+QidSize+4])) }

// SetMode sets the Mode field of the Stat.
func (s Stat) SetMode(v Mode) { bo.PutUint32(s[8+QidSize:8+QidSize+4], uint32(v)) }

// AtimeNoTouch returns true if the Atime field is not set for WriteStat.
func (s Stat) AtimeNoTouch() bool { return s.Atime() == NoTouchU32 }

// Atime returns the Atime field of the Stat which indicates the last access time of the file.
func (s Stat) Atime() uint32 { return bo.Uint32(s[8+QidSize+4 : 8+QidSize+4+4]) }

// SetAtime sets the Atime field of the Stat which indicates the last access time of the file.
func (s Stat) SetAtime(v uint32) { bo.PutUint32(s[8+QidSize+4:8+QidSize+4+4], v) }

// MtimeNoTouch returns true if the Mtime field is not set for WriteStat.
func (s Stat) MtimeNoTouch() bool { return s.Mtime() == NoTouchU32 }

// Mtime returns the Mtime field of the Stat which indicates the last modification time of the file.
func (s Stat) Mtime() uint32 { return bo.Uint32(s[8+QidSize+4+4 : 8+QidSize+4+4+4]) }

// SetMtime sets the Mtime field of the Stat which indicates the last modification time of the file.
func (s Stat) SetMtime(v uint32) { bo.PutUint32(s[8+QidSize+4+4:8+QidSize+4+4+4], v) }

// LengthNoTouch returns true if the Length field is not set for WriteStat.
func (s Stat) LengthNoTouch() bool { return s.Length() == NoTouchU64 }

// Length returns the Length field of the Stat which indicates the size of the file.
func (s Stat) Length() uint64 { return bo.Uint64(s[8+QidSize+4+4+4 : 8+QidSize+4+4+4+8]) }

// SetLength sets the Length field of the Stat which indicates the size of the file.
func (s Stat) SetLength(v uint64) { bo.PutUint64(s[8+QidSize+4+4+4:8+QidSize+4+4+4+8], v) }

func (s Stat) name() msgString { return msgString(s[41:]) }

// NameNoTouch returns true if the Name field is not set for WriteStat.
func (s Stat) NameNoTouch() bool { return len(s.NameBytes()) == 0 }

// NameBytes returns the bytes of the Name field.
func (s Stat) NameBytes() []byte { return s.name().Bytes() }

// Name returns the Name field of the Stat.
func (s Stat) Name() string { return s.name().String() }

func (s Stat) uid() msgString { return msgString(s[41+s.name().Nbytes():]) }

// UidNoTouch returns true if the Uid field is not set for WriteStat.
func (s Stat) UidNoTouch() bool { return len(s.UidBytes()) == 0 }

// UidBytes returns the bytes of the Uid field.
func (s Stat) UidBytes() []byte { return s.uid().Bytes() }

// Uid returns the Uid field of the Stat.
func (s Stat) Uid() string { return s.uid().String() }

func (s Stat) gid() msgString { return msgString(s[41+s.name().Nbytes()+s.uid().Nbytes():]) }

// GidNoTouch returns true if the Gid field is not set for WriteStat.
func (s Stat) GidNoTouch() bool { return len(s.GidBytes()) == 0 }

// GidBytes returns the bytes of the Gid field.
func (s Stat) GidBytes() []byte { return s.gid().Bytes() }

// Gid returns the Gid field of the Stat.
func (s Stat) Gid() string { return s.gid().String() }

func (s Stat) muid() msgString {
	return msgString(s[41+s.name().Nbytes()+s.uid().Nbytes()+s.gid().Nbytes():])
}

// MuidNoTouch returns true if the Muid field is not set for WriteStat.
func (s Stat) MuidNoTouch() bool { return len(s.MuidBytes()) == 0 }

// MuidBytes returns the bytes of the Muid field.
func (s Stat) MuidBytes() []byte { return s.muid().Bytes() }

// Muid returns the Muid field of the Stat.
func (s Stat) Muid() string { return s.muid().String() }

// IsNoTouch returns true if the Stat is equivalent to SyncStat.
func (s Stat) IsNoTouch() bool {
	return s.NameNoTouch() && s.UidNoTouch() && s.GidNoTouch() && s.MuidNoTouch() &&
		s.TypeNoTouch() && s.DevNoTouch() && s.Qid().IsNoTouch() && s.ModeNoTouch() &&
		s.AtimeNoTouch() && s.MtimeNoTouch() && s.LengthNoTouch()
}

// FileInfo returns a StatFileInfo for the Stat. Sys of the returned FileInfo returns the Stat itself.
func (s Stat) FileInfo() fs.FileInfo { return statFileInfo{s} }

// Clone returns a copy of the Stat.
func (s Stat) Clone() Stat {
	st := make(Stat, len(s))
	copy(st, s)
	return st
}

func (s Stat) fileUsers() (uid, gid, muid string, err error) {
	return s.Uid(), s.Gid(), s.Muid(), nil
}

// Adapter to map os.FileInfo for Stat type.
type statFileInfo struct {
	Stat
}

var _ os.FileInfo = (*statFileInfo)(nil)

func (s statFileInfo) Size() int64        { return int64(s.Length()) }
func (s statFileInfo) Name() string       { return s.Stat.Name() }
func (s statFileInfo) Mode() fs.FileMode  { return s.Stat.Mode().ToFsMode() }
func (s statFileInfo) ModTime() time.Time { return time.Unix(int64(s.Mtime()), 0) }
func (s statFileInfo) IsDir() bool        { return s.Stat.Mode()&M_DIR != 0 }
func (s statFileInfo) Sys() interface{}   { return s.Stat }

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

func (r Rerror) ToError() error {
	msg := r.Ename()
	// we want to preserve equality of errors to native os-styled errors
	for _, e := range mappedErrors {
		if msg == e.Error() {
			return e
		}
	}
	// else
	return errors.New(msg)
}

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
//
// Note: this implemention current ignores this

const MAXWELEM = 16 // The 9p protocol's maximum number of elements a Walk request should have.

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
	switch path {
	case "", "/":
		return []string{""}
	default:
		return strings.Split(path, "/")
	}
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
