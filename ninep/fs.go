package ninep

import (
	"context"
	"io"
	"os"
	"sync"
	"time"
)

// Represent a file that can be read or written to. Can be either a file or directory
type FileHandle interface {
	io.ReaderAt
	io.WriterAt
	io.Closer

	Sync() error
}

// Special file handle used for authentication
type AuthFileHandle interface {
	FileHandle
	// Returns true if the user is authorized to access the FileSystem
	// Called when Tattach occurs, after ta user has authenticated from Tauth
	Authorized(usr, mnt string) bool
}

// return os.FileInfo from FileSystem can implement this if they want to
// utilize modes only available in 9P protocol
type FileInfoMode9P interface{ Mode9P() Mode }

// return os.FileInfo from FileSystem can implement this if they want more
// precisely control the Qid version, which should change every time the
// version number changes.
type FileInfoVersion interface{ Version() uint32 }

// return os.FileInfo from FileSystem can implement this if they want more
// precisely control the Qid path, which should change every time the there's a
// different file in the file system. Two files with the same exact filepath can have different paths if:
//
// - Create file // Qid with Path A
// - Delete file
// - Create file // Qid with Path B
//
// Similiar to Linux Inodes.
type FileInfoPath interface{ Path() uint64 }

// if this file info supports plan9 usernames for files
type FileInfoUid interface{ Uid() string }

// if this file info supports plan9 group names for files
type FileInfoGid interface{ Gid() string }

// if this file info supports plan9 usernames names for last modified
type FileInfoMuid interface{ Muid() string }

// TODO: support this feature
// return os.FileInfo from FileSystem can implement this if they want more
// precisely control the Qid path, which represents the same internal file
// in the file system (aka - rm foo.txt; touch foo.txt operate on two different
// paths)
// type FileInfoPath interface{ Path() uint32 }

// Interface for a server to verify a client
// Return nil, nil to indicate no authentication needed
type Authorizer interface {
	Auth(ctx context.Context, addr, user, access string) (AuthFileHandle, error)
}

// Interface for a client to request credentials
type Authorizee interface {
	Prove(ctx context.Context, user, access string) error
}

///////////////////////////////////////////////////////////////

type StatIterator interface {
	FileInfoIterator
	// callers must copy stat if they want to retain it beyond the next call to
	// NextStat() or Close()
	NextStat() (Stat, error)
}

type FileInfoIterator interface {
	// returns io.EOF with os.FileInfo = nil on end
	NextFileInfo() (os.FileInfo, error)
	// resets the reading of the file infos
	Reset() error
	// must be called to free iterator resources
	Close() error
}

type fileInfoSliceIterator struct {
	infos []os.FileInfo
	index int
}

var _ FileInfoIterator = (*fileInfoSliceIterator)(nil)

func FileInfoSliceIterator(fi []os.FileInfo) FileInfoIterator {
	return &fileInfoSliceIterator{fi, 0}
}

func (itr *fileInfoSliceIterator) Close() error { return nil }
func (itr *fileInfoSliceIterator) Reset() error { itr.index = 0; return nil }
func (itr *fileInfoSliceIterator) NextFileInfo() (os.FileInfo, error) {
	idx := itr.index
	if idx >= len(itr.infos) {
		return nil, io.EOF
	}
	itr.index++
	return itr.infos[idx], nil
}

func FileInfoSliceFromIterator(itr FileInfoIterator, max int) ([]os.FileInfo, error) {
	if itr == nil {
		return nil, ErrMissingIterator
	}

	if it, ok := itr.(*fileInfoSliceIterator); ok {
		return it.infos, nil
	}

	items := make([]os.FileInfo, 0, 16)
	for max < 0 || len(items) < max {
		fi, err := itr.NextFileInfo()
		if fi != nil {
			items = append(items, fi)
		}
		if err == io.EOF {
			return items, nil
		} else if err != nil {
			return items, err
		}
	}
	return items, nil
}

///////////////////////////////////////////////////////////////

// A higher-level interface to the Plan9 file system protocol (9P2000)
//
// The following assumptions are part of the interface:
// - paths can be empty strings (which indicates root directory)
// - implementations may return paths with / in system
//
// Context may optionally contain the following keys:
//
//  - "session"    *Session - The server's session, if available
//  - "rawMessage" Message  - The message the server received, if available
//
type FileSystem interface {
	// Creates a directory. Implementations can reject if parent directories are missing
	MakeDir(ctx context.Context, path string, mode Mode) error
	// Creates a file and opens it for readng/writing
	CreateFile(ctx context.Context, path string, flag OpenMode, mode Mode) (FileHandle, error)
	// Opens an existing file for reading/writing
	OpenFile(ctx context.Context, path string, flag OpenMode) (FileHandle, error)
	// Lists directories and files in a given path. Does not include '.' or '..'
	ListDir(ctx context.Context, path string) (FileInfoIterator, error)
	// Lists stats about a given file or directory.
	Stat(ctx context.Context, path string) (os.FileInfo, error)
	// Writes stats about a given file or directory. Implementations perform an all-or-nothing write.
	// Callers must use NoTouch values to indicate the underlying
	// implementation should not overwrite values.
	//
	// Implementers must return os.ErrNotExist for files that do not exist
	//
	// The following stat values are off-limits and must be NoTouch values, according to spec:
	// - Uid (some implementations may break spec and support this value)
	// - Muid
	// - Device (aka - Dev)
	// - Type
	// - Qid
	// - Modifying Mode to change M_DIR
	WriteStat(ctx context.Context, path string, s Stat) error
	// Deletes a file or directory. Implementations may reject directories that aren't empty
	Delete(ctx context.Context, path string) error
}

// A file system that wants more information when deleting a file. This can be
// useful if you need to perform different operations on directories and files,
// but wish to avoid reading from the underlying storage because it may be
// expensive.
type DeleteWithModeFileSystem interface {
	DeleteWithMode(ctx context.Context, path string, m Mode) error
}

// A file system that wants to optimize Twalk operations
type WalkableFileSystem interface {
	// walk receives a number of directories to traverse (with the last one optionally being a file)
	// and returns stats about every file/directory traversed.
	//
	// It's expected that all FileInfos returned except for the last to be directories.
	//
	// Note: simply returning less FileInfos than parts indicates that the cd
	// failed to traversed to a certain depth.
	Walk(ctx context.Context, parts []string) ([]os.FileInfo, error)
}

////////////////////////////////////////////////

// file info helper wrappers
type fileInfoWithName struct {
	fi   os.FileInfo
	name string
}

func FileInfoWithName(fi os.FileInfo, name string) os.FileInfo {
	return &fileInfoWithName{fi, name}
}

func (f *fileInfoWithName) Name() string       { return f.name }
func (f *fileInfoWithName) Size() int64        { return f.fi.Size() }
func (f *fileInfoWithName) Mode() os.FileMode  { return f.fi.Mode() }
func (f *fileInfoWithName) ModTime() time.Time { return f.fi.ModTime() }
func (f *fileInfoWithName) IsDir() bool        { return f.fi.IsDir() }
func (f *fileInfoWithName) Sys() interface{}   { return f.fi.Sys() }

// file info unix to plan9 wrappers
type fileInfoWithUsers struct {
	fi             os.FileInfo
	uid, gid, muid string
}

type FileInfoUsers interface {
	os.FileInfo
	FileInfoUid
	FileInfoGid
	FileInfoMuid
}

func FileInfoWithUsers(fi os.FileInfo, uid, gid, muid string) FileInfoUsers {
	return &fileInfoWithUsers{fi, uid, gid, muid}
}

func (f *fileInfoWithUsers) Name() string       { return f.fi.Name() }
func (f *fileInfoWithUsers) Size() int64        { return f.fi.Size() }
func (f *fileInfoWithUsers) Mode() os.FileMode  { return f.fi.Mode() }
func (f *fileInfoWithUsers) ModTime() time.Time { return f.fi.ModTime() }
func (f *fileInfoWithUsers) IsDir() bool        { return f.fi.IsDir() }
func (f *fileInfoWithUsers) Sys() interface{}   { return f.fi.Sys() }
func (f *fileInfoWithUsers) Uid() string        { return f.uid }
func (f *fileInfoWithUsers) Gid() string        { return f.gid }
func (f *fileInfoWithUsers) Muid() string       { return f.muid }

type fileInfoWithSize struct {
	os.FileInfo
	newSize int64
}

func (f *fileInfoWithSize) Size() int64 { return f.newSize }

func FileInfoWithSize(fi os.FileInfo, size int64) os.FileInfo {
	return &fileInfoWithSize{
		FileInfo: fi,
		newSize:  size,
	}
}

/////////////////////////////////////////////////

type handleReaderWriter struct {
	h      FileHandle
	Offset int64
}

func (r *handleReaderWriter) Read(p []byte) (int, error) {
	n, err := r.h.ReadAt(p, r.Offset)
	r.Offset += int64(n)
	return n, err
}

func (r *handleReaderWriter) Write(p []byte) (int, error) {
	n, err := r.h.WriteAt(p, r.Offset)
	r.Offset += int64(n)
	return n, err
}

func ReaderStartingAt(h FileHandle, start int64) io.Reader { return &handleReaderWriter{h, start} }
func WriterStartingAt(h FileHandle, start int64) io.Writer { return &handleReaderWriter{h, start} }
func Reader(h FileHandle) io.Reader                        { return &handleReaderWriter{h, 0} }
func Writer(h FileHandle) io.Writer                        { return &handleReaderWriter{h, 0} }

/////////////////////////////////////////////////

// Implements a basic, in-memory struct that conforms to os.FileInfo
type SimpleFileInfo struct {
	FIName    string
	FISize    int64
	FIMode    os.FileMode
	FIModTime time.Time
	FISys     interface{}
}

func (f *SimpleFileInfo) Name() string       { return f.FIName }
func (f *SimpleFileInfo) Size() int64        { return f.FISize }
func (f *SimpleFileInfo) Mode() os.FileMode  { return f.FIMode }
func (f *SimpleFileInfo) ModTime() time.Time { return f.FIModTime }
func (f *SimpleFileInfo) IsDir() bool        { return f.FIMode&os.ModeDir != 0 }
func (f *SimpleFileInfo) Sys() interface{}   { return f.FISys }

////////////////////////////////////////////////

// Simple file implements os.FileInfo and FileHandle operations
type SimpleFile struct {
	os.FileInfo
	OpenFn func(mode OpenMode) (FileHandle, error)
}

func NewReadOnlySimpleFile(name string, mode os.FileMode, modTime time.Time, contents []byte) *SimpleFile {
	return &SimpleFile{
		&SimpleFileInfo{
			FIName:    name,
			FISize:    int64(len(contents)),
			FIMode:    mode | 0444,
			FIModTime: modTime,
			FISys:     nil,
		},
		func(m OpenMode) (FileHandle, error) {
			if m.IsReadable() {
				return &ReadOnlyMemoryFileHandle{contents}, nil
			} else {
				return nil, ErrWriteNotAllowed
			}
		},
	}
}

func NewSimpleFile(name string, mode os.FileMode, modTime time.Time, open func(m OpenMode) (FileHandle, error)) *SimpleFile {
	if mode == 0 {
		mode = 0444
	}
	return &SimpleFile{
		&SimpleFileInfo{
			FIName:    name,
			FISize:    0,
			FIMode:    mode,
			FIModTime: modTime,
			FISys:     nil,
		},
		open,
	}
}

func (f *SimpleFile) WriteInfo(in os.FileInfo) error { return ErrUnsupported }
func (f *SimpleFile) Info() (os.FileInfo, error)     { return f, nil }
func (f *SimpleFile) Open(m OpenMode) (FileHandle, error) {
	if f.OpenFn == nil {
		return nil, ErrUnsupported
	}
	return f.OpenFn(m)
}

////////////////////////////////////////////////

type ReadOnlyMemoryFileHandle struct {
	Contents []byte
}

func (h *ReadOnlyMemoryFileHandle) ReadAt(p []byte, off int64) (n int, err error) {
	if off >= int64(len(h.Contents)) || off < 0 {
		return 0, io.EOF
	}
	return copy(p, h.Contents[off:]), nil
}
func (h *ReadOnlyMemoryFileHandle) WriteAt(p []byte, off int64) (n int, err error) {
	return 0, ErrUnsupported
}
func (h *ReadOnlyMemoryFileHandle) Sync() error  { return nil }
func (h *ReadOnlyMemoryFileHandle) Close() error { return nil }

////////////////////////////////////////////////

// Supports receiving writes up to the max size in Buf
type WriteOnlyFileHandle struct {
	m       sync.Mutex
	Buf     []byte
	OnWrite func(p []byte) (int, error)
}

func (h *WriteOnlyFileHandle) ReadAt(p []byte, off int64) (n int, err error) { return 0, ErrUnsupported }
func (h *WriteOnlyFileHandle) WriteAt(p []byte, off int64) (n int, err error) {
	h.m.Lock()
	defer h.m.Unlock()
	if int64(cap(h.Buf)) <= off {
		return 0, io.ErrShortWrite
	}
	end := int64(len(p)) + off
	for int64(len(h.Buf)) < end {
		h.Buf = append(h.Buf, 0)
	}
	copied := copy(h.Buf[off:], p)

	if h.OnWrite != nil {
		n, err = h.OnWrite(h.Buf)
		h.Buf = append(h.Buf[:0], h.Buf[n:]...)
		n = copied
	} else {
		err = ErrUnsupported
	}
	return
}
func (h *WriteOnlyFileHandle) Sync() error  { return nil }
func (h *WriteOnlyFileHandle) Close() error { return nil }

////////////////////////////////////////////////

type RWFileHandle struct {
	R io.Reader
	W io.Writer
}

func (h *RWFileHandle) ReadAt(p []byte, off int64) (n int, err error) {
	if h.R == nil {
		return 0, io.EOF
	}
	n, err = h.R.Read(p)
	if err == io.ErrClosedPipe {
		err = io.EOF
	}
	return n, err
}
func (h *RWFileHandle) WriteAt(p []byte, off int64) (n int, err error) {
	if h.W == nil {
		return 0, io.EOF
	}
	n, err = h.W.Write(p)
	if err == io.ErrClosedPipe {
		err = io.EOF
	}
	return n, err
}
func (h *RWFileHandle) Sync() error { return nil }
func (h *RWFileHandle) Close() error {
	if rc, ok := h.R.(io.Closer); ok {
		rc.Close()
	}
	if wc, ok := h.W.(io.Closer); ok {
		wc.Close()
	}
	return nil
}

////////////////////////////////////////////////

type ProtectedFileHandle struct {
	H    FileHandle
	Flag OpenMode
}

func (h *ProtectedFileHandle) ReadAt(p []byte, off int64) (n int, err error) {
	if !h.Flag.IsReadable() {
		return 0, ErrReadNotAllowed
	}
	return h.H.ReadAt(p, off)
}
func (h *ProtectedFileHandle) WriteAt(p []byte, off int64) (n int, err error) {
	if !h.Flag.IsWriteable() {
		return 0, ErrWriteNotAllowed
	}
	return h.H.WriteAt(p, off)
}

func (h *ProtectedFileHandle) Sync() error  { return h.H.Sync() }
func (h *ProtectedFileHandle) Close() error { return h.H.Close() }
