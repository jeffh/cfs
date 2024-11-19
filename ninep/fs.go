package ninep

import (
	"context"
	"fmt"
	"io"
	"io/fs"
	"iter"
	"path/filepath"
	"runtime/debug"
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

// Return fs.FileInfo from FileSystem can implement this if they want to
// utilize modes only available in 9P protocol
type FileInfoMode9P interface{ Mode9P() Mode }

// Return fs.FileInfo from FileSystem can implement this if they want more
// precisely control the Qid version, which should change every time the
// version number changes.
type FileInfoVersion interface{ Version() uint32 }

// Instead of a fs.FileInfo from a FileSystem implement. This can be returned if that FS wants more
// precise control the Qid path, which should change every time the there's a
// different file in the file system. Two files with the same exact filepath
// can have different paths if:
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
// return fs.FileInfo from FileSystem can implement this if they want more
// precisely control the Qid path, which represents the same internal file
// in the file system (aka - rm foo.txt; touch foo.txt operate on two different
// paths)
// type FileInfoPath interface{ Path() uint32 }

// Interface for a server to verify a client.
// Return (nil, nil) to indicate no authentication needed
type Authorizer interface {
	Auth(ctx context.Context, addr, user, access string) (AuthFileHandle, error)
}

// Interface for a client to request credentials
type Authorizee interface {
	Prove(ctx context.Context, user, access string) error
}

///////////////////////////////////////////////////////////////

// Statable is an interface that can return a Stat object.
// fs.FileInfo types may optionally implement this interface.
type Statable interface {
	AsStat() Stat
}

func statToFileInfo(it iter.Seq2[Stat, error]) iter.Seq2[fs.FileInfo, error] {
	return func(yield func(fs.FileInfo, error) bool) {
		for s, err := range it {
			if err != nil {
				if !yield(nil, err) {
					return
				}
			} else if !yield(s.FileInfo(), err) {
				return
			}
		}
	}
}

func MapFileInfoIterator(itr iter.Seq2[fs.FileInfo, error], f func(fs.FileInfo) fs.FileInfo) iter.Seq2[fs.FileInfo, error] {
	return func(yield func(fs.FileInfo, error) bool) {
		for fi, err := range itr {
			if fi != nil {
				fi = f(fi)
			}
			if !yield(fi, err) {
				return
			}
		}
	}
}

func FileInfoErrorIterator(err error) iter.Seq2[fs.FileInfo, error] {
	return func(yield func(fs.FileInfo, error) bool) {
		yield(nil, err)
	}
}

func FileInfoSliceIterator(fi []fs.FileInfo) iter.Seq2[fs.FileInfo, error] {
	return func(yield func(fs.FileInfo, error) bool) {
		for _, f := range fi {
			if !yield(f, nil) {
				return
			}
		}
	}
}

func FileInfoSliceIteratorWithUsers(fi []fs.FileInfo, uid, gid, muid string) iter.Seq2[fs.FileInfo, error] {
	return func(yield func(fs.FileInfo, error) bool) {
		for _, f := range fi {
			if !yield(FileInfoWithUsers(f, uid, gid, muid), nil) {
				return
			}
		}
	}
}

func DebugIterator[X any](msg string, itr iter.Seq2[X, error]) iter.Seq2[X, error] {
	return func(yield func(X, error) bool) {
		defer func() {
			if r := recover(); r != nil {
				fmt.Printf("DEBUG: %s: %#v\n", msg, r)
				debug.PrintStack()
				panic(r)
			}
		}()
		itr(func(x X, err error) bool {
			return yield(x, err)
		})
	}
}

// Consumes an iterator to produce a slice of fs.FileInfos
// max < 0 means to fetch all items
func TakeErr[X any](itr iter.Seq2[X, error], n int) ([]X, error) {
	out := make([]X, 0, n)
	cnt := 0
	for x, err := range itr {
		if err != nil {
			return out, err
		}
		out = append(out, x)
		cnt++
		if n >= 0 && cnt >= n {
			break
		}
	}
	return out, nil
}

// TODO: inline to Take
// Consumes an iterator to produce a slice of fs.FileInfos
// max < 0 means to fetch all items
func FileInfoSliceFromIterator(itr iter.Seq2[fs.FileInfo, error], max int) ([]fs.FileInfo, error) {
	return TakeErr(itr, max)
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
//   - SessionKey    *Session - The server's session, if available
//   - RawMessageKey Message  - The message the server received, if available
//
// These keys are only populated if the FileSystem is running under a tcp
// server context.
//
// Every method must be safe to call concurrently.
type FileSystem interface {
	// Creates a directory. Implementions should recursively make directories
	// whenever possible.
	MakeDir(ctx context.Context, path string, mode Mode) error
	// Creates a file and opens it for reading/writing
	CreateFile(ctx context.Context, path string, flag OpenMode, mode Mode) (FileHandle, error)
	// Opens an existing file for reading/writing
	OpenFile(ctx context.Context, path string, flag OpenMode) (FileHandle, error)
	// Lists directories and files in a given path. Does not include '.' or '..'
	// fs.FileInfo may optionally implement Statable. Iterations are allowed to
	// return a nil FileInfo if they have an error.
	ListDir(ctx context.Context, path string) iter.Seq2[fs.FileInfo, error]
	// Lists stats about a given file or directory. Implementations may return
	// fs.ErrNotExist for directories that do not exist.
	Stat(ctx context.Context, path string) (fs.FileInfo, error)
	// Writes stats about a given file or directory. Implementations perform an all-or-nothing write.
	// Callers must use NoTouch values to indicate the underlying
	// implementation should not overwrite values.
	//
	// Implementers must return fs.ErrNotExist for files that do not exist
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

// Delete is a wrapper to delete a file
func Delete(ctx context.Context, fs FileSystem, path string, m Mode) error {
	if del, ok := fs.(DeleteWithModeFileSystem); ok {
		return del.DeleteWithMode(ctx, path, m)
	}
	return fs.Delete(ctx, path)
}

// A file system that wants to optimize Twalk operations. Twalk is a protocol operation that allows
// a client to walk a directory tree in a single operation (think: cd /path/to/dir).
//
// Without a custom implementation, the default behavior will recursively call
// Stat for each directory in the path and then traverse the directory.
//
// Unlike cd, Walk works with non-directory files, as its a fast way to check if
// a file exists in a given path.
type WalkableFileSystem interface {
	FileSystem
	// walk receives a number of directories to traverse (with the last one optionally being a file)
	// and returns stats about every file/directory traversed.
	//
	// It's expected that all FileInfos returned except for the last to be directories.
	//
	// Note: simply returning less FileInfos than parts indicates that the cd
	// failed to traverse to the specified depth.
	Walk(ctx context.Context, parts []string) ([]fs.FileInfo, error)
}

func Walk(ctx context.Context, f FileSystem, parts []string) ([]fs.FileInfo, error) {
	if walkable, ok := f.(WalkableFileSystem); ok {
		return walkable.Walk(ctx, parts)
	}
	return NaiveWalk(ctx, f, parts)
}

func NaiveWalk(ctx context.Context, f FileSystem, parts []string) ([]fs.FileInfo, error) {
	infos := make([]fs.FileInfo, 0, len(parts))
	for i := range parts {
		info, err := f.Stat(ctx, filepath.Join(parts[:i+1]...))
		if err != nil {
			return infos, err
		}
		infos = append(infos, info)
	}
	return infos, nil
}

////////////////////////////////////////////////

// file info helper wrappers
type fileInfoWithName struct {
	fi   fs.FileInfo
	name string
}

// Wraps an fs.FileInfo to provide a different Name() return value
func FileInfoWithName(fi fs.FileInfo, name string) fs.FileInfo {
	return &fileInfoWithName{fi, name}
}

func (f *fileInfoWithName) Name() string       { return f.name }
func (f *fileInfoWithName) Size() int64        { return f.fi.Size() }
func (f *fileInfoWithName) Mode() fs.FileMode  { return f.fi.Mode() }
func (f *fileInfoWithName) ModTime() time.Time { return f.fi.ModTime() }
func (f *fileInfoWithName) IsDir() bool        { return f.fi.IsDir() }
func (f *fileInfoWithName) Sys() interface{}   { return f.fi.Sys() }

type fileInfoWithMode struct {
	fs.FileInfo
	mode fs.FileMode
}

func (f *fileInfoWithMode) Mode() fs.FileMode { return f.mode }

func FileInfoWithMode(fi fs.FileInfo, mode fs.FileMode) fs.FileInfo {
	return &fileInfoWithMode{fi, mode}
}

// file info unix to plan9 wrappers
type fileInfoWithUsers struct {
	fi             fs.FileInfo
	uid, gid, muid string
}

// An fs.FileInfo with Plan9 uid & gid support
type FileInfoUsers interface {
	fs.FileInfo
	FileInfoUid
	FileInfoGid
	FileInfoMuid
}

// Returns a FileInfoUser based off of an fs.FileInfo, which the plan9 specific
// values provided.
func FileInfoWithUsers(fi fs.FileInfo, uid, gid, muid string) FileInfoUsers {
	if fi == nil {
		panic("nil fs.FileInfo")
	}
	return &fileInfoWithUsers{fi, uid, gid, muid}
}

func (f *fileInfoWithUsers) Name() string       { return f.fi.Name() }
func (f *fileInfoWithUsers) Size() int64        { return f.fi.Size() }
func (f *fileInfoWithUsers) Mode() fs.FileMode  { return f.fi.Mode() }
func (f *fileInfoWithUsers) ModTime() time.Time { return f.fi.ModTime() }
func (f *fileInfoWithUsers) IsDir() bool        { return f.fi.IsDir() }
func (f *fileInfoWithUsers) Sys() interface{}   { return f.fi.Sys() }
func (f *fileInfoWithUsers) Uid() string        { return f.uid }
func (f *fileInfoWithUsers) Gid() string        { return f.gid }
func (f *fileInfoWithUsers) Muid() string       { return f.muid }

type fileInfoWithSize struct {
	fs.FileInfo
	newSize int64
}

func (f *fileInfoWithSize) Size() int64 { return f.newSize }

// Wraps an fs.FileInfo with an override of the file size
func FileInfoWithSize(fi fs.FileInfo, size int64) fs.FileInfo {
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

// Returns an io.Reader interface around a FileHandle, starting at a given offset
func ReaderStartingAt(h FileHandle, start int64) io.Reader { return &handleReaderWriter{h, start} }

// Returns an io.Writer interface around a FileHandle, starting at a given offset
func WriterStartingAt(h FileHandle, start int64) io.Writer { return &handleReaderWriter{h, start} }

// Returns an io.Reader interface around a FileHandle, starting at the beginning of the file
func Reader(h FileHandle) io.Reader { return &handleReaderWriter{h, 0} }

// Returns an io.Writer interface around a FileHandle, starting at the beginning of the file
func Writer(h FileHandle) io.Writer { return &handleReaderWriter{h, 0} }

/////////////////////////////////////////////////

// RootFileInfo provides a basic fs.FileInfo for root directories
var RootFileInfo fs.FileInfo = &SimpleFileInfo{
	FIName:    ".",
	FISize:    0,
	FIMode:    fs.ModeDir | Readable | Executable,
	FIModTime: time.Time{},
	FISys:     nil,
}

// Implements a basic, in-memory struct that conforms to fs.FileInfo
type SimpleFileInfo struct {
	FIName    string
	FISize    int64
	FIMode    fs.FileMode
	FIModTime time.Time
	FISys     interface{}
}

func (f *SimpleFileInfo) Name() string       { return f.FIName }
func (f *SimpleFileInfo) Size() int64        { return f.FISize }
func (f *SimpleFileInfo) Mode() fs.FileMode  { return f.FIMode }
func (f *SimpleFileInfo) ModTime() time.Time { return f.FIModTime }
func (f *SimpleFileInfo) IsDir() bool        { return f.FIMode&fs.ModeDir != 0 }
func (f *SimpleFileInfo) Sys() interface{}   { return f.FISys }

const (
	Readable   fs.FileMode = 0o444
	Writeable  fs.FileMode = 0o222
	Executable fs.FileMode = 0o111
)

// MakeFileInfo returns an fs.FileInfo with the given name, mode, and modTime
func MakeFileInfo(name string, mode fs.FileMode, modTime time.Time) *SimpleFileInfo {
	return &SimpleFileInfo{
		FIName:    name,
		FIMode:    mode,
		FIModTime: modTime,
	}
}

// MakeFileInfoWithSize returns an fs.FileInfo with the given name, mode, modTime, and size
func MakeFileInfoWithSize(name string, mode fs.FileMode, modTime time.Time, size int64) *SimpleFileInfo {
	return &SimpleFileInfo{
		FIName:    name,
		FIMode:    mode,
		FISize:    size,
		FIModTime: modTime,
	}
}

// DirFileInfo returns an fs.FileInfo that looks like a directory
func DirFileInfo(name string) *SimpleFileInfo {
	return MakeFileInfo(name, fs.ModeDir|Readable|Executable, time.Now())
}

// DevFileInfo returns an fs.FileInfo that looks like device file
func DevFileInfo(name string) *SimpleFileInfo {
	return MakeFileInfo(name, fs.ModeDevice|Readable|Writeable, time.Now())
}

func ReadDevFileInfo(name string) *SimpleFileInfo {
	return MakeFileInfo(name, fs.ModeDevice|Readable, time.Now())
}
func WriteDevFileInfo(name string) *SimpleFileInfo {
	return MakeFileInfo(name, fs.ModeDevice|Writeable, time.Now())
}

func TempFileInfo(name string) *SimpleFileInfo {
	return MakeFileInfo(name, fs.ModeTemporary|Readable|Writeable, time.Now())
}

////////////////////////////////////////////////

// Simple file implements fs.FileInfo and FileHandle operations
type SimpleFile struct {
	fs.FileInfo
	OpenFn func(mode OpenMode) (FileHandle, error)
}

func NewReadOnlySimpleFile(name string, mode fs.FileMode, modTime time.Time, contents []byte) *SimpleFile {
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

func NewSimpleFile(name string, mode fs.FileMode, modTime time.Time, open func(m OpenMode) (FileHandle, error)) *SimpleFile {
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

func (f *SimpleFile) WriteInfo(in fs.FileInfo) error { return ErrUnsupported }
func (f *SimpleFile) Info() (fs.FileInfo, error)     { return f, nil }
func (f *SimpleFile) Open(m OpenMode) (FileHandle, error) {
	if f.OpenFn == nil {
		return nil, ErrUnsupported
	}
	return f.OpenFn(m)
}

////////////////////////////////////////////////

type ReaderFileHandle struct {
	io.Reader
}

func NewReaderFileHandle(r io.Reader) *ReaderFileHandle {
	return &ReaderFileHandle{r}
}

func (h *ReaderFileHandle) ReadAt(p []byte, off int64) (n int, err error) {
	if h.Reader == nil {
		return 0, io.ErrClosedPipe
	}
	return h.Reader.Read(p)
}
func (h *ReaderFileHandle) WriteAt(p []byte, off int64) (n int, err error) {
	return 0, fs.ErrPermission
}
func (h *ReaderFileHandle) Sync() error { return nil }
func (h *ReaderFileHandle) Close() error {
	if rc, ok := h.Reader.(io.Closer); ok {
		err := rc.Close()
		h.Reader = nil
		return err
	}
	return nil
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

func (h *WriteOnlyFileHandle) ReadAt(p []byte, off int64) (n int, err error) {
	return 0, ErrUnsupported
}
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

func WriteOnlyDeviceHandle() (*RWFileHandle, *io.PipeReader) {
	h := &RWFileHandle{}
	var r *io.PipeReader
	r, h.W = io.Pipe()
	return h, r
}

func DeviceHandle(flag OpenMode) (*RWFileHandle, *io.PipeReader, *io.PipeWriter) {
	h := &RWFileHandle{}
	var r *io.PipeReader
	if flag.IsWriteable() {
		r, h.W = io.Pipe()
	}
	var w *io.PipeWriter
	if flag.IsReadable() {
		h.R, w = io.Pipe()
	}
	return h, r, w
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

///////////////////////////////////////////////////

// MemoryFileHandle is a file handle that stores data in memory.
type MemoryFileHandle struct {
	data    []byte
	onWrite func([]byte, int64, int) error
	onFlush func([]byte) error
}

func NewMemoryFileHandle(data []byte, onWrite func([]byte, int64, int) error, onFlush func([]byte) error) FileHandle {
	return &MemoryFileHandle{data, onWrite, onFlush}
}

func (h *MemoryFileHandle) ReadAt(p []byte, off int64) (n int, err error) {
	if off >= int64(len(h.data)) {
		return 0, io.EOF
	}
	n = copy(p, h.data[off:])
	return
}

func (h *MemoryFileHandle) WriteAt(p []byte, off int64) (n int, err error) {
	if off > int64(len(h.data)) {
		return 0, io.ErrShortWrite
	}
	for off+int64(len(p)) > int64(len(h.data)) {
		h.data = append(h.data, 0)
	}
	n = copy(h.data[off:], p)
	if h.onWrite != nil {
		err = h.onWrite(h.data, off, n)
	}
	return
}

func (h *MemoryFileHandle) Sync() error {
	if h.onFlush != nil {
		return h.onFlush(h.data)
	}
	return nil
}

func (h *MemoryFileHandle) Close() error {
	err := h.Sync()
	if err != nil {
		return err
	}
	h.data = nil
	return nil
}

///////////////////////////////////////////////////

// ToFS converts a ninep FileSystem to a fs.FS
func ToFS(f FileSystem) fs.FS { return &ioFS{f} }

type ioFS struct{ FileSystem }
type iofsFile struct {
	h      FileHandle
	info   func() (fs.FileInfo, error)
	offset int64
}

func (f *iofsFile) Stat() (fs.FileInfo, error) { return f.info() }

func (f *iofsFile) Read(p []byte) (int, error) {
	n, err := f.h.ReadAt(p, f.offset)
	f.offset += int64(n)
	return n, err
}

func (f *iofsFile) Close() error { return f.h.Close() }

var _ fs.ReadDirFS = (*ioFS)(nil)
var _ fs.StatFS = (*ioFS)(nil)

func (f *ioFS) Open(name string) (fs.File, error) {
	h, err := f.FileSystem.OpenFile(context.Background(), name, OREAD)
	if err != nil {
		return nil, err
	}
	return &iofsFile{
		h: h,
		info: func() (fs.FileInfo, error) {
			return f.FileSystem.Stat(context.Background(), name)
		},
	}, nil
}

func (f *ioFS) Stat(name string) (fs.FileInfo, error) {
	return f.FileSystem.Stat(context.Background(), name)
}

func (f *ioFS) ReadDir(name string) ([]fs.DirEntry, error) {
	var result []fs.DirEntry
	for info, err := range f.FileSystem.ListDir(context.Background(), name) {
		if err != nil {
			return nil, err
		} else {
			result = append(result, fs.FileInfoToDirEntry(info))
		}
	}
	return result, nil
}
