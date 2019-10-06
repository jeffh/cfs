package ninep

import (
	"context"
	"io"
	"os"
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

// Return nil, nil to indicate no authentication needed
type Authorizer interface {
	Auth(ctx context.Context, addr, user, access string) (AuthFileHandle, error)
}

type Authorizee interface {
	Prove(ctx context.Context, user, access string) error
}

// A higher-level interface to the Plan9 file system protocol (9P2000)
//
// The following assumptions are part of the interface:
// - paths can be empty strings (which indicates root directory)
type FileSystem interface {
	// Creates a directory. Implementations can reject if parent directories are missing
	MakeDir(path string, mode Mode) error
	// Creates a file and opens it for readng/writing
	CreateFile(path string, flag OpenMode, mode Mode) (FileHandle, error)
	// Opens an existing file for reading/writing
	OpenFile(path string, flag OpenMode) (FileHandle, error)
	// Lists directories and files in a given path. Does not include '.' or '..'
	ListDir(path string) ([]os.FileInfo, error)
	// Lists stats about a given file or directory.
	Stat(path string) (os.FileInfo, error)
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
	WriteStat(path string, s Stat) error
	// Deletes a file or directory. Implementations may reject directories that aren't empty
	Delete(path string) error
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

/////////////////////////////////////////////////

type handleReaderWriter struct {
	h      FileHandle
	offset int64
}

func (r *handleReaderWriter) Read(p []byte) (int, error) {
	n, err := r.h.ReadAt(p, r.offset)
	r.offset += int64(n)
	return n, err
}

func (r *handleReaderWriter) Write(p []byte) (int, error) {
	n, err := r.h.WriteAt(p, r.offset)
	r.offset += int64(n)
	return n, err
}

func Reader(h FileHandle) io.Reader { return &handleReaderWriter{h, 0} }
func Writer(h FileHandle) io.Writer { return &handleReaderWriter{h, 0} }
