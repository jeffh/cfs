package mirrorfs

import (
	"context"
	"io/fs"
	"iter"
	"os"
	"path/filepath"

	"github.com/jeffh/cfs/fs/proxy"
	"github.com/jeffh/cfs/ninep"
)

var skippableErrors []error

func init() {
	skippableErrors = []error{
		ninep.ErrWriteNotAllowed,
		ninep.ErrReadNotAllowed,
		os.ErrNotExist,
	}
}

type WALOperationKind int

const (
	WALOpCreateFile WALOperationKind = iota
	WALOpMakeDir
	WALOpOpenFile
	WALOpWriteStat
	WALOpDelete
	WALOpFileWrite
	WALOpFileClose
)

type WALOperationState int

const (
	WALOpStateRequested WALOperationState = iota
	WALOpStateValid
	WALOpStateStarted
	WALOpStateFinished
)

type WALOperation struct {
	Id   uint64
	Kind WALOperationKind
	Path string

	// confirmed by other members
	State WALOperationState

	// create/open
	Flag ninep.OpenMode
	Mode ninep.Mode

	// write
	Data   []byte
	Offset int64
}

type mirrorFS struct {
	walFS proxy.FileSystemMount   // for WAL log
	fsms  []proxy.FileSystemMount // for reading/writing
}

var _ ninep.FileSystem = (*mirrorFS)(nil)

func (mfs *mirrorFS) MakeDir(ctx context.Context, path string, mode ninep.Mode) error {
	return ninep.ErrNotImplemented
	// for i, fsm := range mfs.fsms {
	// 	err := fsm.FS.MakeDir(filepath.Join(fsm.Prefix, path), mode)
	// 	if err != nil {
	// 		for j, f := range mfs.fsms {
	// 			if j >= i {
	// 				break
	// 			}
	// 			// best attempts
	// 			f.FS.Delete(filepath.Join(f.Prefix, path))
	// 		}
	// 		return err
	// 	}
	// }
	// return nil
}
func (mfs *mirrorFS) CreateFile(ctx context.Context, path string, flag ninep.OpenMode, mode ninep.Mode) (ninep.FileHandle, error) {
	return nil, ninep.ErrNotImplemented
	// hs := make([]ninep.FileHandle, 0, len(mfs.fsms))
	// for i, fsm := range mfs.fsms {
	// 	h, err := fsm.FS.CreateFile(filepath.Join(fsm.Prefix, path), flag, mode)
	// 	if err != nil {
	// 		for j, f := range mfs.fsms {
	// 			if j >= i {
	// 				break
	// 			}
	// 			// best attempts
	// 			f.FS.Delete(filepath.Join(f.Prefix, path))
	// 		}
	// 		return nil, err
	// 	}
	// 	hs = append(hs, h)
	// }
	// return &fanoutHandle{mfs.fsms, hs, path}, nil
}
func (mfs *mirrorFS) OpenFile(ctx context.Context, path string, flag ninep.OpenMode) (ninep.FileHandle, error) {
	return nil, ninep.ErrNotImplemented
}
func (mfs *mirrorFS) ListDir(ctx context.Context, path string) iter.Seq2[fs.FileInfo, error] {
	return ninep.FileInfoErrorIterator(ninep.ErrNotImplemented)
	// uitr := makeUnionIterator(path, mfs.fsms)
	// if !uitr.hasDir() {
	// 	return nil, os.ErrNotExist
	// }
	// return uitr, nil
}
func (mfs *mirrorFS) Stat(ctx context.Context, path string) (os.FileInfo, error) {
	for _, fsm := range mfs.fsms {
		fi, err := fsm.FS.Stat(ctx, filepath.Join(fsm.Prefix, path))
		if err == os.ErrNotExist {
			continue
		} else if err != nil {
			return fi, err
		} else {
			return fi, nil
		}
	}
	return nil, os.ErrNotExist
}
func (mfs *mirrorFS) WriteStat(ctx context.Context, path string, s ninep.Stat) error {
	return os.ErrNotExist
}
func (mfs *mirrorFS) Delete(ctx context.Context, path string) error {
	for _, fsm := range mfs.fsms {
		err := fsm.FS.Delete(ctx, filepath.Join(fsm.Prefix, path))
		if err == os.ErrNotExist {
			continue
		} else if err != nil {
			return err
		} else {
			return nil
		}
	}
	return os.ErrNotExist
}

// New creates a new mirror fs - where two directories in two or more file
// servers are mirrored.
//
// MirrorFS assumes it controls each given mount point. There is no defined
// behavior if one file system initially differs from another.
//
// For any read operation, any file system is used that is up-to-date with the
// WAL.
//
// For any write operation, they are first written to the WAL before the
// operation proceeds. Writes will perform as fast as the slowest writer.
//
// Connecting to each of the file systems performs the a WAL consistency check
// before returning.
func New(walfs proxy.FileSystemMount, fses []proxy.FileSystemMount) ninep.FileSystem {
	return &mirrorFS{walfs, fses}
}
