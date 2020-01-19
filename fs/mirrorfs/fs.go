package mirrorfs

import (
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
	Flag ninep.Flag
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

func (ufs *mirrorFS) MakeDir(path string, mode ninep.Mode) error {
	for i, fsm := range ufs.wfsms {
		err := fsm.FS.MakeDir(filepath.Join(fsm.Prefix, path), mode)
		if err != nil {
			for j, f := range ufs.wfsms {
				if j >= i {
					break
				}
				// best attempts
				f.FS.Delete(filepath.Join(f.Prefix, path))
			}
			return err
		}
	}
	return nil
}
func (ufs *mirrorFS) CreateFile(path string, flag ninep.OpenMode, mode ninep.Mode) (ninep.FileHandle, error) {
	hs := make([]ninep.FileHandle, 0, len(ufs.wfsms))
	for i, fsm := range ufs.wfsms {
		h, err := fsm.FS.CreateFile(filepath.Join(fsm.Prefix, path), flag, mode)
		if err != nil {
			for j, f := range ufs.wfsms {
				if j >= i {
					break
				}
				// best attempts
				f.FS.Delete(filepath.Join(f.Prefix, path))
			}
			return nil, err
		}
		hs = append(hs, h)
	}
	return &fanoutHandle{ufs.wfsms, hs, path}, nil
}
func (ufs *mirrorFS) OpenFile(path string, flag ninep.OpenMode) (ninep.FileHandle, error) {
	targetFSMs := ufs.wfsms
	if flag.IsReadOnly() {
		targetFSMs = ufs.fsms
	}

	hs := make([]ninep.FileHandle, 0, len(targetFSMs))
	fsms := make([]proxy.FileSystemMount, 0, len(targetFSMs))

	for i, fsm := range targetFSMs {
		h, err := fsm.FS.OpenFile(filepath.Join(fsm.Prefix, path), flag)
		if err != nil {
			continue
		}
		hs = append(hs, h)
		fsms = append(fsms, targetFSMs[i])
	}
	if len(hs) == 0 {
		return nil, os.ErrNotExist
	}
	return &fanoutHandle{fsms, hs, path}, nil
}
func (ufs *mirrorFS) ListDir(path string) (ninep.FileInfoIterator, error) {
	uitr := makeUnionIterator(path, ufs.fsms)
	if !uitr.hasDir() {
		return nil, os.ErrNotExist
	}
	return uitr, nil
}
func (ufs *mirrorFS) Stat(path string) (os.FileInfo, error) {
	for _, fsm := range ufs.fsms {
		fi, err := fsm.FS.Stat(filepath.Join(fsm.Prefix, path))
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
func (ufs *mirrorFS) WriteStat(path string, s ninep.Stat) error {
	for _, fsm := range ufs.fsms {
		err := fsm.FS.WriteStat(filepath.Join(fsm.Prefix, path), s)
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
func (ufs *mirrorFS) Delete(path string) error {
	for _, fsm := range ufs.fsms {
		err := fsm.FS.Delete(filepath.Join(fsm.Prefix, path))
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

// New creates a new mirror fs - where two directories in two file servers are mirrored.
//
// THis means each file system will receive writes and one will receive a read.
// More underlying file systems will slow down writes since all needs to be
// conformed.
//
// As an implementation detail, mirrorfs creates a dir call .mirrorfs where all
// internal book-keeping belongs. Primarily information about WALs.
//
// For example, given [fs1, fs2]:
//
//    where fs1:
//      - /bin/a
//      - /bin/b
//      - /bin/dir/a
//      - /bin/dir/b
//
//    and where fs2:
//      - /bin/b
//      - /bin/c
//      - /bin/dir/b
//      - /bin/dir/c
//
// Then the union file system will provide:
//   - /bin/a # from fs1
//   - /bin/b # from fs1
//   - /bin/c # from fs2
//   - /bin/dir/a # from fs1
//   - /bin/dir/b # from fs1
//   - /bin/dir/c # from fs2
//
// All writes occur on the first file system (fs1) in the slice.
//
func New(fses []proxy.FileSystemMount) ninep.FileSystem {
	return &mirrorFs{fses}
}
