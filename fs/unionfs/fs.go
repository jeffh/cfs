package unionfs

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

type unionFS struct {
	fsms  []proxy.FileSystemMount // for reading
	wfsms []proxy.FileSystemMount // for writing
}

var _ ninep.FileSystem = (*unionFS)(nil)

func (ufs *unionFS) MakeDir(ctx context.Context, path string, mode ninep.Mode) error {
	for i, fsm := range ufs.wfsms {
		err := fsm.FS.MakeDir(ctx, filepath.Join(fsm.Prefix, path), mode)
		if err != nil {
			for j, f := range ufs.wfsms {
				if j >= i {
					break
				}
				// best attempts
				f.FS.Delete(ctx, filepath.Join(f.Prefix, path))
			}
			return err
		}
	}
	return nil
}
func (ufs *unionFS) CreateFile(ctx context.Context, path string, flag ninep.OpenMode, mode ninep.Mode) (ninep.FileHandle, error) {
	hs := make([]ninep.FileHandle, 0, len(ufs.wfsms))
	for i, fsm := range ufs.wfsms {
		h, err := fsm.FS.CreateFile(ctx, filepath.Join(fsm.Prefix, path), flag, mode)
		if err != nil {
			for j, f := range ufs.wfsms {
				if j >= i {
					break
				}
				// best attempts
				f.FS.Delete(ctx, filepath.Join(f.Prefix, path))
			}
			return nil, err
		}
		hs = append(hs, h)
	}
	return &fanoutHandle{ufs.wfsms, hs, path}, nil
}
func (ufs *unionFS) OpenFile(ctx context.Context, path string, flag ninep.OpenMode) (ninep.FileHandle, error) {
	targetFSMs := ufs.wfsms
	if flag.IsReadOnly() {
		targetFSMs = ufs.fsms
	}

	hs := make([]ninep.FileHandle, 0, len(targetFSMs))
	fsms := make([]proxy.FileSystemMount, 0, len(targetFSMs))

	for i, fsm := range targetFSMs {
		h, err := fsm.FS.OpenFile(ctx, filepath.Join(fsm.Prefix, path), flag)
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
func (ufs *unionFS) ListDir(ctx context.Context, path string) iter.Seq2[fs.FileInfo, error] {
	hasDir := func(ctx context.Context, path string, ufs *unionFS) bool {
		for _, fsms := range ufs.fsms {
			_, err := fsms.FS.Stat(ctx, filepath.Join(fsms.Prefix, path))
			if err == nil {
				return true
			}
		}
		return false
	}
	if !hasDir(ctx, path, ufs) {
		return ninep.FileInfoErrorIterator(os.ErrNotExist)
	}
	return makeUnionIterator(ctx, path, ufs.fsms)
}
func (ufs *unionFS) Stat(ctx context.Context, path string) (os.FileInfo, error) {
	for _, fsm := range ufs.fsms {
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
func (ufs *unionFS) WriteStat(ctx context.Context, path string, s ninep.Stat) error {
	for _, fsm := range ufs.fsms {
		err := fsm.FS.WriteStat(ctx, filepath.Join(fsm.Prefix, path), s)
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
func (ufs *unionFS) Delete(ctx context.Context, path string) error {
	for _, fsm := range ufs.fsms {
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

// New creates a new union fs - where all directories are unioned.
// File systems are accessed in order until one file system can provide. This means the top-most
// directories of each file system is joined. File and directories in each
// top-level directory are shared.
//
// For example, given [fs1, fs2]:
//
//	where fs1:
//	  - /bin/a
//	  - /bin/b
//	  - /bin/dir/a
//	  - /bin/dir/b
//
//	and where fs2:
//	  - /bin/b
//	  - /bin/c
//	  - /bin/dir/b
//	  - /bin/dir/c
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
func New(fses []proxy.FileSystemMount) ninep.FileSystem {
	return &unionFS{fses, []proxy.FileSystemMount{fses[0]}}
}
