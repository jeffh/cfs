package unionfs

import (
	"os"
	"path/filepath"
	"strings"

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

type FileSystemMount struct {
	FS     ninep.FileSystem
	Prefix string
}

type FileSystemMountConfig struct {
	Addr   string
	Prefix string
}

func ParseMounts(args []string) []FileSystemMountConfig {
	fsmc := make([]FileSystemMountConfig, 0, len(args))
	for _, arg := range args {
		parts := strings.Split(arg, "/")
		count := len(parts)
		if count == 1 {
			fsmc = append(fsmc, FileSystemMountConfig{Addr: parts[0]})
		} else if count >= 2 {
			fsmc = append(fsmc, FileSystemMountConfig{
				Addr:   parts[0],
				Prefix: parts[1],
			})
		}
	}
	return fsmc
}

type unionFS struct {
	fsms  []FileSystemMount // for reading
	wfsms []FileSystemMount // for writing
}

var _ ninep.FileSystem = (*unionFS)(nil)

func (ufs *unionFS) MakeDir(path string, mode ninep.Mode) error {
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
func (ufs *unionFS) CreateFile(path string, flag ninep.OpenMode, mode ninep.Mode) (ninep.FileHandle, error) {
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
func (ufs *unionFS) OpenFile(path string, flag ninep.OpenMode) (ninep.FileHandle, error) {
	targetFSMs := ufs.wfsms
	if flag.IsReadOnly() {
		targetFSMs = ufs.fsms
	}

	hs := make([]ninep.FileHandle, 0, len(targetFSMs))
	fsms := make([]FileSystemMount, 0, len(targetFSMs))

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
func (ufs *unionFS) ListDir(path string) (ninep.FileInfoIterator, error) {
	uitr := makeUnionIterator(path, ufs.fsms)
	if !uitr.hasDir() {
		return nil, os.ErrNotExist
	}
	return uitr, nil
}
func (ufs *unionFS) Stat(path string) (os.FileInfo, error) {
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
func (ufs *unionFS) WriteStat(path string, s ninep.Stat) error {
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
func (ufs *unionFS) Delete(path string) error {
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

// NewBasicUnionFS creates a new Plan9-styled union fs - only top-level directory is unioned.
// File systems are accessed in order until one file system can provide. This means the top-most
// directories of each file system is joined. File and directories in each
// top-level directory are shared.
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
func NewUnionFS(fses []FileSystemMount) ninep.FileSystem {
	return &unionFS{fses, []FileSystemMount{fses[0]}}
}
