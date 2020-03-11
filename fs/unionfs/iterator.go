package unionfs

import (
	"context"
	"io"
	"os"
	"path/filepath"

	"github.com/jeffh/cfs/fs/proxy"
	"github.com/jeffh/cfs/ninep"
)

type unionIterator struct {
	it      ninep.FileInfoIterator
	lastErr error

	fsmsOffset int
	fsms       []proxy.FileSystemMount
	seenPaths  map[string]bool

	path string
	ctx  context.Context
}

func makeUnionIterator(ctx context.Context, path string, fsms []proxy.FileSystemMount) *unionIterator {
	return &unionIterator{path: path, fsms: fsms, ctx: ctx}
}

var _ ninep.FileInfoIterator = (*unionIterator)(nil)

func (itr *unionIterator) seen(path string) bool {
	if itr.seenPaths == nil {
		itr.seenPaths = make(map[string]bool)
	}
	_, ok := itr.seenPaths[path]
	itr.seenPaths[path] = true
	return ok
}

func (itr *unionIterator) hasDir() bool {
	for _, fsms := range itr.fsms {
		_, err := fsms.FS.Stat(itr.ctx, filepath.Join(fsms.Prefix, itr.path))
		if err == nil {
			return true
		}
	}
	return false
}

func (itr *unionIterator) NextFileInfo() (fi os.FileInfo, err error) {
	for {
		err = itr.lastErr
		if err == io.EOF {
			itr.Close()
			itr.it = nil
			if itr.fsmsOffset >= len(itr.fsms) {
				return nil, io.EOF
			}
		} else if err != nil {
			return
		}
		if itr.it == nil {
			fsm := itr.fsms[itr.fsmsOffset]
			itr.it, err = fsm.FS.ListDir(itr.ctx, filepath.Join(fsm.Prefix, itr.path))
			if err != nil {
				return
			}
			itr.fsmsOffset++
		}
		fi, itr.lastErr = itr.it.NextFileInfo()
		if fi != nil {
			fpath := filepath.Join(itr.path, fi.Name())
			if itr.seen(fpath) {
				continue
			}
		}

		return
	}
}

func (itr *unionIterator) Reset() error {
	if itr.it != nil {
		if err := itr.it.Close(); err != nil {
			return err
		}
	}
	itr.it = nil
	itr.fsmsOffset = 0
	itr.lastErr = nil
	itr.seenPaths = nil
	return nil
}

func (itr *unionIterator) Close() error {
	if itr.it != nil {
		return itr.it.Close()
	}
	itr.it = nil
	itr.lastErr = nil
	itr.seenPaths = nil
	return nil
}
