package unionfs

import (
	"io"

	"github.com/jeffh/cfs/fs/proxy"
	"github.com/jeffh/cfs/ninep"
)

type fanoutHandle struct {
	fsms []proxy.FileSystemMount
	hs   []ninep.FileHandle
	path string
}

var _ ninep.FileHandle = (*fanoutHandle)(nil)

func (f *fanoutHandle) ReadAt(p []byte, offset int64) (n int, err error) {
	for _, h := range f.hs {
		n, err = h.ReadAt(p, offset)
		if err == nil || err == io.EOF {
			return
		}
	}
	return
}

func (f *fanoutHandle) WriteAt(p []byte, offset int64) (size int, err error) {
	for _, h := range f.hs {
		size, err = h.WriteAt(p, offset)
		if err != nil {
			// TODO: revert!
			// for j, hPrime := range f.hs {
			// 	if j >= i {
			// 		break
			// 	}
			// }
			// undo
			return
		}
	}
	return
}

func (f *fanoutHandle) Sync() (err error) {
	for _, h := range f.hs {
		e := h.Sync()
		if e != nil {
			err = e
		}
	}
	return
}

func (f *fanoutHandle) Close() (err error) {
	for _, h := range f.hs {
		e := h.Close()
		if e != nil {
			err = e
		}
	}
	return
}
