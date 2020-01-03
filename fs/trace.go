package fs

import (
	"fmt"
	"os"
	"strings"

	ninep "github.com/jeffh/cfs/ninep"
)

func TraceFs(fs ninep.FileSystem, l ninep.Loggable) ninep.FileSystem {
	if f, ok := fs.(ninep.WalkableFileSystem); ok {
		return &WalkableTraceFileSystem{
			TraceFileSystem{fs, l},
			f,
		}
	} else {
		return &TraceFileSystem{fs, l}
	}
}

// Loggable
type TraceFileHandle struct {
	H    ninep.FileHandle
	Path string
	ninep.Loggable
}

func (h *TraceFileHandle) ReadAt(p []byte, offset int64) (int, error) {
	n, err := h.H.ReadAt(p, offset)
	if err != nil {
		h.Tracef("File(%v).ReadAt(_, %v) => (%d, %s)", h.Path, offset, n, err)
		h.Errorf("File(%v).ReadAt(_, %v) => (%d, %s)", h.Path, offset, n, err)
	} else {
		h.Tracef("File(%v).ReadAt(_, %v) => (%d, nil)", h.Path, offset, n)
	}
	return n, err
}

func (h *TraceFileHandle) WriteAt(p []byte, offset int64) (int, error) {
	n, err := h.H.WriteAt(p, offset)
	if err != nil {
		h.Tracef("File(%v).WriteAt(len(%d), %v) => (%d, %s)", h.Path, len(p), offset, n, err)
		h.Errorf("File(%v).WriteAt(len(%d), %v) => (%d, %s)", h.Path, len(p), offset, n, err)
	} else {
		h.Tracef("File(%v).WriteAt(len(%d), %v) => (%d, nil)", h.Path, len(p), offset, n)
	}
	return n, err
}

func (h *TraceFileHandle) Sync() error {
	err := h.H.Sync()
	if err != nil {
		h.Tracef("File(%v).Sync() => %s", h.Path, err)
		h.Errorf("File(%v).Sync() => %s", h.Path, err)
	} else {
		h.Tracef("File(%v).Sync() => nil", h.Path)
	}
	return err
}

func (h *TraceFileHandle) Close() error {
	err := h.H.Close()
	if err != nil {
		h.Tracef("File(%v).Close() => %s", h.Path, err)
		h.Errorf("File(%v).Close() => %s", h.Path, err)
	} else {
		h.Tracef("File(%v).Close() => nil", h.Path)
	}
	return err
}

////////////////////

type TraceFileSystem struct {
	Fs ninep.FileSystem
	ninep.Loggable
}

func (f TraceFileSystem) MakeDir(path string, mode ninep.Mode) error {
	err := f.Fs.MakeDir(path, mode)
	f.Tracef("FS.MakeDir(%v, %s) => %s", path, mode, err)
	if err != nil {
		f.Errorf("FS.MakeDir(%v, %s) => %s", path, mode, err)
	}
	return err
}

func (f TraceFileSystem) CreateFile(path string, flag ninep.OpenMode, mode ninep.Mode) (ninep.FileHandle, error) {
	h, err := f.Fs.CreateFile(path, flag, mode)
	f.Tracef("FS.CreateFile(%v, %s, %s) => (%v, %s)", path, flag, mode, h, err)
	if err != nil || h == nil {
		f.Errorf("FS.CreateFile(%v, %s, %s) => (%v, %s)", path, flag, mode, h, err)
	}
	h = &TraceFileHandle{
		H:        h,
		Path:     path,
		Loggable: f.Loggable,
	}
	return h, err
}

func (f TraceFileSystem) OpenFile(path string, flag ninep.OpenMode) (ninep.FileHandle, error) {
	h, err := f.Fs.OpenFile(path, flag)
	f.Tracef("FS.OpenFile(%v, %s) => (%v, %s)", path, flag, h, err)
	if err != nil || h == nil {
		f.Errorf("FS.OpenFile(%v, %s) => (%v, %s)", path, flag, h, err)
	}
	h = &TraceFileHandle{
		H:        h,
		Path:     path,
		Loggable: f.Loggable,
	}
	return h, err
}

func (f TraceFileSystem) ListDir(path string) (ninep.FileInfoIterator, error) {
	itr, err := f.Fs.ListDir(path)
	var v []os.FileInfo
	if itr != nil {
		v, _ = ninep.FileInfoSliceFromIterator(itr, 21)
		itr.Reset()
	}
	if len(v) > 20 {
		sb := []string{}
		for _, j := range v[:20] {
			sb = append(sb, fmt.Sprintf("FileInfo[%#v]", j.Name()))
		}
		f.Tracef("FS.ListDir(%v) => (%#v..., %s)", path, strings.Join(sb, ", "), err)
	} else {
		sb := []string{}
		for _, j := range v {
			sb = append(sb, fmt.Sprintf("FileInfo[%#v]", j.Name()))
		}
		f.Tracef("FS.ListDir(%v) => (%#v, %s)", path, strings.Join(sb, ", "), err)
	}
	if err != nil {
		f.Errorf("FS.ListDir(%v) => (_, %s)", path, err)
	}
	return itr, err
}

func (f TraceFileSystem) Stat(path string) (os.FileInfo, error) {
	info, err := f.Fs.Stat(path)
	if info != nil {
		f.Tracef("FS.Stat(%v) => (os.FileInfo{name: %#v, size: %d...}, %s)", path, info.Name(), info.Size(), err)
	} else {
		f.Tracef("FS.Stat(%v) => (nil, %s)", path, err)
	}
	if err != nil {
		f.Errorf("FS.Stat(%v) => (_, %s)", path, err)
	}
	return info, err
}

func (f TraceFileSystem) WriteStat(path string, s ninep.Stat) error {
	f.Tracef("FS.WriteStat(%v, %s)", path, s)
	err := f.Fs.WriteStat(path, s)
	if err != nil {
		f.Errorf("FS.WriteStat(%v, %s) => %s", path, s, err)
	}
	return err
}

func (f TraceFileSystem) Delete(path string) error {
	f.Tracef("FS.Delete(%v)", path)
	err := f.Fs.Delete(path)
	if err != nil {
		f.Errorf("FS.Delete(%v) => %s", path, err)
	}
	return err
}

func (f TraceFileSystem) DeleteWithMode(path string, mode ninep.Mode) error {
	if fs, ok := f.Fs.(ninep.DeleteWithModeFileSystem); ok {
		f.Tracef("FS.DeleteWithMode(%v, %s)", path, mode)
		err := fs.DeleteWithMode(path, mode)
		if err != nil {
			f.Errorf("FS.DeleteWithMode(%v, %s) => %s", path, mode, err)
		}
		return err
	} else {
		return f.Delete(path)
	}
}

////////////////////

type WalkableTraceFileSystem struct {
	TraceFileSystem
	Wfs ninep.WalkableFileSystem
}

var _ ninep.WalkableFileSystem = (*WalkableTraceFileSystem)(nil)

func (f *WalkableTraceFileSystem) Walk(parts []string) ([]os.FileInfo, error) {
	f.TraceFileSystem.Tracef("FS.Walk(%v)", parts)
	infos, err := f.Wfs.Walk(parts)
	if err != nil {
		f.TraceFileSystem.Errorf("FS.Walk(%v) => %s", parts, err)
	}
	return infos, err
}
