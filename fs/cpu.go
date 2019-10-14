package fs

import (
	"os"
	"time"

	ninep "github.com/jeffh/cfs/ninep"
	"github.com/shirou/gopsutil/process"
)

type Proc struct {
}

func (f *Proc) MakeDir(path string, mode ninep.Mode) error { return ninep.ErrUnsupported }
func (f *Proc) CreateFile(path string, flag ninep.OpenMode, mode ninep.Mode) (ninep.FileHandle, error) {
	return nil, ninep.ErrUnsupported
}
func (f *Proc) OpenFile(path string, flag ninep.OpenMode) (ninep.FileHandle, error) {
	return nil, ninep.ErrUnsupported
}
func (f *Proc) ListDir(path string) ([]os.FileInfo, error) {
	if path == "" {
		pids, err := process.Pids()
		if err != nil {
			return nil, err
		}
		ppids := make([]os.FileInfo, len(pids))
		for i, pid := range pids {
			ppids[i] = processPid(pid)
		}
		return ppids, nil
	} else {
		return nil, ninep.ErrNotImplemented
	}
}
func (f *Proc) Stat(path string) (os.FileInfo, error) {
	if path == "" {
	} else {
	}
	return nil, nil
}
func (f *Proc) WriteStat(path string, s ninep.Stat) error { return nil }
func (f *Proc) Delete(path string) error                  { return nil }

//////////////////////////////////////

type processPid int

func (f processPid) Name() string       { return string(f) }
func (f processPid) Size() int64        { return 0 }
func (f processPid) Mode() os.FileMode  { return os.ModeDir }
func (f processPid) ModTime() time.Time { return time.Now() }
func (f processPid) IsDir() bool        { return true }
func (f processPid) Sys() interface{}   { return nil }
