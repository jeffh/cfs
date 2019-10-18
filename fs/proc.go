package fs

import (
	"encoding/json"
	"fmt"
	"os"
	"os/user"
	"strconv"
	"strings"
	"time"

	ninep "github.com/jeffh/cfs/ninep"
	"github.com/shirou/gopsutil/process"
)

type Proc struct{}

func (f *Proc) findPid(pid int32) (*process.Process, error) {
	proc, err := process.NewProcess(pid)
	if err != nil {
		return nil, os.ErrNotExist
	}
	return proc, nil
}

func (f *Proc) pidDir(proc *process.Process) ([]os.FileInfo, error) {
	// for thread safety, recreate the process struct
	files := make([]os.FileInfo, 0, 4)
	now := time.Now()
	/*
		Note: this method assumes we're in /proc/<pid>/

		Files:
			/proc/<pid>/cmdline - arguments that were called with the program
			/proc/<pid>/cwd - current working directory of the process
			/proc/<pid>/status - returns general process stats, separated by newlines
			/proc/<pid>/status.json - returns general process stats as json
			/proc/<pid>/uids - returns uids and usernames
			/proc/<pid>/gids - returns gids and group names

			/proc/<pid>/ctl
			/proc/<pid>/fd
			/proc/<pid>/mem
			/proc/<pid>/note
			/proc/<pid>/noteid
			/proc/<pid>/notepg
			/proc/<pid>/ns
			/proc/<pid>/proc
			/proc/<pid>/profile
			/proc/<pid>/regs
			/proc/<pid>/segment
			/proc/<pid>/status
			/proc/<pid>/text
			/proc/<pid>/wait
	*/
	{
		cwd, err := proc.Cwd()
		if err != nil {
			cwd = fmt.Sprintf("error: %s", err.Error())
		}
		cwd = cwd + "\n"
		files = append(files, ninep.NewReadOnlySimpleFile("cwd", 0, now, []byte(cwd)))
	}
	{
		uids, err := proc.Uids()
		if err != nil {
			panic(err)
			files = append(files, ninep.NewReadOnlySimpleFile("uids", 0, now, []byte(fmt.Sprintf("error: %s", err.Error()))))
		} else {
			uidstr := make([]string, 0, len(uids)*2)
			for _, uid := range uids {
				uid_str := strconv.Itoa(int(uid))
				var username string
				usr, err := user.LookupId(uid_str)
				if err != nil {
					panic(err)
					username = fmt.Sprintf("error: %s", err.Error())
				} else {
					username = usr.Username
				}
				uidstr = append(uidstr, fmt.Sprintf("%s %s", uid_str, username))
			}
			files = append(files, ninep.NewReadOnlySimpleFile("uids", 0, now, []byte(strings.Join(uidstr, "\n"))))
		}
	}
	{
		gids, err := proc.Gids()
		if err != nil {
			files = append(files, ninep.NewReadOnlySimpleFile("gids", 0, now, []byte(fmt.Sprintf("error: %s", err.Error()))))
		} else {
			gidstr := make([]string, 0, len(gids)*2)
			for _, gid := range gids {
				gid_str := strconv.Itoa(int(gid))
				var username string
				usr, err := user.LookupGroupId(gid_str)
				if err != nil {
					username = fmt.Sprintf("error: %s", err.Error())
				} else {
					username = usr.Name
				}
				gidstr = append(gidstr, fmt.Sprintf("%s %s", gid_str, username))
			}
			files = append(files, ninep.NewReadOnlySimpleFile("gids", 0, now, []byte(strings.Join(gidstr, "\n"))))
		}
	}
	{
		type ProcStats struct {
			Name    string   `json:"name"`
			Args    []string `json:"args"`
			Status  string   `json:"status"`
			CPU     float64  `json:"cpu_percent"`    // %
			Mem     float32  `json:"memory_percent"` // %
			MemRSS  uint64   `json:"memory_rss"`     // bytes
			MemVMS  uint64   `json:"memory_vms"`     // bytes
			MemSwap uint64   `json:"memory_swap"`    // bytes
		}
		var (
			stat ProcStats

			err error
		)
		{
			cmd, err := proc.CmdlineSlice()
			if err != nil {
				cmd = []string{fmt.Sprintf("error: %s", err)}
			}
			files = append(files, ninep.NewReadOnlySimpleFile("cmdline", 0, now, []byte(strings.Join(cmd, " "))))
			stat.Name = cmd[0]
			stat.Args = cmd[1:]
		}
		{
			var status string
			status, err = proc.Status()
			if err != nil {
				status = fmt.Sprintf("error: %s", err.Error())
			} else {
				fmt.Printf("Status: %s", status)
				switch status[0] {
				case 'R':
					status = "running"
				case 'S':
					status = "sleeping"
				case 'T':
					status = "stopped"
				case 'I':
					status = "idle"
				case 'Z':
					status = "zombied"
				case 'W':
					status = "waiting"
				case 'L':
					status = "locked"
				default:
					status = "unknown"
				}
			}
			stat.Status = status + "\n"
		}
		{
			cpu, err := proc.CPUPercent()
			if err != nil {
				cpu = -1
			}
			stat.CPU = cpu
		}
		{
			mem, err := proc.MemoryPercent()
			if err != nil {
				mem = -1
			}
			stat.Mem = mem
		}
		{
			mem, err := proc.MemoryInfo()
			if err != nil {
				stat.MemRSS = 0
				stat.MemVMS = 0
				stat.MemSwap = 0
			} else {
				stat.MemRSS = mem.RSS
				stat.MemVMS = mem.VMS
				stat.MemSwap = mem.Swap
			}
		}
		{
			txt := fmt.Sprintf(
				"%s\n%s\n%s\ncpu%% %f\nmem%% %f\nrss %d\nvms %d\nswap %d\n",
				stat.Name,
				strings.Join(stat.Args, " "),
				stat.Status,
				stat.CPU,
				stat.Mem,
				stat.MemRSS,
				stat.MemVMS,
				stat.MemSwap,
			)

			files = append(files, ninep.NewReadOnlySimpleFile("status", 0, now, []byte(txt)))

			encodedJson, err := json.Marshal(stat)
			if err != nil {
				panic(err)
				return nil, err
			}
			files = append(files, ninep.NewReadOnlySimpleFile("status.json", 0, now, encodedJson))
		}
	}
	return files, nil
}

func (f *Proc) MakeDir(path string, mode ninep.Mode) error { return ninep.ErrUnsupported }
func (f *Proc) CreateFile(path string, flag ninep.OpenMode, mode ninep.Mode) (ninep.FileHandle, error) {
	return nil, ninep.ErrUnsupported
}
func (f *Proc) OpenFile(path string, flag ninep.OpenMode) (ninep.FileHandle, error) {
	parts := ninep.PathSplit(path)[1:]
	if len(parts) != 2 {
		return nil, os.ErrNotExist
	}

	pid, err := strconv.Atoi(parts[0])
	if err != nil {
		return nil, os.ErrNotExist
	}
	proc, err := f.findPid(int32(pid))
	if err != nil {
		return nil, os.ErrNotExist
	}

	parts = parts[1:]

	infos, err := f.pidDir(proc)
	if err != nil {
		return nil, err
	}

	for _, info := range infos {
		if info.Name() == parts[0] {
			return info.(*ninep.SimpleFile).Open()
		}
	}
	return nil, os.ErrNotExist
}
func (f *Proc) ListDir(path string) (ninep.FileInfoIterator, error) {
	if path == "/" {
		pids, err := process.Pids()
		if err != nil {
			return nil, err
		}
		infos := make([]os.FileInfo, len(pids))
		for i, pid := range pids {
			infos[i] = processPid{pid, time.Now()}
		}
		return ninep.FileInfoSliceIterator(infos), nil
	} else {
		parts := ninep.PathSplit(path)[1:]
		pid, err := strconv.Atoi(parts[0])
		if err != nil {
			return nil, os.ErrNotExist
		}
		proc, err := f.findPid(int32(pid))
		if err != nil {
			return nil, os.ErrNotExist
		}

		parts = parts[1:]

		if len(parts) == 0 {
			infos, err := f.pidDir(proc)
			return ninep.FileInfoSliceIterator(infos), err
		} else {
			return nil, os.ErrNotExist
		}
	}
}
func (f *Proc) Stat(path string) (os.FileInfo, error) {
	if path == "/" {
		info := &ninep.SimpleFileInfo{
			FIName:    "/",
			FISize:    0,
			FIMode:    os.ModeDir | 0555,
			FIModTime: time.Now(),
		}
		return info, nil
	} else {
		parts := ninep.PathSplit(path)[1:]
		pid, err := strconv.Atoi(parts[0])
		if err != nil {
			return nil, os.ErrNotExist
		}
		proc, err := f.findPid(int32(pid))
		if err != nil {
			return nil, os.ErrNotExist
		}

		parts = parts[1:]

		if len(parts) == 0 {
			info := &ninep.SimpleFileInfo{
				FIName:    strconv.Itoa(pid),
				FISize:    0,
				FIMode:    os.ModeDir | 0555,
				FIModTime: time.Now(),
			}
			return info, nil
		} else {
			infos, err := f.pidDir(proc)
			if err != nil {
				return nil, err
			}

			for _, info := range infos {
				if info.Name() == parts[0] {
					return info, nil
				}
			}
			return nil, os.ErrNotExist
		}

		return nil, ninep.ErrNotImplemented
	}
	return nil, nil
}
func (f *Proc) WriteStat(path string, s ninep.Stat) error { return ninep.ErrUnsupported }
func (f *Proc) Delete(path string) error                  { return ninep.ErrUnsupported }

//////////////////////////////////////

type processPid struct {
	Pid     int32
	modTime time.Time
}

func (f processPid) Name() string       { return strconv.Itoa(int(f.Pid)) }
func (f processPid) Size() int64        { return 0 }
func (f processPid) Mode() os.FileMode  { return os.ModeDir }
func (f processPid) ModTime() time.Time { return f.modTime }
func (f processPid) IsDir() bool        { return true }
func (f processPid) Sys() interface{}   { return nil }
