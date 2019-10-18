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
	cpu "github.com/shirou/gopsutil/cpu"
	psnet "github.com/shirou/gopsutil/net"
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
			/proc/<pid>/rlimit - returns process limits
			/proc/<pid>/rlimit.json - returns process limits as json
			/proc/<pid>/fd - returns open file descriptors
			/proc/<pid>/fd.json - returns open file descriptors as json
			/proc/<pid>/net - returns open network connections
			/proc/<pid>/net.json - returns open network connections as json
			/proc/<pid>/threads - returns thread information
			/proc/<pid>/threads.json - returns thread information as json

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
	runs := func(f func() ([]byte, error)) func() (ninep.FileHandle, error) {
		return func() (ninep.FileHandle, error) {
			b, err := f()
			return &ninep.ReadOnlyMemoryFileHandle{b}, err
		}
	}

	{
		files = append(files, ninep.NewSimpleFile("cwd", 0, now, runs(func() ([]byte, error) {
			cwd, err := proc.Cwd()
			return []byte(cwd + "\n"), err
		})))
	}
	{
		files = append(files, ninep.NewSimpleFile("uids", 0, now, runs(func() ([]byte, error) {
			uids, err := proc.Uids()
			if err != nil {
				return nil, err
			} else {
				uidstr := make([]string, 0, len(uids)*2)
				for _, uid := range uids {
					uid_str := strconv.Itoa(int(uid))
					var username string
					usr, err := user.LookupId(uid_str)
					if err != nil {
						username = fmt.Sprintf("error: %s", err.Error())
					} else {
						username = usr.Username
					}
					uidstr = append(uidstr, fmt.Sprintf("%s %s", uid_str, username))
				}
				return []byte(strings.Join(uidstr, "\n")), nil
			}
		})))
	}
	{
		files = append(files, ninep.NewSimpleFile("gids", 0, now, runs(func() ([]byte, error) {
			gids, err := proc.Gids()
			if err != nil {
				return nil, err
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
				return []byte(strings.Join(gidstr, "\n")), nil
			}
		})))
	}
	{
		files = append(files, ninep.NewSimpleFile("cmdline", 0, now, runs(func() ([]byte, error) {
			cmd, err := proc.Cmdline()
			return []byte(cmd), err
		})))
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
		Stats := func() (ProcStats, error) {
			var (
				stat ProcStats

				err      error
				finalErr error
			)
			{
				cmd, err := proc.CmdlineSlice()
				if err != nil {
					finalErr = err
				}
				stat.Name = cmd[0]
				stat.Args = cmd[1:]
			}
			{
				var status string
				status, err = proc.Status()
				if err != nil {
					finalErr = err
				} else {
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
				stat.Status = status
			}
			{
				cpu, err := proc.CPUPercent()
				if err != nil {
					cpu = -1
					finalErr = err
				}
				stat.CPU = cpu
			}
			{
				mem, err := proc.MemoryPercent()
				if err != nil {
					mem = -1
					finalErr = err
				}
				stat.Mem = mem
			}
			{
				mem, err := proc.MemoryInfo()
				if err != nil {
					stat.MemRSS = 0
					stat.MemVMS = 0
					stat.MemSwap = 0
					finalErr = err
				} else {
					stat.MemRSS = mem.RSS
					stat.MemVMS = mem.VMS
					stat.MemSwap = mem.Swap
				}
			}
			return stat, finalErr
		}
		{
			files = append(files, ninep.NewSimpleFile("status", 0, now, runs(func() ([]byte, error) {
				stat, err := Stats()
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
				return []byte(txt), err
			})))

			files = append(files, ninep.NewSimpleFile("status.json", 0, now, runs(func() ([]byte, error) {
				stat, err := Stats()
				finalErr := err
				encodedJson, err := json.Marshal(stat)
				if finalErr == nil {
					finalErr = err
				}
				return encodedJson, finalErr
			})))
		}
		{
			files = append(files, ninep.NewSimpleFile("rlimit", 0, now, runs(func() ([]byte, error) {
				var contents []string
				rlimit, err := proc.Rlimit()
				for _, r := range rlimit {
					contents = append(contents, fmt.Sprintf("resource %d\nsoft%d\nhard %d\nused %d\n", r.Resource, r.Soft, r.Hard, r.Used))
				}
				return []byte(strings.Join(contents, "\n")), err
			})))

			files = append(files, ninep.NewSimpleFile("rlimit.json", 0, now, runs(func() ([]byte, error) {
				rlimit, err := proc.Rlimit()
				finalErr := err

				type Res struct {
					Limits []process.RlimitStat `json:"limits"`
				}

				encodedJson, err := json.Marshal(Res{rlimit})
				if finalErr == nil {
					finalErr = err
				}
				return encodedJson, finalErr
			})))
		}
		{
			files = append(files, ninep.NewSimpleFile("fd", 0, now, runs(func() ([]byte, error) {
				var contents []string
				fds, err := proc.OpenFiles()
				for _, fd := range fds {
					contents = append(contents, fmt.Sprintf("%d %s", fd.Fd, fd.Path))
				}
				return []byte(strings.Join(contents, "\n")), err
			})))

			files = append(files, ninep.NewSimpleFile("fd.json", 0, now, runs(func() ([]byte, error) {
				fds, err := proc.OpenFiles()

				type Res struct {
					Fds []process.OpenFilesStat `json:"fds"`
				}

				encodedJson, err := json.Marshal(Res{fds})
				return encodedJson, err
			})))
		}
		{
			files = append(files, ninep.NewSimpleFile("net", 0, now, runs(func() ([]byte, error) {
				var contents []string
				conns, err := proc.Connections()
				for _, c := range conns {
					contents = append(contents, fmt.Sprintf(
						"%d %d %d %s:%d %s:%d %s %d", c.Fd, c.Family, c.Type, c.Laddr.IP, c.Laddr.Port, c.Raddr.IP, c.Raddr.Port, c.Status, c.Pid,
					))
				}

				return []byte(strings.Join(contents, "\n")), err
			})))

			files = append(files, ninep.NewSimpleFile("net.json", 0, now, runs(func() ([]byte, error) {
				conns, err := proc.Connections()
				finalErr := err
				type Res struct {
					Connections []psnet.ConnectionStat `json:"connections"`
				}

				encodedJson, err := json.Marshal(Res{conns})
				if finalErr == nil {
					finalErr = err
				}

				return encodedJson, err
			})))
		}
		{
			files = append(files, ninep.NewSimpleFile("threads", 0, now, runs(func() ([]byte, error) {
				var contents []string
				threads, err := proc.Threads()
				contents = append(contents, "tid cpu user sys idle nice iowait irq softirq steal guest guestNice")
				for tid, t := range threads {
					contents = append(contents, fmt.Sprintf(
						"%d %s %f %f %f %f %f %f %f %f %f", tid, t.CPU, t.User, t.System, t.Nice, t.Iowait, t.Irq, t.Softirq, t.Steal, t.Guest, t.GuestNice,
					))
				}

				return []byte(strings.Join(contents, "\n")), err
			})))

			files = append(files, ninep.NewSimpleFile("threads.json", 0, now, runs(func() ([]byte, error) {
				threads, err := proc.Threads()
				finalErr := err
				type Res struct {
					Threads map[int32]*cpu.TimesStat `json:"threads"`
				}

				encodedJson, err := json.Marshal(Res{threads})
				if finalErr == nil {
					finalErr = err
				}

				return encodedJson, err
			})))
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
