package fs

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/user"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/google/shlex"
	ninep "github.com/jeffh/cfs/ninep"
	cpu "github.com/shirou/gopsutil/cpu"
	psnet "github.com/shirou/gopsutil/net"
	"github.com/shirou/gopsutil/process"
)

type Proc struct {
	ctl procCtlHandle

	cache idCache
}

var procCtlStat = &ninep.SimpleFileInfo{
	FIName: "ctl",
	FISize: 0,
	FIMode: os.ModeAppend | 0666,
}

func (f *Proc) findPid(pid int32) (*process.Process, error) {
	proc, err := process.NewProcess(pid)
	if err != nil {
		return nil, os.ErrNotExist
	}
	return proc, nil
}

func (f *Proc) pidDir(proc *process.Process) ([]os.FileInfo, error) {
	// for thread safety, recreate the process struct
	files := make([]os.FileInfo, 0, 15)
	now := time.Now()
	/*
		Note: this method assumes we're in /proc/<pid>/

		Files:
			/proc/ctl - run a command, separated by null terminator or newline
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
			/proc/<pid>/signal - write-only file to send unix signals to this process
			                     Use newline or null-terminated strings that are either signal integers or
								 one of the following named signals: SIGHUP,
								 SIGINT, SIGQUIT, SIGTRAP, SIGABRT, SIGKILL,
								 SIGALRM, SIGTERM, SIGSTOP, SIGCONT
	*/
	readOnly := func(f func() ([]byte, error)) func(m ninep.OpenMode) (ninep.FileHandle, error) {
		return func(m ninep.OpenMode) (ninep.FileHandle, error) {
			if !m.IsReadable() {
				return nil, ninep.ErrWriteNotAllowed
			}
			b, err := f()
			return &ninep.ReadOnlyMemoryFileHandle{b}, err
		}
	}

	writeOnly := func(bufSize int, f func([]byte) (int, error)) func(m ninep.OpenMode) (ninep.FileHandle, error) {
		return func(m ninep.OpenMode) (ninep.FileHandle, error) {
			if !m.IsWriteable() {
				return nil, ninep.ErrReadNotAllowed
			}
			h := &ninep.WriteOnlyFileHandle{
				Buf:     make([]byte, 0, bufSize),
				OnWrite: f,
			}
			return h, nil
		}
	}

	{
		files = append(files, ninep.NewSimpleFile("cwd", 0, now, readOnly(func() ([]byte, error) {
			cwd, err := proc.Cwd()
			return []byte(cwd + "\n"), err
		})))
	}
	{
		files = append(files, ninep.NewSimpleFile("uids", 0, now, readOnly(func() ([]byte, error) {
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
		files = append(files, ninep.NewSimpleFile("gids", 0, now, readOnly(func() ([]byte, error) {
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
		files = append(files, ninep.NewSimpleFile("cmdline", 0, now, readOnly(func() ([]byte, error) {
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
			files = append(files, ninep.NewSimpleFile("status", 0, now, readOnly(func() ([]byte, error) {
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

			files = append(files, ninep.NewSimpleFile("status.json", 0, now, readOnly(func() ([]byte, error) {
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
			files = append(files, ninep.NewSimpleFile("rlimit", 0, now, readOnly(func() ([]byte, error) {
				var contents []string
				rlimit, err := proc.Rlimit()
				for _, r := range rlimit {
					contents = append(contents, fmt.Sprintf("resource %d\nsoft%d\nhard %d\nused %d\n", r.Resource, r.Soft, r.Hard, r.Used))
				}
				return []byte(strings.Join(contents, "\n")), err
			})))

			files = append(files, ninep.NewSimpleFile("rlimit.json", 0, now, readOnly(func() ([]byte, error) {
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
			files = append(files, ninep.NewSimpleFile("fd", 0, now, readOnly(func() ([]byte, error) {
				var contents []string
				fds, err := proc.OpenFiles()
				for _, fd := range fds {
					contents = append(contents, fmt.Sprintf("%d %s", fd.Fd, fd.Path))
				}
				return []byte(strings.Join(contents, "\n")), err
			})))

			files = append(files, ninep.NewSimpleFile("fd.json", 0, now, readOnly(func() ([]byte, error) {
				fds, err := proc.OpenFiles()

				type Res struct {
					Fds []process.OpenFilesStat `json:"fds"`
				}

				encodedJson, err := json.Marshal(Res{fds})
				return encodedJson, err
			})))
		}
		{
			files = append(files, ninep.NewSimpleFile("net", 0, now, readOnly(func() ([]byte, error) {
				var contents []string
				conns, err := proc.Connections()
				for _, c := range conns {
					contents = append(contents, fmt.Sprintf(
						"%d %d %d %s:%d %s:%d %s %d", c.Fd, c.Family, c.Type, c.Laddr.IP, c.Laddr.Port, c.Raddr.IP, c.Raddr.Port, c.Status, c.Pid,
					))
				}

				return []byte(strings.Join(contents, "\n")), err
			})))

			files = append(files, ninep.NewSimpleFile("net.json", 0, now, readOnly(func() ([]byte, error) {
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
			files = append(files, ninep.NewSimpleFile("threads", 0, now, readOnly(func() ([]byte, error) {
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

			files = append(files, ninep.NewSimpleFile("threads.json", 0, now, readOnly(func() ([]byte, error) {
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
		{
			files = append(files, ninep.NewSimpleFile("signal", os.ModeAppend|0222, now, writeOnly(512, func(b []byte) (int, error) {
				sig, n, err := parseSignal(b)
				if err != nil {
					return n, err
				}
				if sig == 0 {
					return n, nil
				}

				err = proc.SendSignal(sig)

				return n, err
			})))
		}
	}
	return files, nil
}

func (f *Proc) MakeDir(path string, mode ninep.Mode) error { return ninep.ErrUnsupported }
func (f *Proc) CreateFile(path string, flag ninep.OpenMode, mode ninep.Mode) (ninep.FileHandle, error) {
	return f.OpenFile(path, flag)
}
func (f *Proc) OpenFile(path string, flag ninep.OpenMode) (ninep.FileHandle, error) {
	parts := ninep.PathSplit(path)[1:]
	switch len(parts) {
	case 1:
		if parts[0] == "ctl" {
			return &f.ctl, nil
		}
	case 2:
		pid, err := strconv.Atoi(parts[0])
		if err != nil {
			return nil, os.ErrNotExist
		}
		proc, err := f.findPid(int32(pid))
		if err != nil {
			return nil, os.ErrNotExist
		}

		infos, err := f.pidDir(proc)
		if err != nil {
			return nil, err
		}

		for _, info := range infos {
			if info.Name() == parts[1] {
				return info.(*ninep.SimpleFile).Open(flag)
			}
		}
	default:
		break
	}
	return nil, os.ErrNotExist
}

func (f *Proc) ListDir(path string) (ninep.FileInfoIterator, error) {
	parts := ninep.PathSplit(path)[1:]
	if len(parts) == 0 {
		pids, err := process.Pids()
		if err != nil {
			return nil, err
		}
		infos := make([]os.FileInfo, len(pids)+1)
		infos[0] = procCtlStat

		for i, pid := range pids {
			infos[i+1] = processPid{Pid: pid, modTime: time.Now(), cache: &f.cache}
		}
		return ninep.FileInfoSliceIterator(infos), nil
	} else if parts[0] == "ctl" {
		return nil, ninep.ErrListingOnNonDir
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
	parts := ninep.PathSplit(path)[1:]
	if len(parts) == 0 {
		info := &ninep.SimpleFileInfo{
			FIName:    "/",
			FISize:    0,
			FIMode:    os.ModeDir | 0555,
			FIModTime: time.Now(),
		}
		return info, nil
	} else if parts[0] == "ctl" {
		info := procCtlStat
		return info, nil
	} else {
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

	uid int32
	gid int32

	cache *idCache
}

func (f processPid) Name() string       { return strconv.Itoa(int(f.Pid)) }
func (f processPid) Size() int64        { return 0 }
func (f processPid) Mode() os.FileMode  { return os.ModeDir | 0777 }
func (f processPid) ModTime() time.Time { return f.modTime }
func (f processPid) IsDir() bool        { return true }
func (f processPid) Sys() interface{}   { return nil }
func (f processPid) Uid() string {
	uid := atomic.LoadInt32(&f.uid)
	if uid == 0 {
		proc, err := process.NewProcess(f.Pid)
		if err != nil {
			return ""
		}
		uids, err := proc.Uids()
		if err != nil || len(uids) == 0 {
			return ""
		}
		uid = uids[0]
		atomic.StoreInt32(&f.uid, uids[0])
	}
	return f.cache.Username(int(uid))
}
func (f processPid) Gid() string {
	gid := atomic.LoadInt32(&f.gid)
	if gid == 0 {
		proc, err := process.NewProcess(f.Pid)
		if err != nil {
			return ""
		}
		gids, err := proc.Gids()
		if err != nil || len(gids) == 0 {
			return ""
		}
		gid = gids[0]
		atomic.StoreInt32(&f.gid, gids[0])
	}
	return f.cache.Groupname(int(gid))
}

///////////////////////////////////////

func parseSignal(b []byte) (syscall.Signal, int, error) {
	var (
		p   []byte
		err error
		sig syscall.Signal
	)
	for i, c := range b {
		if c == 0 || c == '\n' {
			p = b[:i]
		}
	}

	str := string(p)
	v, err := strconv.Atoi(str)
	// See: https://en.wikipedia.org/wiki/Signal_(IPC)#POSIX_signals
	// For POSIX compliant signal values
	//
	// most signal values listed via `kill -l` are OS-specific
	if err != nil {
		err = nil
		switch str {
		case "SIGHUP":
			v = 1
		case "SIGINT":
			v = 2
		case "SIGQUIT":
			v = 3
		case "SIGTRAP":
			v = 5
		case "SIGABRT":
			v = 6
		case "SIGKILL":
			v = 9
		case "SIGALRM":
			v = 14
		case "SIGTERM":
			v = 15
		case "SIGSTOP":
			v = int(syscall.SIGSTOP)
		case "SIGCONT":
			v = int(syscall.SIGCONT)
		default:
			err = fmt.Errorf("Invalid signal: %v", str)
		}
	}
	sig = syscall.Signal(v)

	return sig, len(p), err
}

///////////////////////////////////////

type procCtlHandle struct {
	m   sync.Mutex
	in  []byte
	out []byte
}

func (h *procCtlHandle) ReadAt(p []byte, off int64) (n int, err error) {
	end := int64(len(p)) + off

	h.m.Lock()
	size := int64(len(h.out))
	if off >= size {
		h.m.Unlock()
		return 0, io.EOF
	}
	if end >= size {
		err = io.EOF
		end = size
	}
	n = copy(p, h.out[off:end])
	h.m.Unlock()
	return n, err
}

func (h *procCtlHandle) WriteAt(p []byte, off int64) (n int, err error) {
	// we're append only, ignore offset
	n = len(p)
	h.m.Lock()
	h.in = append(h.in, p...)
	for i, ch := range p {
		if ch == '\n' || ch == 0 {
			var pid int
			pid, err = h.exec(h.in[:i])
			h.in = append(h.in[:0], h.in[i:]...)
			if pid > 0 {
				h.out = append(h.out, []byte(fmt.Sprintf("%d\n", pid))...)
			}
			if err != nil {
				n = i
				goto exit
			}
		}
	}
exit:
	h.m.Unlock()
	return n, err
}
func (h *procCtlHandle) Sync() error  { return nil }
func (h *procCtlHandle) Close() error { return nil }

func (h *procCtlHandle) exec(cmd []byte) (pid int, err error) {
	args, err := shlex.Split(string(cmd))
	if err != nil {
		return 0, err
	}
	if len(args) < 1 {
		return 0, errors.New("No command given")
	}
	pattr := os.ProcAttr{
		// TODO: expose these values
	}
	proc, err := os.StartProcess(args[0], args[1:], &pattr)
	return proc.Pid, err
}
