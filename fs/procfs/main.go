package procfs

import (
	"errors"
	"fmt"
	"io"
	"os"
	"os/user"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/jeffh/cfs/ninep"
)

var (
	ErrFailedPidInfo = errors.New("failed to get process info")
	ErrUnsupported   = errors.New("unsupported")
)

type PidQuery int

const (
	QUERY_ALL PidQuery = iota
	QUERY_PGRP
	QUERY_TTY
	QUERY_UID
	QUERY_RUID
	QUERY_PPID
)

type Status uint8
type Uid uint32
type Gid uint32
type Pid uint32

func (s Status) String() string {
	switch s {
	case STATUS_IDLE:
		return "idle"
	case STATUS_RUNNING:
		return "running"
	case STATUS_WAITING:
		return "waiting"
	case STATUS_SLEEPING:
		return "sleeping"
	case STATUS_STOPPED:
		return "stopped"
	case STATUS_ZOMBIE:
		return "zombie"
	default:
		return "unknown"
	}
}

const (
	STATUS_UNKNOWN Status = iota
	STATUS_IDLE
	STATUS_RUNNING
	STATUS_SLEEPING
	STATUS_STOPPED
	STATUS_ZOMBIE
	STATUS_WAITING
	STATUS_DEAD // linux
)

type ProcInfo struct {
	Status Status
	Pid    Pid

	RealUid Uid
	RealGid Gid

	// Effective
	Uid Uid
	Gid Gid

	ParentPid Pid
	PidGroup  int
	Nice      int
}

type FDType string

const (
	FDTypeFile    FDType = "file"
	FDTypeSocket         = "socket"
	FDTypePSEM           = "psem"
	FDTypePSHM           = "pshm"
	FDTypePipe           = "pipe"
	FDTypeUnknown        = "unknown"
)

type Fd struct {
	Num  int
	Type FDType
	Name string

	SocketType       string
	SocketSourceAddr string
	SocketRemoteAddr string
}

func procCtl(r io.Reader, w io.Writer) {
}

func procDir(pid Pid) func() ([]ninep.Node, error) {
	return func() ([]ninep.Node, error) {
		now := time.Now()
		pi, err := pidInfo(pid)
		if err != nil {
			return nil, err
		}
		uid := int(pi.Uid)
		var username string
		{
			usr, err := user.LookupId(strconv.Itoa(uid))
			if err == nil {
				username = usr.Username
			}
		}
		gid := int(pi.Gid)
		var groupname string
		{
			grp, err := user.LookupGroupId(strconv.Itoa(gid))
			if err == nil {
				groupname = grp.Name
			}
		}
		nodes := []ninep.Node{
			staticStringFile("pid", now, strconv.Itoa(int(pid))),
			staticStringFile("status", now, pi.Status.String()),
			staticStringFile("uid", now, fmt.Sprintf("%d\n%s\n", uid, username)),
			staticStringFile("gid", now, fmt.Sprintf("%d\n%s\n", gid, groupname)),
			staticStringFile("real_uid", now, strconv.Itoa(int(pi.RealUid))),
			staticStringFile("real_gid", now, strconv.Itoa(int(pi.RealGid))),
			staticStringFile("parent_pid", now, strconv.Itoa(int(pi.ParentPid))),
			staticStringFile("pid_group", now, strconv.Itoa(pi.PidGroup)),
			dynamicStringFile("cmdline", now, func() ([]byte, error) {
				argv, err := pidArgs(pid)
				if err == nil {
					for i, arg := range argv[1:] {
						if strings.Index(arg, " ") != -1 {
							argv[i+1] = fmt.Sprintf("%#v", arg)
						}
					}
				}
				return []byte(strings.Join(argv, " ")), err
			}),
			dynamicStringFile("env", now, func() ([]byte, error) {
				env, err := pidEnv(pid)
				return []byte(strings.Join(env, "\n")), err
			}),
			dynamicStringFile("fds", now, func() ([]byte, error) {
				fds, err := pidFds(pid)
				str := make([]string, len(fds))
				for i, fd := range fds {
					str[i] = fmt.Sprintf("%#v", fd)
				}
				return []byte(strings.Join(str, "\n")), err
			}),
		}
		return nodes, nil
	}
}

func procList() ([]ninep.Node, error) {
	pids, err := pidsList(QUERY_ALL)
	if err != nil {
		return nil, err
	}
	sort.Sort(SortablePids(pids))
	nodes := make([]ninep.Node, len(pids)+1)
	nodes[0] = dynamicCtlFile("ctl", procCtl)
	for i, pid := range pids {
		nodes[i+1] = dynamicDir(strconv.Itoa(int(pid)), procDir(pid))
	}
	return nodes, nil
}

func NewFs() ninep.FileSystem {
	fs := &ninep.SimpleFileSystem{
		Root: ninep.DynamicRootDir(procList),
	}
	return fs
}

func staticDir(name string, children ...ninep.Node) *ninep.StaticReadOnlyDir {
	return &ninep.StaticReadOnlyDir{
		SimpleFileInfo: ninep.SimpleFileInfo{
			FIName: name,
			FIMode: os.ModeDir | 0777,
		},
		Children: children,
	}
}

func dynamicDir(name string, resolve func() ([]ninep.Node, error)) *ninep.DynamicReadOnlyDir {
	return &ninep.DynamicReadOnlyDir{
		SimpleFileInfo: ninep.SimpleFileInfo{
			FIName: name,
			FIMode: os.ModeDir | 0777,
		},
		GetChildren: resolve,
	}
}

func dynamicCtlFile(name string, thread func(r io.Reader, w io.Writer)) *ninep.SimpleFile {
	return ninep.CtlFile(name, 0777, time.Time{}, thread)
}

func staticStringFile(name string, modTime time.Time, contents string) *ninep.SimpleFile {
	return ninep.StaticReadOnlyFile(name, 0444, modTime, []byte(contents))
}

func dynamicStringFile(name string, modTime time.Time, content func() ([]byte, error)) *ninep.SimpleFile {
	return ninep.DynamicReadOnlyFile(name, 0777, modTime, content)
}

///////////////////////////////////////

type SortablePids []Pid

func (s SortablePids) Len() int           { return len(s) }
func (s SortablePids) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
func (s SortablePids) Less(i, j int) bool { return s[i] < s[j] }
