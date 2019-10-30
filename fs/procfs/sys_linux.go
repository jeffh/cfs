// +build linux

package procfs

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
)

func pidFds(p Pid) ([]Fd, error) {
	path := fmt.Sprintf("/proc/%d/fd", p)
	infos, err := ioutil.ReadDir(path)
	if err != nil {
		return nil, err
	}

	type netState struct {
		kind       string
		remoteAddr string
		localAddr  string
	}

	netState := make(map[string]netState)
	{
		f, err := os.Open(fmt.Sprintf("/proc/%d/net/tcp", p))
		if err != nil {
			return nil, err
		}
		buf, err := ioutil.ReadAll(f)
		f.Close()
		if err != nil {
			return nil, err
		}
	}

	fds := make([]Fd, 0, len(infos))
	for _, info := range infos {
		numStr := info.Name()
		num, err := strconv.Atoi(numStr)
		if err != nil {
			return nil, err
		}

		name, err := os.Readlink(filepath.Join(path, numStr))
		if err != nil {
			return nil, err
		}

		kind := FDTypeFile
		typeIndex := strings.Index(name, ":")
		if typeIndex != -1 {
			fdType := name[:typeIndex]
			switch fdType {
			case "socket":
				kind = FDTypeSocket
			case "pipe":
				kind = FDTypePipe
			case "unix":
				kind = FDTypeSocket
			default:
				kind = FDTypeUnknown
			}
		}
		fds = append(fds, Fd{
			Num:  num,
			Name: name,
			Type: kind,
		})
	}
	return fds, nil
}

func pidEnv(p Pid) ([]string, error) {
	f, err := os.Open(fmt.Sprintf("/proc/%d/environ", p))
	if err != nil {
		return nil, err
	}
	buf, err := ioutil.ReadAll(f)
	f.Close()
	if err != nil {
		return nil, err
	}

	argc := 0
	{
		for _, b := range buf {
			if b == 0 {
				argc++
			}
		}
	}

	args := make([]string, 0, argc)
	start := 0
	for i, b := range buf {
		if b == 0 {
			args = append(args, string(buf[start:i]))
			start = i + 1
		}
	}
	return args, nil
}

func pidArgs(p Pid) ([]string, error) {
	f, err := os.Open(fmt.Sprintf("/proc/%d/cmdline", p))
	if err != nil {
		return nil, err
	}
	buf, err := ioutil.ReadAll(f)
	f.Close()
	if err != nil {
		return nil, err
	}

	argc := 0
	{
		for _, b := range buf {
			if b == 0 {
				argc++
			}
		}
	}

	args := make([]string, 0, argc)
	start := 0
	for i, b := range buf {
		if b == 0 {
			args = append(args, string(buf[start:i]))
			start = i + 1
		}
	}
	return args, nil
}

func pidInfo(p Pid) (ProcInfo, error) {
	var pi ProcInfo
	path := fmt.Sprintf("/proc/%d/stat", p)
	f, err := os.Open(path)
	if err != nil {
		return pi, err
	}
	buf, err := ioutil.ReadAll(f)
	f.Close()
	if err != nil {
		return pi, err
	}

	argc := 0
	{
		for _, b := range buf {
			if b == 0 {
				argc++
			}
		}
	}

	args := make([]string, 0, argc)
	start := 0
	for i, b := range buf {
		if b == ' ' {
			args = append(args, string(buf[start:i]))
			start = i + 1
		}
	}

	info, err := os.Stat(path)
	if err != nil {
		return pi, err
	}

	var uid, gid int
	{
		statT, ok := info.Sys().(*syscall.Stat_t)
		if ok {
			uid = int(statT.Uid)
			gid = int(statT.Gid)
		}
	}

	// for ordering of args, see man proc/5

	var pid int
	{
		pid, err = strconv.Atoi(args[0])
		if err != nil {
			return pi, err
		}
	}

	var status Status
	{
		switch args[2][0] {
		case 'R':
			status = STATUS_RUNNING
		case 'S', 'D': // 'S' = sleeping; 'D' = uninterruptable disk sleep
			status = STATUS_SLEEPING
		case 'Z':
			status = STATUS_ZOMBIE
		case 'T', 't': // 'T' = stopped; 't' = tracing stop
			status = STATUS_STOPPED
		case 'X', 'x': // 'x' is for older versions of linux
			status = STATUS_DEAD
		case 'W': // older version of linux
			status = STATUS_RUNNING
		case 'P', 'K': // older version of linux
			status = STATUS_SLEEPING
		default:
			status = STATUS_UNKNOWN
		}
	}

	var ppid int
	{
		ppid, err = strconv.Atoi(args[3])
		if err != nil {
			return pi, err
		}
	}

	var pgrp int
	{
		pgrp, err = strconv.Atoi(args[4])
		if err != nil {
			return pi, err
		}
	}

	pi = ProcInfo{
		Status: status,
		Pid:    Pid(pid),

		RealUid:   Uid(0),
		RealGid:   Gid(0),
		Uid:       Uid(uid),
		Gid:       Gid(gid),
		ParentPid: Pid(ppid),
		PidGroup:  pgrp,
	}
	return pi, nil
}

func pidsList(pq PidQuery) ([]Pid, error) {

	switch pq {
	case QUERY_ALL:
		break
	case QUERY_PGRP:
		return nil, ErrUnsupported
	case QUERY_TTY:
		return nil, ErrUnsupported
	case QUERY_UID:
		return nil, ErrUnsupported
	case QUERY_RUID:
		return nil, ErrUnsupported
	case QUERY_PPID:
		return nil, ErrUnsupported
	default:
		return nil, ErrUnsupported
	}

	infos, err := ioutil.ReadDir("/proc/")
	if err != nil {
		return nil, err
	}

	pids := make([]Pid, 0, len(infos)-58) // subtracting 58 to capacity is just a heuristic

	for _, info := range infos {
		p, err := strconv.Atoi(info.Name())
		if err != nil {
			continue
		}
		pids = append(pids, Pid(p))
	}

	return pids, nil
}
