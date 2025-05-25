// Implements a 9p file server that behaves similar to /proc on some unix systems
package procfs

import (
	"errors"

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
	FDTypeFile   FDType = "file"
	FDTypeSocket FDType = "socket"
	FDTypePSEM   FDType = "psem"
	FDTypePSHM    FDType = "pshm"
	FDTypePipe    FDType = "pipe"
	FDTypeUnknown FDType = "unknown"
)

type Fd struct {
	Num  int
	Type FDType
	Name string

	SocketType       string
	SocketSourceAddr string
	SocketRemoteAddr string
}

func NewFs() ninep.FileSystem {
	return &fsys{}
}

///////////////////////////////////////

type SortablePids []Pid

func (s SortablePids) Len() int           { return len(s) }
func (s SortablePids) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
func (s SortablePids) Less(i, j int) bool { return s[i] < s[j] }
