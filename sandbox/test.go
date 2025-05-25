//go:build darwin
// +build darwin

package main

// #include <sys/sysctl.h>
// #include <libproc.h>
import "C"

import (
	"fmt"
	"unsafe"

	"golang.org/x/sys/unix"
)

const (
	STATUS_IDLE     = 1
	STATUS_RUNNING  = 2
	STATUS_SLEEPING = 3
	STATUS_STOPPED  = 4
	STATUS_ZOMBIE   = 5
)

type Uid uint32
type Gid uint32
type Pid uint32

type ProcInfo struct {
	Status uint8
	Pid    Pid

	RealUid Uid
	RealGid Gid

	// Effective
	Uid Uid
	Gid Gid

	ParentPid Pid
}

func main() {
	// CTL_KERN := 1
	// KERN_PROCARGS := 38
	// PID := 98699
	// mib := [3]int{CTL_KERN, KERN_PROCARGS, PID}
	r, err := unix.SysctlRaw("kern.proc.all", 0)
	k := (*C.struct_kinfo_proc)(unsafe.Pointer(&r[0]))
	pi := ProcInfo{
		Status: uint8(k.kp_proc.p_stat),
		Pid:    Pid(k.kp_proc.p_pid),

		RealUid:   Uid(k.kp_eproc.e_pcred.p_ruid),
		RealGid:   Gid(k.kp_eproc.e_pcred.p_rgid),
		Uid:       Uid(k.kp_eproc.e_pcred.p_svuid),
		Gid:       Gid(k.kp_eproc.e_pcred.p_svgid),
		ParentPid: Pid(k.kp_eproc.e_ppid),
	}
	fmt.Printf("SYSCALL: %#v, %#v\n", pi, err)
}
