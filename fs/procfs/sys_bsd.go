//go:build bsd || darwin
// +build bsd darwin

package procfs

// #include <sys/sysctl.h>
// #include <sys/socket.h>
// #include <libproc.h>
import "C"
import (
	"encoding/binary"
	"fmt"
	"math/big"
	"strconv"
	"syscall"
	"unsafe"

	"golang.org/x/sys/unix"
)

func intToIpv4(ipInt int64) string {
	return fmt.Sprintf("%d.%d.%d.%d",
		(ipInt>>24)&0xff,
		(ipInt>>16)&0xff,
		(ipInt>>8)&0xff,
		(ipInt & 0xff),
	)
	// b0 := strconv.FormatInt((ipInt>>24)&0xff, 10)
	// b1 := strconv.FormatInt((ipInt>>16)&0xff, 10)
	// b2 := strconv.FormatInt((ipInt>>8)&0xff, 10)
	// b3 := strconv.FormatInt((ipInt & 0xff), 10)
	// return b0 + "." + b1 + "." + b2 + "." + b3
}

func bytesToIpv6(b []byte) string {
	ip := big.NewInt(0)
	ip.SetBytes(b)
	return ip.String()
}

func pidFds(p Pid) ([]Fd, error) {
	size := C.proc_pidinfo(C.int(p), C.PROC_PIDLISTFDS, 0, C.NULL, 0)
	if size < 0 {
		return nil, syscall.EINVAL
	}

	buf := make([]byte, size)
	used := C.proc_pidinfo(C.int(p), C.PROC_PIDLISTFDS, 0, unsafe.Pointer(&buf[0]), size)
	if used < 0 {
		return nil, syscall.EINVAL
	}

	cnt := uintptr(size / C.PROC_PIDLISTFD_SIZE)
	items := make([]Fd, 0, cnt)
	for i := uintptr(0); i < cnt; i++ {
		fdi := *(*C.struct_proc_fdinfo)(unsafe.Pointer(&buf[i*C.PROC_PIDLISTFD_SIZE]))

		switch fdi.proc_fdtype {
		case C.PROX_FDTYPE_VNODE:
			var info C.struct_vnode_fdinfowithpath
			n := C.proc_pidfdinfo(C.int(p), fdi.proc_fd, C.PROC_PIDFDVNODEPATHINFO, unsafe.Pointer(&info), C.PROC_PIDFDVNODEPATHINFO_SIZE)
			if n == C.PROC_PIDFDVNODEPATHINFO_SIZE {
				items = append(items, Fd{
					Num:  int(fdi.proc_fd),
					Type: FDTypeFile,
					Name: C.GoString((*C.char)(unsafe.Pointer(&info.pvip.vip_path[0]))),
				})
			}
		case C.PROX_FDTYPE_SOCKET:
			var info C.struct_socket_fdinfo
			n := C.proc_pidfdinfo(C.int(p), fdi.proc_fd, C.PROC_PIDFDSOCKETINFO, unsafe.Pointer(&info), C.PROC_PIDFDSOCKETINFO_SIZE)
			if n == C.PROC_PIDFDSOCKETINFO_SIZE {
				var (
					kind     string
					sockType string

					laddr, raddr string
					lport, rport int

					srcAddr, remAddr string
					postfix          string
				)
				switch info.psi.soi_kind {
				case C.SOCKINFO_GENERIC:
					kind = "generic"
				case C.SOCKINFO_IN:
					kind = "in"
				case C.SOCKINFO_TCP:
					kind = "tcp"
					tcp := (*C.struct_tcp_sockinfo)(unsafe.Pointer(&info.psi.soi_proto[0]))
					lport = int(tcp.tcpsi_ini.insi_lport)
					rport = int(tcp.tcpsi_ini.insi_fport)
					switch tcp.tcpsi_ini.insi_vflag {
					case C.INI_IPV4:
						lipv4 := (*C.struct_in4in6_addr)(unsafe.Pointer(&tcp.tcpsi_ini.insi_laddr[0]))
						ripv4 := (*C.struct_in4in6_addr)(unsafe.Pointer(&tcp.tcpsi_ini.insi_faddr[0]))
						laddr = intToIpv4(int64(lipv4.i46a_addr4.s_addr))
						raddr = intToIpv4(int64(ripv4.i46a_addr4.s_addr))
					case C.INI_IPV6:
						lipv4 := (*C.struct_in6_addr)(unsafe.Pointer(&tcp.tcpsi_ini.insi_laddr[0]))
						ripv4 := (*C.struct_in6_addr)(unsafe.Pointer(&tcp.tcpsi_ini.insi_faddr[0]))
						laddr = bytesToIpv6(lipv4.__u6_addr[:])
						raddr = bytesToIpv6(ripv4.__u6_addr[:])
					default:
						break
					}
					if lport > 0 {
						srcAddr = fmt.Sprintf("%s:%d", laddr, lport)
					}
					if rport > 0 {
						remAddr = fmt.Sprintf("%s:%d", raddr, rport)
					}
					if lport > 0 && rport > 0 {
						postfix = fmt.Sprintf("%s->%s", srcAddr, remAddr)
					} else if rport > 0 {
						postfix = fmt.Sprintf("*->%s", remAddr)
					} else {
						postfix = fmt.Sprintf("%s", srcAddr)
					}
				case C.SOCKINFO_UN:
					kind = "unix"
				case C.SOCKINFO_NDRV:
					kind = "ndrv"
				case C.SOCKINFO_KERN_EVENT:
					kind = "kernel_event"
				case C.SOCKINFO_KERN_CTL:
					kind = "kernel_ctl"
				default:
					kind = "unknown"
				}
				if kind != "tcp" {
					switch info.psi.soi_type {
					case C.SOCK_STREAM:
						sockType = "tcp|"
					case C.SOCK_DGRAM:
						sockType = "udp|"
					case C.SOCK_RAW:
						sockType = "raw|"
					case C.SOCK_RDM:
						sockType = "rdm|"
					case C.SOCK_SEQPACKET:
						sockType = "seqpacket|"
					default:
						sockType = "unknown|"
					}
				}

				items = append(items, Fd{
					Num:              int(fdi.proc_fd),
					Type:             FDTypeSocket,
					Name:             fmt.Sprintf("%s|%s%s", kind, sockType, postfix),
					SocketType:       kind,
					SocketSourceAddr: srcAddr,
					SocketRemoteAddr: remAddr,
				})
			}
		case C.PROX_FDTYPE_PSEM:
			var info C.struct_psem_fdinfo
			n := C.proc_pidfdinfo(C.int(p), fdi.proc_fd, C.PROC_PIDFDPSEMINFO, unsafe.Pointer(&info), C.PROC_PIDFDPSEMINFO_SIZE)
			if n == C.PROC_PIDFDPSEMINFO_SIZE {
				items = append(items, Fd{
					Num:  int(fdi.proc_fd),
					Type: FDTypeFile,
					Name: C.GoString((*C.char)(unsafe.Pointer(&info.pseminfo.psem_name[0]))),
				})
			}
		case C.PROX_FDTYPE_PSHM:
			var info C.struct_pshm_fdinfo
			n := C.proc_pidfdinfo(C.int(p), fdi.proc_fd, C.PROC_PIDFDPSHMINFO, unsafe.Pointer(&info), C.PROC_PIDFDPSHMINFO_SIZE)
			if n == C.PROC_PIDFDPSHMINFO_SIZE {
				items = append(items, Fd{
					Num:  int(fdi.proc_fd),
					Type: FDTypeFile,
					Name: C.GoString((*C.char)(unsafe.Pointer(&info.pshminfo.pshm_name[0]))),
				})
			}
		case C.PROX_FDTYPE_PIPE:
			var info C.struct_pipe_fdinfo
			n := C.proc_pidfdinfo(C.int(p), fdi.proc_fd, C.PROC_PIDFDPIPEINFO, unsafe.Pointer(&info), C.PROC_PIDFDPIPEINFO_SIZE)
			if n == C.PROC_PIDFDPIPEINFO_SIZE {
				items = append(items, Fd{
					Num:  int(fdi.proc_fd),
					Type: FDTypePipe,
					Name: strconv.Itoa(int(info.pipeinfo.pipe_handle)),
				})
			}
		case C.PROX_FDTYPE_KQUEUE, C.PROX_FDTYPE_FSEVENTS, C.PROX_FDTYPE_NETPOLICY:
			fallthrough
		default:
			break
		}
	}
	return items, nil
}

func pidEnv(p Pid) ([]string, error) {
	_, _, env, err := pidNameArgsAndEnvs(p)
	return env, err
}

func pidArgs(p Pid) ([]string, error) {
	exec, argv, _, err := pidNameArgsAndEnvs(p)
	if err == nil {
		argv[0] = exec
	}
	return argv, err
}

func pidInfo(p Pid) (ProcInfo, error) {
	var pi ProcInfo
	r, err := unix.SysctlRaw("kern.proc.pid", int(p))
	if err != nil {
		return pi, err
	}
	k := (*C.struct_kinfo_proc)(unsafe.Pointer(&r[0]))

	status := STATUS_UNKNOWN
	switch k.kp_proc.p_stat {
	case 1:
		status = STATUS_IDLE
	case 2:
		status = STATUS_RUNNING
	case 3:
		status = STATUS_SLEEPING
	case 4:
		status = STATUS_STOPPED
	case 5:
		status = STATUS_ZOMBIE
	default:
		status = STATUS_UNKNOWN
	}

	pi = ProcInfo{
		Status: status,
		Pid:    Pid(k.kp_proc.p_pid),

		RealUid:   Uid(k.kp_eproc.e_pcred.p_ruid),
		RealGid:   Gid(k.kp_eproc.e_pcred.p_rgid),
		Uid:       Uid(k.kp_eproc.e_pcred.p_svuid),
		Gid:       Gid(k.kp_eproc.e_pcred.p_svgid),
		ParentPid: Pid(k.kp_eproc.e_ppid),
		PidGroup:  int(k.kp_eproc.e_pgid),
	}
	return pi, nil
}

func pidsList(pq PidQuery) ([]Pid, error) {

	var q C.uint32_t
	switch pq {
	case QUERY_ALL:
		q = C.PROC_ALL_PIDS
	case QUERY_PGRP:
		q = C.PROC_PGRP_ONLY
	case QUERY_TTY:
		q = C.PROC_TTY_ONLY
	case QUERY_UID:
		q = C.PROC_UID_ONLY
	case QUERY_RUID:
		q = C.PROC_RUID_ONLY
	case QUERY_PPID:
		q = C.PROC_PPID_ONLY
	default:
		return nil, ErrUnsupported
	}

	n := C.proc_listpids(q, 0, nil, 0)

	if n < 0 {
		return nil, syscall.EINVAL
	}

	buf := make([]byte, n)

	n = C.proc_listpids(q, 0, unsafe.Pointer(&buf[0]), n)
	if n < 0 {
		return nil, syscall.ENOMEM
	}

	var pid Pid
	pidSize := binary.Size(pid)
	cnt := int(n) / pidSize
	pids := make([]Pid, 0, cnt)
	for i := 0; i < cnt; i++ {
		pid = Pid(binary.LittleEndian.Uint32(buf[i*pidSize:]))
		if pid == 0 {
			continue
		}

		pids = append(pids, pid)
	}

	return pids, nil
}

////////////////////////////////////////////////////////////////////

func pidNameArgsAndEnvs(p Pid) (exec string, argv, env []string, err error) {
	/*
	 * Make a sysctl() call to get the raw argument space of the process.
	 * The layout is documented in start.s, which is part of the Csu
	 * project.  In summary, it looks like:
	 *
	 * /---------------\ 0x00000000
	 * :               :
	 * :               :
	 * |---------------|
	 * | argc          |
	 * |---------------|
	 * | arg[0]        |
	 * |---------------|
	 * :               :
	 * :               :
	 * |---------------|
	 * | arg[argc - 1] |
	 * |---------------|
	 * | 0             |
	 * |---------------|
	 * | env[0]        |
	 * |---------------|
	 * :               :
	 * :               :
	 * |---------------|
	 * | env[n]        |
	 * |---------------|
	 * | 0             |
	 * |---------------| <-- Beginning of data returned by sysctl() is here.
	 * | argc          |
	 * |---------------|
	 * | exec_path     |
	 * |:::::::::::::::|
	 * |               |
	 * | String area.  |
	 * |               |
	 * |---------------| <-- Top of stack.
	 * :               :
	 * :               :
	 * \---------------/ 0xffffffff
	 */
	size := C.size_t(C.ARG_MAX)
	mib := [3]C.int{C.CTL_KERN, C.KERN_PROCARGS2, C.int(p)}
	buf := [C.ARG_MAX]byte{}
	_, _, errno := unix.Syscall6(
		syscall.SYS___SYSCTL,
		uintptr(unsafe.Pointer(&mib[0])),
		uintptr(len(mib)),
		uintptr(unsafe.Pointer(&buf[0])),
		uintptr(unsafe.Pointer(&size)),
		0,
		0,
	)
	if errno != 0 {
		err = errno
		return
	}

	argc := *(*C.int)(unsafe.Pointer(&buf[0]))

	execPath := buf[unsafe.Sizeof(argc):]
	rest := execPath
	for i, b := range execPath {
		if b == 0 {
			rest = execPath[i+1:]
			execPath = execPath[:i]
			break
		}
	}

	args := make([]string, 0, argc)
	n := 0
	for i, b := range rest {
		if b == 0 {
			if n != i-1 && i > 0 {
				args = append(args, string(rest[n+1:i]))
			}
			n = i
		}
	}

	exec = string(execPath)
	argv = args[:argc]
	env = args[argc:]
	return
}
