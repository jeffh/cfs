package cexec

import (
	"fmt"
	"os"
	"os/exec"
	"syscall"
)

// Executes a command, running in a process in a chrooted file system.
// This is not for security, just attempts to isolate its use of the local file system
func LinuxContainerExec(c *Cmd) error {
	cmd := makeCmd(&Cmd{
		Cmd:  "/proc/self/exe",
		Args: append(append([]string{"cfs-container-exec"}, c.Cmd), c.Args...),
	})

	flags := syscall.CLONE_NEWNS
	if c.Root != "" {
		flags |= syscall.CLONE_NEWNS
	}
	if c.IsolatePids {
		flags |= syscall.CLONE_NEWPID
	}
	if c.IsolateHostAndDomain {
		flags |= syscall.CLONE_NEWUTS
	}
	if c.IsolateIPC {
		flags |= syscall.CLONE_NEWIPC
	}
	if c.IsolateNetwork {
		flags |= syscall.CLONE_NEWNET
	}
	if c.IsolateUsers {
		flags |= syscall.CLONE_NEWUSER
	}
	cmd.SysProcAttr = &syscall.SysProcAttr{
		Cloneflags: flags,
	}

	err := cmd.Run()

	if cmd.ProcessState != nil {
		c.State = &ProcessState{
			Pid:      cmd.ProcessState.Pid(),
			ExitCode: cmd.ProcessState.ExitCode(),
			Exited:   cmd.ProcessState.Exited(),
		}
	}

	return err
}

func LinuxContainerHandleContainerExec() {
	must := func(err error, errExitCode int) {
		if err != nil {
			fmt.Fprintf(os.Stderr, "error: %s\n", err)
			os.Exit(errExitCode)
		}
	}
	if len(os.Args) > 1 && os.Args[0] == "cfs-container-exec" {

		must(syscall.Mount("rootfs", "rootfs", "", syscall.MS_BIND, ""), 2)
		must(os.MkdirAll("rootfs/oldrootfs", 0700), 3)
		must(syscall.PivotRoot("rootfs", "rootfs/oldrootfs"), 4)
		must(os.Chdir("/"), 5)

		cmd := exec.Command(os.Args[1], os.Args[2:]...)
		cmd.Dir = ""
		cmd.Stdin = os.Stdin
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr

		must(cmd.Run(), 1)
		os.Exit(cmd.ExitCode())
	}
}
