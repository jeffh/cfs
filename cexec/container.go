package cexec

import (
	"syscall"
)

type linuxContainerExecutor struct{}

func (e *linuxContainerExecutor) Name() string { return "lxc" }

func (e *linuxContainerExecutor) Run(c *Cmd) error {
	return LinuxContainerExec(c)
}

////////////////////////////////////////////////////////

// Runs exec, but chroots to Dir
// Supports Cmd.Root
func ChrootExec(c *Cmd) error {
	cmd := makeCmd(c)
	cmd.SysProcAttr = &syscall.SysProcAttr{
		Chroot: c.Root,
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

// Implements a simple fork-exec
type chrootExecutor struct{}

func (e *chrootExecutor) Name() string { return "chroot" }

func (e *chrootExecutor) Run(c *Cmd) error {
	return ChrootExec(c)
}
