package cexec

import "errors"

var errUnsupported = errors.New("unsupported on this operating system")

// LinuxContainerExec executes a command, running in a process in a chrooted file system.
// This is not for security, just attempts to isolate its use of the local file system
func LinuxContainerExec(c *Cmd) error {
	return errUnsupported
}

func LinuxContainerHandleContainerExec() {}
