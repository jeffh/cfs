package cexec

import "errors"

// Executes a command, running in a process in a chrooted file system.
// This is not for security, just attempts to isolate its use of the local file system
func LinuxContainerExec(c *Cmd) error {
	return errors.New("Unsupported on this operating system")
}

func LinuxContainerHandleContainerExec() {
	return
}
