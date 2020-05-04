package cexec

import (
	"os/exec"
)

func makeCmd(c *Cmd) *exec.Cmd {
	cmd := exec.Command(c.Cmd, c.Args...)
	cmd.Env = c.Env
	cmd.Dir = c.Dir
	cmd.Stdin = c.Stdin
	cmd.Stdout = c.Stdout
	cmd.Stderr = c.Stderr
	return cmd
}

// Traditional Unix exec of a command
func ForkExec(c *Cmd) error {
	cmd := makeCmd(c)
	if err := cmd.Run(); err != nil {
		return err
	}

	c.State = &ProcessState{
		Pid:      cmd.ProcessState.Pid(),
		ExitCode: cmd.ProcessState.ExitCode(),
		Exited:   cmd.ProcessState.Exited(),
	}

	return nil
}

// Implements a simple fork-exec
type forkExecutor struct{}

func (e *forkExecutor) Run(c *Cmd) error {
	return ForkExec(c)
}
