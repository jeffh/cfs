package cexec

import "io"

// Cmd represents a command to execute
type Cmd struct {
	Cmd    string
	Args   []string
	Env    []string
	Dir    string
	Stdin  io.Reader
	Stdout io.Writer
	Stderr io.Writer

	// implementation specific, an executor may ignore these value
	Root                 string
	IsolateHostAndDomain bool
	IsolatePids          bool
	IsolateIPC           bool
	IsolateNetwork       bool
	IsolateUsers         bool

	State *ProcessState
}

// ProcessState represents the state of a completed process
type ProcessState struct {
	Pid      int
	ExitCode int
	Exited   bool
}

// Success returns true if the process exited and the exit code was 0
func (s *ProcessState) Success() bool { return s.Exited && s.ExitCode == 0 }

// Executor is an interface for executing a command
type Executor interface {
	Run(c *Cmd) error
	Name() string
}

// ForkExecutor returns an Executor that uses the traditional Unix exec
func ForkExecutor() Executor { return &forkExecutor{} }

// ChrootExecutor returns an Executor that chroots to the specified directory
func ChrootExecutor() Executor { return &chrootExecutor{} }

// LinuxContainerExecutor returns an Executor that runs a command in a container
func LinuxContainerExecutor() Executor { return &linuxContainerExecutor{} }
