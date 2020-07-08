package cexec

import "io"

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

type ProcessState struct {
	Pid      int
	ExitCode int
	Exited   bool
}

func (s *ProcessState) Success() bool { return s.Exited && s.ExitCode == 0 }

type Executor interface {
	Run(c *Cmd) error
	Name() string
}

func ForkExecutor() Executor           { return &forkExecutor{} }
func ChrootExecutor() Executor         { return &chrootExecutor{} }
func LinuxContainerExecutor() Executor { return &linuxContainerExecutor{} }
