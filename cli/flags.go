package cli

import (
	"flag"
	"io"
	"os"

	"github.com/google/shlex"
)

type Flags interface {
	StringVar(*string, string, string, string)
	IntVar(*int, string, int, string)
	BoolVar(*bool, string, bool, string)

	ReadFileConfig(filename string) error

	Parse() ([]string, error)
	Usage()
}

type StdFlags struct {
	args []string
}

func (f *StdFlags) StringVar(p *string, name string, defaultValue string, help string) {
	flag.StringVar(p, name, defaultValue, help)
}

func (f *StdFlags) IntVar(p *int, name string, defaultValue int, help string) {
	flag.IntVar(p, name, defaultValue, help)
}

func (f *StdFlags) BoolVar(p *bool, name string, defaultValue bool, help string) {
	flag.BoolVar(p, name, defaultValue, help)
}

func (f *StdFlags) ReadFileConfig(filename string) error {
	h, err := os.Open(filename)
	if err != nil {
		return err
	}
	defer h.Close()

	b, err := io.ReadAll(h)
	if err != nil {
		return err
	}

	f.args, err = shlex.Split(string(b))
	if err != nil {
		return err
	}
	return nil
}

func (f *StdFlags) Parse() ([]string, error) {
	err := flag.CommandLine.Parse(f.getArgs())
	return flag.Args(), err
}

func (f *StdFlags) Usage() {
	flag.Usage()
}

func (f *StdFlags) getArgs() []string {
	if f.args == nil {
		return os.Args[1:]
	}
	return f.args
}
