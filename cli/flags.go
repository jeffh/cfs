package cli

import "flag"

type Flags interface {
	StringVar(*string, string, string, string)
	IntVar(*int, string, int, string)
	BoolVar(*bool, string, bool, string)
}

type StdFlags struct{}

func (f *StdFlags) StringVar(p *string, name string, defaultValue string, help string) {
	flag.StringVar(p, name, defaultValue, help)
}

func (f *StdFlags) IntVar(p *int, name string, defaultValue int, help string) {
	flag.IntVar(p, name, defaultValue, help)
}

func (f *StdFlags) BoolVar(p *bool, name string, defaultValue bool, help string) {
	flag.BoolVar(p, name, defaultValue, help)
}
