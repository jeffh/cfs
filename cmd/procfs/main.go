package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/jeffh/cfs/cli"
	"github.com/jeffh/cfs/fs/procfs"
	_ "go.uber.org/automaxprocs"
)

func main() {
	flag.Usage = func() {
		w := flag.CommandLine.Output()
		_, _ = fmt.Fprintf(w, "Usage: %s [OPTIONS]\n\n", os.Args[0])
		_, _ = fmt.Fprintf(w, "Exposes local process information as a 9p file server.\n\n")
		_, _ = fmt.Fprintf(w, "OPTIONS:\n")
		flag.PrintDefaults()
	}
	cli.ServiceMain(procfs.NewFs)
}
