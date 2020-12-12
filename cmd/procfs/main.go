package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/jeffh/cfs/cli"
	"github.com/jeffh/cfs/fs/procfs"
	"github.com/kardianos/service"
)

func main() {
	cfg := &service.Config{
		Name:        "procfs",
		DisplayName: "Process File System Service",
		Description: "Provides a 9p file system that exposes local process metadata & controls",
	}

	flag.Usage = func() {
		w := flag.CommandLine.Output()
		fmt.Fprintf(w, "Usage: %s [OPTIONS]\n\n", os.Args[0])
		fmt.Fprintf(w, "Exposes local process information as a 9p file server.\n\n")
		fmt.Fprintf(w, "OPTIONS:\n")
		flag.PrintDefaults()
	}
	cli.ServiceMain(cfg, procfs.NewFs)
}
