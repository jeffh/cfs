package main

import (
	"flag"
	"fmt"
	"io"
	"os"

	"github.com/jeffh/cfs/cli"
	"github.com/jeffh/cfs/ninep"
	_ "go.uber.org/automaxprocs"
)

func main() {
	cltCfg := cli.ClientConfig{PrintPrefix: "[client] "}
	srvCfg := cli.ServerConfig{PrintPrefix: "[server] "}

	cltCfg.SetFlags(nil)
	srvCfg.SetFlags(nil)

	flag.Usage = func() {
		w := flag.CommandLine.Output()
		_, _ = fmt.Fprintf(w, "Usage: %s [OPTIONS] FORWARD_ADDR\n\n", os.Args[0])
		_, _ = fmt.Fprintf(w, "Useful for proxying 9p file servers to a new network address\n\n")
		_, _ = fmt.Fprintf(w, "OPTIONS\n")
		flag.PrintDefaults()
	}

	flag.Parse()

	cltCfg.LogLevel = srvCfg.LogLevel
	cltCfg.Logger = srvCfg.Logger

	if flag.NArg() == 0 {
		flag.Usage()
		os.Exit(1)
	}
	srcAddr := flag.Arg(0)

	clt, fs, err := cltCfg.CreateFs(srcAddr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %s\n", err)
	}
	defer func() { _ = clt.Close() }()

	createcfg := func(stdout, stderr io.Writer) cli.ServerConfig {
		srvCfg.Stdout = stdout
		srvCfg.Stderr = stderr
		return srvCfg
	}
	createfs := func() ninep.FileSystem { return fs }
	cli.ServiceMainWithFactory(createcfg, createfs)
}
