package main

import (
	"flag"
	"fmt"
	"io"
	"os"

	"github.com/jeffh/cfs/cli"
	"github.com/jeffh/cfs/ninep"
	"github.com/kardianos/service"
)

func main() {
	cltCfg := cli.ClientConfig{PrintPrefix: "[client] "}
	srvCfg := cli.ServerConfig{PrintPrefix: "[server] "}

	cltCfg.SetFlags(nil)
	srvCfg.SetFlags(nil)

	flag.Usage = func() {
		fmt.Fprintf(flag.CommandLine.Output(), "Usage: %s [OPTIONS] FORWARD_ADDR\n", os.Args[0])
		flag.PrintDefaults()
	}

	flag.Parse()

	cltCfg.PrintTraceMessages = srvCfg.PrintTraceMessages
	cltCfg.PrintErrorMessages = srvCfg.PrintErrorMessages

	if flag.NArg() == 0 {
		flag.Usage()
		os.Exit(1)
	}
	srcAddr := flag.Arg(0)

	clt, fs, err := cltCfg.CreateFs(srcAddr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %s\n", err)
	}
	defer clt.Close()

	serviceCfg := &service.Config{
		Name:        "proxyfs",
		DisplayName: "Proxy File System Service",
		Description: "Provides a 9p file system that proxies to other 9p file systems",
	}
	createcfg := func(stdout, stderr io.Writer) cli.ServerConfig {
		srvCfg.Stdout = stdout
		srvCfg.Stderr = stderr
		return srvCfg
	}
	createfs := func() ninep.FileSystem { return fs }
	cli.ServiceMainWithFactory(serviceCfg, createcfg, createfs)
}
