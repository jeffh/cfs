package main

import (
	"flag"
	"fmt"
	"os"
	"runtime"

	"github.com/jeffh/cfs/cli"
	"github.com/jeffh/cfs/fs/proxy"
	"github.com/jeffh/cfs/fs/unionfs"
	"github.com/jeffh/cfs/ninep"
	"github.com/kardianos/service"
)

func main() {
	cltCfg := cli.ClientConfig{
		PrintPrefix: "[client] ",
	}
	flag.IntVar(&cltCfg.TimeoutInSeconds, "client-timeout", 5, "Timeout in seconds for client requests")
	flag.BoolVar(&cltCfg.PrintTraceMessages, "client-trace", false, "Print trace of 9p client to stdout")
	flag.BoolVar(&cltCfg.PrintErrorMessages, "client-err", false, "Print errors of 9p client to stdout")
	flag.BoolVar(&cltCfg.UseRecoverClient, "r", false, "Use recover client for talking over flaky networks")

	flag.Usage = func() {
		fmt.Fprintf(flag.CommandLine.Output(), "Usage: %s [OPTIONS] FORWARD_ADDR\n", os.Args[0])
		flag.PrintDefaults()
	}

	cfg := &service.Config{
		Name:        "proxyfs",
		DisplayName: "Proxy File System Service",
		Description: "Provides a 9p file system that proxies one 9p file system into another one. Useful to remap a private 9p server to a public ip.",
	}

	var exitCode int

	defer func() { os.Exit(exitCode) }()
	closers := make([]func(), 0)
	defer func() {
		for _, c := range closers {
			c()
		}
	}()

	cli.ServiceMain(cfg, func() ninep.FileSystem {

		if flag.NArg() == 1 {
		}

		fsmc := proxy.ParseMounts(flag.Args())
		fsm := make([]proxy.FileSystemMount, 0, len(fsmc)-1)

		for _, mcfg := range fsmc {
			clt, fs, err := cltCfg.CreateFs(mcfg.Addr)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Error connecting: %v\n", err)
				runtime.Goexit()
			}
			closers = append(closers, func() { clt.Close() })

			fsm = append(fsm, proxy.FileSystemMount{
				fs,
				mcfg.Prefix,
			})
		}

		return unionfs.New(fsm)
	})
}
