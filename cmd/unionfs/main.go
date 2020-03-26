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

	var exitCode = 0

	exitcodePtr := &exitcode
	defer func() {
		os.Exit(*exitcodePtr)
	}()

	var cliCfg cli.ClientConfig

	flag.BoolVar(&cliCfg.UseRecoverClient, "recover", false, "Use recover client for talking over flaky/unreliable networks")
	flag.IntVar(&cliCfg.TimeoutInSeconds, "client_timeout", 5, "Timeout in seconds for client requests")

	closers := make([]func(), 0)

	defer func() {
		for _, c := range closers {
			c()
		}
	}()

	cfg := &service.Config{
		Name:        "unionfs",
		DisplayName: "Union File System Service",
		Description: "Provides a 9p file system that merges several file systems into one",
	}

	cli.ServiceMain(cfg, func() ninep.FileSystem {
		fsmc := proxy.ParseMounts(flag.Args())
		fsm := make([]proxy.FileSystemMount, 0, len(fsmc))
		for _, c := range fsmc {
			clt, fs, err := cliCfg.CreateFs(c.Addr)

			if err != nil {
				fmt.Printf("Failed to connect to 9p server: %s\n", err)
				exitcode = 1
				runtime.Goexit()
			}
			closers = append(closers, func() { clt.Close() })

			fsm = append(fsm, proxy.FileSystemMount{
				FS:     fs,
				Prefix: c.Prefix,
			})
		}
		return unionfs.New(fsm)
	})
}
