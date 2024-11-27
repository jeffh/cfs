package main

import (
	"flag"
	"fmt"
	"os"
	"runtime"
	"sync"

	"github.com/jeffh/cfs/cli"
	"github.com/jeffh/cfs/fs/proxy"
	"github.com/jeffh/cfs/fs/unionfs"
	"github.com/jeffh/cfs/ninep"
	_ "go.uber.org/automaxprocs"
)

func main() {

	var exitCode = 0

	exitcodePtr := &exitCode
	defer func() {
		os.Exit(*exitcodePtr)
	}()

	var cliCfg cli.ClientConfig

	flag.BoolVar(&cliCfg.UseRecoverClient, "recover", false, "Use recover client for talking over flaky/unreliable networks")
	flag.IntVar(&cliCfg.TimeoutInSeconds, "client_timeout", 5, "Timeout in seconds for client requests")

	flag.Usage = func() {
		o := flag.CommandLine.Output()
		fmt.Fprintf(o, "Usage: %s [OPTIONS] MOUNTS...\n", os.Args[0])
		fmt.Fprintf(o, "Combines two or more separate 9p file systems into one 9p file system overlay.\n")
		proxy.PrintMountsHelp(o)
		fmt.Fprintf(o, "\nOPTIONS:\n")
		flag.PrintDefaults()
	}

	var m sync.Mutex
	closers := make([]func(), 0)

	defer func() {
		m.Lock()
		defer m.Unlock()
		for _, c := range closers {
			c()
		}
	}()

	cli.ServiceMain(func() ninep.FileSystem {
		fsmc := proxy.ParseMounts(flag.Args())
		fsm := make([]proxy.FileSystemMount, 0, len(fsmc))
		for _, c := range fsmc {
			clt, fs, err := cliCfg.CreateFs(c.Addr)

			if err != nil {
				fmt.Printf("Failed to connect to 9p server: %s\n", err)
				exitCode = 1
				runtime.Goexit()
			}
			m.Lock()
			closers = append(closers, func() { clt.Close() })
			m.Unlock()
			fsm = append(fsm, proxy.FileSystemMount{
				FS:     fs,
				Prefix: c.Prefix,
				Client: clt,
				Addr:   c.Addr,
			})
		}
		ufs := unionfs.New(func(mc proxy.FileSystemMountConfig) (proxy.FileSystemMount, error) {
			clt, fs, err := cliCfg.CreateFs(mc.Addr)

			if err != nil {
				fmt.Printf("Failed to connect to 9p server: %s\n", err)
				exitCode = 1
				runtime.Goexit()
			}
			m.Lock()
			closers = append(closers, func() { clt.Close() })
			m.Unlock()
			return proxy.FileSystemMount{
				FS:     fs,
				Client: clt,
				Prefix: mc.Prefix,
				Addr:   mc.Addr,
			}, nil
		})
		for _, m := range fsm {
			path, err := ufs.Mount(m)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Error mounting %s: %s\n", m.String(), err)
				exitCode = 1
				runtime.Goexit()
			}
			err = ufs.Bind(path, "/")
			if err != nil {
				fmt.Fprintf(os.Stderr, "Error binding %s to %s: %s\n", path, m.String(), err)
				exitCode = 1
				runtime.Goexit()
			}
		}
		return ufs
	})
}
