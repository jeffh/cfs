package main

import (
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/jeffh/cfs/cli"
	"github.com/jeffh/cfs/fs/proxy"
	"github.com/jeffh/cfs/fs/unionfs"
	"github.com/jeffh/cfs/ninep"
	"github.com/kardianos/service"
)

func main() {
	var (
		timeout int
		recov   bool
	)

	flag.BoolVar(&recov, "recover", false, "Use recover client for talking over flaky/unreliable networks")
	flag.IntVar(&timeout, "client_timeout", 5, "Timeout in seconds for client requests")

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
		if recov {
			for _, c := range fsmc {
				clt := ninep.RecoverClient{
					BasicClient: ninep.BasicClient{
						Timeout: time.Duration(timeout) * time.Second,
					},
				}

				if err := clt.Connect(c.Addr); err != nil {
					fmt.Printf("Failed to connect to 9p server: %s\n", err)
					os.Exit(1)
				}
				closers = append(closers, func() { clt.Close() })

				fs, err := clt.Fs()
				if err != nil {
					fmt.Printf("Failed to attach to 9p server: %s\n", err)
					os.Exit(1)
				}

				fsm = append(fsm, proxy.FileSystemMount{
					FS:     fs,
					Prefix: c.Prefix,
				})
			}
		} else {
			for _, c := range fsmc {
				clt := ninep.BasicClient{
					Timeout: time.Duration(timeout) * time.Second,
				}

				if err := clt.Connect(c.Addr); err != nil {
					fmt.Printf("Failed to connect to 9p server: %s\n", err)
					os.Exit(1)
				}
				closers = append(closers, func() { clt.Close() })

				fs, err := clt.Fs("", "")
				if err != nil {
					fmt.Printf("Failed to attach to 9p server: %s\n", err)
					os.Exit(1)
				}

				fsm = append(fsm, proxy.FileSystemMount{
					FS:     fs,
					Prefix: c.Prefix,
				})
			}
		}
		return unionfs.New(fsm)
	})
}
