package main

import (
	"flag"
	"fmt"
	"io"
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

	exitcodePtr := &exitCode
	defer func() {
		os.Exit(*exitcodePtr)
	}()

	cltCfg := cli.ClientConfig{PrintPrefix: "[client] "}
	srvCfg := cli.ServerConfig{PrintPrefix: "[server] "}

	cltCfg.SetFlags(nil)
	srvCfg.SetFlags(nil)

	flag.Usage = func() {
		fmt.Fprintf(flag.CommandLine.Output(), "Usage: %s [OPTIONS] KEYS_MOUNT DIR_MOUNT\n", os.Args[0])
		flag.PrintDefaults()
	}

	flag.Parse()

	cltCfg.PrintTraceMessages = srvCfg.PrintTraceMessages
	cltCfg.PrintErrorMessages = srvCfg.PrintErrorMessages

	if flag.NArg() != 2 {
		flag.Usage()
		os.Exit(1)
	}
	fsmc := proxy.ParseMounts(flag.Args())
	clt, keysfs, err := cltCfg.CreateFs(fsmc[0].Addr)
	if err != nil {
		fmt.Printf("Failed to connect to 9p key server: %s\n", err)
		exitCode = 1
		runtime.Goexit()
	}
	defer clt.Close()

	clt, datafs, err := cltCfg.CreateFs(fsmc[1].Addr)
	if err != nil {
		fmt.Printf("Failed to connect to 9p data server: %s\n", err)
		exitCode = 1
		runtime.Goexit()
	}
	defer clt.Close()

	serviceCfg := &service.Config{
		Name:        "encryptfs",
		DisplayName: "Encrypted File System Service",
		Description: "Provides a 9p file system that proxies to two 9p file systems, encrypting both its contents",
	}
	createsrvcfg := func(stdout, stderr io.Writer) cli.ServerConfig {
		srvCfg.Stdout = stdout
		srvCfg.Stderr = stderr
		return srvCfg
	}
	createfs := func() ninep.FileSystem {
		return unionfs.New([]proxy.FileSystemMount{
			proxy.FileSystemMount{
				FS:     datafs,
				Prefix: "/data",
			},
			proxy.FileSystemMount{
				FS:     keysfs,
				Prefix: "/keys",
			},
		})
	}
	cli.ServiceMainWithFactory(serviceCfg, createsrvcfg, createfs)
}
