package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/jeffh/cfs/cli"
	"github.com/jeffh/cfs/ninep"
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

	var privateKeyPath string

	flag.StringVar(&privateKeyPath, "private-key", "", "[REQUIRED]: Path to private key to decrypt")

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

	err = srvCfg.CreateServerAndListen(func() ninep.FileSystem {
		return fs
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %s\n", err)
	}
}
