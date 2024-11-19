package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"runtime"

	"github.com/jeffh/cfs/cli"
	"github.com/jeffh/cfs/fs"
	"github.com/jeffh/cfs/fs/encryptfs"
	"github.com/jeffh/cfs/fs/proxy"
	"github.com/jeffh/cfs/ninep"
	"github.com/kardianos/service"
)

func main() {
	var exitCode = 0
	var ppkFile string
	var genKey bool

	exitcodePtr := &exitCode
	defer func() {
		os.Exit(*exitcodePtr)
	}()

	cltCfg := cli.ClientConfig{PrintPrefix: "[client] "}
	srvCfg := cli.ServerConfig{PrintPrefix: "[server] "}

	flag.StringVar(&ppkFile, "encryption-key", "private.key", "Path to the private RSA key that can decrypt the metadata.")
	flag.BoolVar(&genKey, "gen-encryption-key", false, "Generates an RSA encryption key at the path specified by -encryption-key, then exits")
	cltCfg.SetFlags(nil)
	srvCfg.SetFlags(nil)

	flag.Usage = func() {
		o := flag.CommandLine.Output()
		fmt.Fprintf(o, "Usage: %s [OPTIONS] KEYS_MOUNT DIR_MOUNT [DECRYPT_MOUNT]\n\n", os.Args[0])
		fmt.Fprintf(o, "Reads and stores encrypted data in DIR_MOUNT (note, file names are not encrypted).\n")
		fmt.Fprintf(o, "\nUses with a unique symmetric key per file stored in KEYS_MOUNT to encrypt DIR_MOUNT. Every key in KEYS_MOUNT is are encrypted via public-private key.\n")
		fmt.Fprintf(o, "DECRYPT_MOUNT is a temporary file location to hold decrypted files for reading and writing. If not provided, defaults to an in memory 9p file system.\n")
		proxy.PrintMountsHelp(o)
		fmt.Fprintf(o, "\nOPTIONS:\n")
		flag.PrintDefaults()
	}

	flag.Parse()

	cltCfg.LogLevel = srvCfg.LogLevel
	cltCfg.Logger = srvCfg.Logger

	if genKey {
		_, err := encryptfs.GeneratePrivateKey(ppkFile, encryptfs.PrivateKeyBits)
		if err != nil {
			fmt.Printf("Failed to generate key: %s: %s\n", ppkFile, err)
			exitCode = 1
		} else {
			fmt.Printf("Generated key: %s\n", ppkFile)
		}
		runtime.Goexit()
	}

	ppk, err := encryptfs.LoadPrivateKey(ppkFile)
	if err != nil {
		fmt.Printf("Failed to load private key file: %s: %s\n", ppkFile, err)
		exitCode = 1
		runtime.Goexit()
	}

	if flag.NArg() < 2 || flag.NArg() > 3 {
		flag.Usage()
		os.Exit(1)
	}
	fsmc := proxy.ParseMounts(flag.Args())
	fsm, err := cltCfg.FSMountMany(fsmc)
	if err != nil {
		fmt.Printf("Failed to connect to 9p key server: %s\n", err)
		exitCode = 1
		runtime.Goexit()
	}
	defer func() {
		for _, m := range fsm {
			m.Close()
		}
	}()
	keysMnt := fsm[0]
	dataMnt := fsm[1]
	var decryptMnt proxy.FileSystemMount
	if len(fsm) > 2 {
		decryptMnt = fsm[2]
	} else {
		decryptMnt = proxy.FileSystemMount{
			FS:     &fs.Mem{},
			Prefix: "/",
		}
	}

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
		fs := encryptfs.New(ppk, keysMnt, dataMnt, decryptMnt)

		if err := fs.Init(context.Background()); err != nil {
			fmt.Fprintf(os.Stderr, "Failed to initialize: %s", err)
			exitCode = 1
			runtime.Goexit()
		}

		return fs
	}
	cli.ServiceMainWithFactory(serviceCfg, createsrvcfg, createfs)
}
