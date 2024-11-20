package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"runtime"
	"strings"

	"github.com/jeffh/cfs/cli"
	"github.com/jeffh/cfs/fs/sftpfs"
	"github.com/jeffh/cfs/ninep"
	"golang.org/x/crypto/ssh"
)

func main() {
	var (
		username string
		sshKey   string
		prefix   string

		exitCode int
	)

	defer func() {
		if exitCode != 0 {
			os.Exit(exitCode)
		}
	}()

	flag.StringVar(&username, "ssh-user", "", "SSH Username")
	flag.StringVar(&sshKey, "ssh-key", "", "SSH Private Key")
	flag.StringVar(&prefix, "prefix", "", "SFTP directory prefix")

	flag.Usage = func() {
		fmt.Fprintf(flag.CommandLine.Output(), "sftp for CFS\n")
		fmt.Fprintf(flag.CommandLine.Output(), "Usage: %s [OPTIONS] SFTP_ADDR\n", os.Args[0])
		flag.PrintDefaults()
	}

	cli.ServiceMain(func() ninep.FileSystem {
		if flag.NArg() < 1 {
			flag.Usage()
			exitCode = 1
			runtime.Goexit()
		}

		sshConfig, err := sftpfs.DefaultSSHConfig(username, sshKey)
		if err != nil {
			log.Fatalf("error: %s\n", err)
		}

		sshAddr := flag.Arg(0)
		if strings.Index(sshAddr, ":") == -1 {
			sshAddr += ":22"
		}

		log.Printf("Connecting to %s@%s\n", sshConfig.User, sshAddr)
		conn, err := ssh.Dial("tcp", sshAddr, sshConfig)
		if err != nil {
			log.Fatalf("Failed to open ssh connection: %s\n", err)
		}

		fs, err := sftpfs.New(conn, prefix)
		if err != nil {
			defer conn.Close()
			log.Fatalf("Failed to create sftp connection: %s\n", err)
		}
		return fs
	})
}
