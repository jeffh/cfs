package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"os"
	"os/user"
	"path/filepath"
	"runtime"
	"strings"

	"github.com/jeffh/cfs/cli"
	"github.com/jeffh/cfs/fs/sftpfs"
	"github.com/jeffh/cfs/ninep"
	"github.com/kardianos/service"
	"golang.org/x/crypto/ssh"
	"golang.org/x/crypto/ssh/agent"
	"golang.org/x/crypto/ssh/terminal"
)

func publicKeyFile(file string) ssh.AuthMethod {
	if strings.HasPrefix(file, "~/") {
		usr, _ := user.Current()
		file = filepath.Join(usr.HomeDir, file[2:])
	}

	buffer, err := ioutil.ReadFile(file)
	if err != nil {
		return nil
	}

	key, err := ssh.ParsePrivateKey(buffer)
	if err != nil {
		if _, ok := err.(*ssh.PassphraseMissingError); ok {
			fmt.Printf("Please enter passphrase for %v: ", file)
			password, _ := terminal.ReadPassword(0)
			key, err = ssh.ParsePrivateKeyWithPassphrase(buffer, password)
			println("")
		}
		if err != nil {
			log.Printf("failed to parse file: %v -- %v: %s", file, len(buffer), err)
			return nil
		}
	}
	return ssh.PublicKeys(key)
}

func removeNils(methods []ssh.AuthMethod) []ssh.AuthMethod {
	res := make([]ssh.AuthMethod, 0, len(methods))
	for _, m := range methods {
		if m != nil {
			res = append(res, m)
		}
	}
	return res
}

func SSHAgent() ssh.AuthMethod {
	sshAgent, err := net.Dial("unix", os.Getenv("SSH_AUTH_SOCK"))
	if err != nil {
		log.Printf("Error connecting to ssh agent: %s\n", err)
		return nil
	}
	return ssh.PublicKeysCallback(agent.NewClient(sshAgent).Signers)
}

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

	cfg := &service.Config{
		Name:        "sftpfs",
		DisplayName: "SSH File Transfer Protocol File System Service",
		Description: "Provide a 9p file system that exposts an sftp server",
	}

	cli.ServiceMain(cfg, func() ninep.FileSystem {
		if flag.NArg() < 1 {
			flag.Usage()
			exitCode = 1
			runtime.Goexit()
		}

		if username == "" {
			user, err := user.Current()
			if err != nil {
				log.Fatalf("Failed to determine current user: %s", err)
			}
			username = user.Username
		}

		sshConfig := &ssh.ClientConfig{
			User: username,
			Auth: removeNils([]ssh.AuthMethod{
				SSHAgent(),
				publicKeyFile(sshKey),
				// publicKeyFile("~/.ssh/id_ed25519"),
				// publicKeyFile("~/.ssh/id_rsa"),
				// publicKeyFile("~/.ssh/id_dsa"),
			}),
			HostKeyCallback: ssh.InsecureIgnoreHostKey(), // TODO: read local hosts file
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
