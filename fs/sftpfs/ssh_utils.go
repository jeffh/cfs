package sftpfs

import (
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"os"
	"os/user"
	"path/filepath"
	"strings"

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
