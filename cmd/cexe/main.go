package main

import (
	"encoding/gob"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"path/filepath"
	"strings"

	"github.com/jeffh/cfs/cexec"
	"github.com/jeffh/cfs/fs/sftpfs"
	"github.com/pkg/sftp"
	"golang.org/x/crypto/ssh"
)

const (
	methodForkExec      = "fork"
	methodChrootExec    = "chroot"
	methodContainerExec = "container"
)

var (
	addr    string
	sshAddr string
	sshUser string
	sshKey  string
	method  string
	mode    string
	srv     bool

	uploads stringList
	env     stringList

	root string

	progname string
	progargs []string
)

func main() {

	cexec.LinuxContainerHandleContainerExec()

	flag.StringVar(&addr, "addr", "", "The remote srv to connect to execute commands")
	flag.StringVar(&sshAddr, "ssh-addr", "", "The remote system to run this command on. Host should support SSH. Default runs against current machine.")
	flag.StringVar(&sshUser, "ssh-user", "", "The SSH Username")
	flag.StringVar(&sshKey, "ssh-key", "", "The SSH Private Key")
	flag.StringVar(&root, "root", "", "The root dir of the process. This is dependent on the method to utilize.")
	flag.StringVar(&method, "method", "", "The strategy to execute")
	flag.StringVar(&mode, "mode", "run", "How this binary operates, can be 'run' to indicate executing programs, 'srv' for receiving programs to run, or 'fork' to execute the program in question")
	flag.BoolVar(&srv, "srv", false, "Run as a server to receive requests. Default mode listens on 'localhost:6667' since it's expected to use ssh tunneling to connect.")
	flag.Var(&uploads, "upload", "Uploads file to remote system, can be used multiple times.")
	flag.Var(&env, "e", "Set an environment var to be used for the program. In form 'NAME=VALUE'. Can be used multiple times.")

	flag.Usage = func() {
		o := flag.CommandLine.Output()
		fmt.Fprintf(o, "exec for a remote system\n")
		fmt.Fprintf(o, "\n")
		fmt.Fprintf(o, "Executes a command. There's several modes of operation:\n")
		fmt.Fprintf(o, " - executing commands on the current machine\n")
		fmt.Fprintf(o, " - executing commands on a remote machine using ssh\n")
		fmt.Fprintf(o, " - executing commands on a remote machine using %s host, using ssh\n", os.Args[0])
		fmt.Fprintf(o, "\nAlong with methods for running commands (which is OS dependent):\n")
		fmt.Fprintf(o, " - exec\n")
		fmt.Fprintf(o, " - chroot + exec\n")
		fmt.Fprintf(o, " - linux namespaces + exec\n")
		fmt.Fprintf(o, "\n")
		fmt.Fprintf(o, "Note: these methods do not ensure isolation or for security.\n")
		fmt.Fprintf(o, "Note: these methods do not ensure isolation or for security.\n")
		fmt.Fprintf(o, "\n")
		fmt.Fprintf(o, "Usage: %s [OPTIONS] CMD [ARGS]\n", os.Args[0])
		flag.PrintDefaults()
	}

	flag.Parse()

	switch mode {
	case "run":
		runRemoteProgram()
	default:
		log.Fatalf("Unsupported mode: %s\n", mode)
	}
}

func runRemoteProgram() {
	if flag.NArg() < 1 {
		flag.Usage()
		os.Exit(1)
	}

	method = strings.ToLower(method)
	if method == "" {
		method = methodForkExec
	}

	var executor cexec.Executor
	switch method {
	case methodForkExec:
		executor = cexec.ForkExecutor()
	case methodChrootExec:
		executor = cexec.ChrootExecutor()
	case methodContainerExec:
		executor = cexec.LinuxContainerExecutor()
	}

	progname = flag.Arg(0)
	progargs = flag.Args()[1:]

	if srv {
		if addr == "" {
			addr = "localhost:6667"
		}

		ln, err := net.Listen("tcp", addr)
		if err != nil {
			log.Fatalf("error: %s\n", err)
		}
		defer ln.Close()

		for {
			conn, err := ln.Accept()
			if err != nil {
				log.Printf("Failed to accept connection: %s\n", err)
			}

			go handleConnection(conn, executor)
		}
	} else if sshAddr != "" {
		sshCfg, err := sftpfs.DefaultSSHConfig(sshUser, sshKey)
		if err != nil {
			log.Fatalf("error: %s\n", err)
		}
		if strings.Index(sshAddr, ":") == -1 {
			sshAddr += ":22"
		}

		// log.Printf("Connecting to %s@%s\n", sshCfg.User, sshAddr)

		conn, err := ssh.Dial("tcp", sshAddr, sshCfg)
		if err != nil {
			log.Fatalf("Failed to open ssh connection: %s\n", err)
		}

		if len(uploads) > 0 {
			// Upload bootstrap executable
			sftpConn, err := sftp.NewClient(conn)
			if err != nil {
				log.Fatalf("Failed to upload file: %s\n", err)
			}
			defer sftpConn.Close()
			for _, upload := range uploads {
				out, err := os.Open(upload)
				if err != nil {
					log.Fatalf("Failed to read executable: %s: %s\n", upload, err)
				}
				name := filepath.Base(upload)
				in, err := sftpConn.OpenFile(name, os.O_WRONLY|os.O_CREATE|os.O_TRUNC)
				if err != nil {
					out.Close()
					log.Fatalf("Failed to upload executable: %s\n", err)
				}
				_, err = io.Copy(in, out)
				out.Close()
				in.Close()
				if err != nil {
					log.Fatalf("Failed to upload executable: %s\n", err)
				}
			}

			defer func() {
				for _, upload := range uploads {
					name := filepath.Base(upload)
					if name != "" {
						sftpConn.Remove(name)
					}
				}
			}()
		}

		sess, err := conn.NewSession()
		if err != nil {
			log.Fatalf("Failed to connect to remote: %s\n", err)
		}

		defer sess.Close()

		for _, stmt := range env {
			i := strings.Index(stmt, "=")

			var err error
			if i == -1 {
				err = sess.Setenv(stmt[:i], stmt[i+1:])
			} else {
				err = sess.Setenv(stmt, os.Getenv(stmt))
			}

			if err != nil {
				log.Fatalf("Failed setting up env vars: %s\n", err)
			}
		}

		command := make([]string, 0, len(progargs)+1)
		command = append(command, fmt.Sprintf("%#v", progname))
		for _, arg := range progargs {
			command = append(command, fmt.Sprintf("%#v", arg))
		}
		sess.Stdin = os.Stdin
		sess.Stdout = os.Stdout
		sess.Stderr = os.Stderr
		err = sess.Run(strings.Join(command, " "))
		if err != nil {
			log.Fatalf("error: %s\n", err)
		}
	} else {
		cmd := cexec.Cmd{
			Cmd:    progname,
			Args:   progargs,
			Env:    env,
			Stdin:  os.Stdin,
			Stdout: os.Stdout,
			Stderr: os.Stderr,
		}
		if err := executor.Run(&cmd); err != nil {
			fmt.Fprintf(os.Stderr, "error: %s", err)
			os.Exit(1)
		}
	}
}

func handleConnection(rwc net.Conn, executor cexec.Executor) {
	defer rwc.Close()

	var cmd cexec.Cmd
	d := gob.NewDecoder(rwc)

	err := d.Decode(&cmd)
	if err != nil {
		log.Printf("error for %s: %s\n", rwc.RemoteAddr(), err)
		rwc.Write([]byte(fmt.Sprintf("error: %s", err)))
		return
	}

	cmd.Stdin = rwc
	cmd.Stdout = rwc
	cmd.Stderr = rwc

	if err := executor.Run(&cmd); err != nil {
		log.Printf("error for %s: %s", rwc.RemoteAddr(), err)
		rwc.Write([]byte(fmt.Sprintf("error: %s", err)))
		return
	}

	if cmd.State == nil {
		log.Printf("error for %s: executor did not fill State", rwc.RemoteAddr())
		rwc.Write([]byte("Internal executor error"))
		return
	}
	_, err = rwc.Write([]byte(fmt.Sprintf("\n\nExited: %v", cmd.State.ExitCode)))
	if err != nil {
		log.Printf("error for %s: %s", rwc.RemoteAddr(), err)
		rwc.Write([]byte(fmt.Sprintf("error: %s", err)))
		return
	}
}

type ReqCmd struct {
	Cmd    string
	Args   []string
	Env    []string
	Dir    string
	Stdin  string
	Stdout string
	Stderr string

	Root                 string
	IsolateHostAndDomain bool
	IsolatePids          bool
	IsolateIPC           bool
	IsolateNetwork       bool
	IsolateUsers         bool
}
