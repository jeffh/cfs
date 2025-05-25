package main

import (
	"encoding/gob"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"log"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"strings"

	"github.com/jeffh/cfs/cexec"
	"github.com/jeffh/cfs/fs/sftpfs"
	"github.com/pkg/sftp"
	_ "go.uber.org/automaxprocs"
	"golang.org/x/crypto/ssh"
)

const (
	methodForkExec      = "fork"
	methodChrootExec    = "chroot"
	methodContainerExec = "container"
)

const TERM_BYTE = byte(0)

var (
	addr    string
	sshAddr string
	sshUser string
	sshKey  string
	method  string
	mode    string

	uploads stringList
	env     stringList

	root string

	progname string
	progargs []string

	exitCode int

	logCommands bool
	uploadCmd   string
)

func main() {

	cexec.LinuxContainerHandleContainerExec()

	flag.StringVar(&addr, "addr", "localhost:7171", "The remote srv to connect to execute commands")
	flag.StringVar(&sshAddr, "ssh-addr", "", "The remote system to run this command on. Host should support SSH. Default runs against current machine.")
	flag.StringVar(&sshUser, "ssh-user", "", "The SSH Username")
	flag.StringVar(&sshKey, "ssh-key", "", "The SSH Private Key")
	flag.StringVar(&root, "root", "", "The root dir of the process. This is dependent on the method to utilize.")
	flag.StringVar(&method, "method", methodForkExec, "The strategy to execute.  Can be 'fork', 'chroot', or 'container'")
	flag.StringVar(&mode, "mode", "run", "How this binary operates, can be 'run' to indicate executing programs, 'server' for receiving programs to run, or 'client' to execute the program in question")
	flag.StringVar(&uploadCmd, "upload-cmd", "", "Uploads a file to the remote server before running the command on the server as the environment variable: $CEXE_FILE")
	flag.Var(&uploads, "upload", "Uploads file to remote system, can be used multiple times. Only works for ssh executions with mode=run")
	flag.Var(&env, "e", "Set an environment var to be used for the program. In form 'NAME=VALUE'. Can be used multiple times.")
	flag.BoolVar(&logCommands, "log-cmds", false, "Print commands to stdout. Only affects server.")

	flag.Usage = func() {
		o := flag.CommandLine.Output()
		_, _ = fmt.Fprintf(o, "Usage: %s [OPTIONS] CMD [ARGS]\n\n", os.Args[0])
		_, _ = fmt.Fprintf(o, "exec for a remote system\n")
		_, _ = fmt.Fprintf(o, "\n")
		_, _ = fmt.Fprintf(o, "Executes a command. There's several modes of operation:\n")
		_, _ = fmt.Fprintf(o, " - executing commands on the current machine\n")
		_, _ = fmt.Fprintf(o, " - executing commands on a remote machine using ssh\n")
		_, _ = fmt.Fprintf(o, " - executing commands on a remote machine using %s host, using ssh\n", os.Args[0])
		_, _ = fmt.Fprintf(o, "\nAlong with methods for running commands (which is OS dependent):\n")
		_, _ = fmt.Fprintf(o, " - exec\n")
		_, _ = fmt.Fprintf(o, " - chroot + exec\n")
		_, _ = fmt.Fprintf(o, " - linux namespaces + exec\n")
		_, _ = fmt.Fprintf(o, "\n")
		_, _ = fmt.Fprintf(o, "Note: these methods do not ensure isolation or for security.\n")
		_, _ = fmt.Fprintf(o, "\n")
		flag.PrintDefaults()
	}

	flag.Parse()

	exitCode = -1
	defer func() {
		if exitCode > -1 {
			os.Exit(exitCode)
		}
	}()

	if addr == "" {
		addr = "localhost:6667"
	}

	switch mode {
	case "run":
		runRemoteProgram()
	case "client":
		runRequest()
	case "server":
		runServer()
	default:
		log.Fatalf("Unsupported mode: %s\n", mode)
	}
}

func exit(code int) {
	exitCode = code
	runtime.Goexit()
}

func runRequest() {
	if flag.NArg() < 1 {
		flag.Usage()
		exit(1)
	}

	progname = flag.Arg(0)
	progargs = flag.Args()[1:]

	var stdin io.Reader
	stat, err := os.Stdin.Stat()
	if err != nil {
		log.Fatalf("Stdin error: %s", err)
	}
	if stat.Mode()&fs.ModeCharDevice == 0 {
		stdin = os.Stdin
		defer func() { _ = os.Stdin.Close() }()
	}

	var con net.Conn
	if sshAddr != "" {
		sshCfg, err := sftpfs.DefaultSSHConfig(sshUser, sshKey)
		if err != nil {
			log.Fatalf("error: %s\n", err)
		}
		if !strings.Contains(sshAddr, ":") {
			sshAddr += ":22"
		}

		// log.Printf("Connecting to %s@%s\n", sshCfg.User, sshAddr)

		sshClient, err := ssh.Dial("tcp", sshAddr, sshCfg)
		if err != nil {
			log.Fatalf("Failed to open ssh connection: %s\n", err)
		}

		if len(uploads) > 0 {
			// Upload bootstrap executable
			sftpConn, err := sftp.NewClient(sshClient)
			if err != nil {
				log.Fatalf("Failed to upload file: %s\n", err)
			}
			defer func() { _ = sftpConn.Close() }()
			for _, upload := range uploads {
				out, err := os.Open(upload)
				if err != nil {
					log.Fatalf("Failed to read executable: %s: %s\n", upload, err)
				}
				name := filepath.Base(upload)
				in, err := sftpConn.OpenFile(name, os.O_WRONLY|os.O_CREATE|os.O_TRUNC)
				if err != nil {
					_ = out.Close()
					log.Fatalf("Failed to upload executable: %s\n", err)
				}
				_, err = io.Copy(in, out)
				_ = out.Close()
				_ = in.Close()
				if err != nil {
					log.Fatalf("Failed to upload executable: %s\n", err)
				}
			}

			defer func() {
				for _, upload := range uploads {
					name := filepath.Base(upload)
					if name != "" {
						_ = sftpConn.Remove(name)
					}
				}
			}()
		}

		sess, err := sshClient.NewSession()
		if err != nil {
			log.Fatalf("Failed to connect to remote: %s\n", err)
		}

		defer func() { _ = sess.Close() }()

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

		con, err = sshClient.Dial("tcp", addr)
		if err != nil {
			log.Fatalf("Failed to connect to server: %s: %s", addr, err)
		}
	} else {
		con, err = net.Dial("tcp", addr)
		if err != nil {
			log.Fatalf("Failed to connect to server: %s: %s", addr, err)
		}
	}
	defer func() { _ = con.Close() }()

	e := gob.NewEncoder(con)
	req := ReqCmd{
		Cmd:   progname,
		Args:  progargs,
		Env:   env,
		Root:  root,
		Stdin: stdin != nil,
	}

	if uploadCmd != "" {
		f, err := os.Open(uploadCmd)
		if err != nil {
			log.Fatalf("Failed to open binary to upload: %s", err)
		}
		data, err := io.ReadAll(f)
		_ = f.Close()
		if err != nil {
			log.Fatalf("Failed to read binary to upload: %s", err)
		}
		req.ProgData = data
	}

	err = e.Encode(&req)
	if err != nil {
		log.Fatalf("Failed to encode request: %s: %s", addr, err)
	}

	if stdin != nil {
		go func() {
			var tmp [4096]byte
			var r ReqMessage
			buf := tmp[:]
			for {
				n, err := stdin.Read(buf)
				r.Text = buf[:n]

				if er := e.Encode(&r); er != nil {
					log.Fatalf("Failed to encode stdin: %s: %s", addr, er)
				}

				if err != nil {
					if errors.Is(err, io.EOF) {
						break
					}
					log.Fatalf("Failed to write stdin: %s: %s", addr, err)
				}
			}
			r.Text = nil
			r.Kind = ReqMessageTypeWait

			if err := e.Encode(&r); err != nil {
				log.Fatalf("Failed to encode wait: %s: %s", addr, err)
			}
		}()
	}

	d := gob.NewDecoder(con)

	var exitCode int
	for {
		var m Message
		if err := d.Decode(&m); err != nil {
			if !errors.Is(err, io.EOF) {
				log.Fatalf("Failed to parse response: %s: %s", addr, err)
				exitCode = 1
			}
			break
		}

		switch m.Kind {
		case MessageTypeStdout:
			_, _ = os.Stdout.Write(m.Text)
		case MessageTypeStderr:
			_, _ = os.Stderr.Write(m.Text)
		case MessageTypeError:
			_, _ = os.Stderr.Write(m.Text)
			exitCode = m.ExitCode
		case MessageTypeExited:
			_, _ = os.Stderr.Write(m.Text)
			exitCode = m.ExitCode
		}
	}
	os.Exit(exitCode)
}

func runServer() {
	method = strings.ToLower(method)
	var executor cexec.Executor
	switch method {
	case methodForkExec:
		executor = cexec.ForkExecutor()
	case methodChrootExec:
		executor = cexec.ChrootExecutor()
	case methodContainerExec:
		executor = cexec.LinuxContainerExecutor()
	default:
		log.Fatalf("Unsupported executor: %#v", method)
	}

	ln, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatalf("error: %s\n", err)
	}
	log.Printf("Listening on %s\n", ln.Addr())
	defer func() { _ = ln.Close() }()

	for {
		conn, err := ln.Accept()
		if err != nil {
			log.Printf("Failed to accept connection: %s\n", err)
		}

		go handleConnection(conn, executor)
	}
}

func runRemoteProgram() {
	if flag.NArg() < 1 {
		flag.Usage()
		exit(1)
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

	var stdin io.Reader
	stat, err := os.Stdin.Stat()
	if err != nil {
		log.Fatalf("Stdin error: %s", err)
	}
	if stat.Mode()&fs.ModeCharDevice == 0 {
		stdin = os.Stdin
	}

	if sshAddr != "" {
		sshCfg, err := sftpfs.DefaultSSHConfig(sshUser, sshKey)
		if err != nil {
			log.Fatalf("error: %s\n", err)
		}
		if !strings.Contains(sshAddr, ":") {
			sshAddr += ":22"
		}

		// log.Printf("Connecting to %s@%s\n", sshCfg.User, sshAddr)

		sshClient, err := ssh.Dial("tcp", sshAddr, sshCfg)
		if err != nil {
			log.Fatalf("Failed to open ssh connection: %s\n", err)
		}

		if len(uploads) > 0 {
			// Upload bootstrap executable
			sftpConn, err := sftp.NewClient(sshClient)
			if err != nil {
				log.Fatalf("Failed to upload file: %s\n", err)
			}
			defer func() { _ = sftpConn.Close() }()
			for _, upload := range uploads {
				out, err := os.Open(upload)
				if err != nil {
					log.Fatalf("Failed to read executable: %s: %s\n", upload, err)
				}
				name := filepath.Base(upload)
				in, err := sftpConn.OpenFile(name, os.O_WRONLY|os.O_CREATE|os.O_TRUNC)
				if err != nil {
					_ = out.Close()
					log.Fatalf("Failed to upload executable: %s\n", err)
				}
				_, err = io.Copy(in, out)
				_ = out.Close()
				_ = in.Close()
				if err != nil {
					log.Fatalf("Failed to upload executable: %s\n", err)
				}
			}

			defer func() {
				for _, upload := range uploads {
					name := filepath.Base(upload)
					if name != "" {
						_ = sftpConn.Remove(name)
					}
				}
			}()
		}

		sess, err := sshClient.NewSession()
		if err != nil {
			log.Fatalf("Failed to connect to remote: %s\n", err)
		}

		defer func() { _ = sess.Close() }()

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
		sess.Stdin = stdin
		sess.Stdout = os.Stdout
		sess.Stderr = os.Stderr
		err = sess.Run(strings.Join(command, " "))
		if err != nil {
			log.Fatalf("error: %s\n", err)
		}
	} else {
		if uploadCmd != "" {
			_ = env.Set(fmt.Sprintf("CEXE_FILE=%#v", uploadCmd))
		}
		cmd := cexec.Cmd{
			Cmd:    progname,
			Args:   progargs,
			Env:    env,
			Stdin:  stdin,
			Root:   root,
			Stdout: os.Stdout,
			Stderr: os.Stderr,
		}
		if err := executor.Run(&cmd); err != nil {
			fmt.Fprintf(os.Stderr, "error: %s", err)
			exit(1)
		}
		exit(cmd.State.ExitCode)
	}
}

type MessageType int

const (
	MessageTypeNone MessageType = iota
	MessageTypeStdout
	MessageTypeStderr
	MessageTypeError
	MessageTypeExited
)

type Message struct {
	Kind     MessageType
	Text     []byte
	ExitCode int
}

type messageWriter struct {
	e    *gob.Encoder
	kind MessageType
}

func (w *messageWriter) Write(p []byte) (int, error) {
	m := Message{
		Kind:     w.kind,
		Text:     p,
		ExitCode: 0,
	}
	err := w.e.Encode(&m)
	return len(p), err
}

func handleConnection(rwc net.Conn, executor cexec.Executor) {
	defer func() { _ = rwc.Close() }()

	var req ReqCmd
	d := gob.NewDecoder(rwc)
	e := gob.NewEncoder(rwc)

	err := d.Decode(&req)
	if err != nil {
		log.Printf("error for %s: %s\n", rwc.RemoteAddr(), err)
		err = e.Encode(&Message{
			Kind:     MessageTypeError,
			Text:     []byte(err.Error()),
			ExitCode: 1,
		})
		if err != nil {
			log.Printf("error encoding message for %s: %s\n", rwc.RemoteAddr(), err)
		}
		return
	}

	if req.ProgData != nil {
		f, err := os.CreateTemp("", "cexe-")
		name := f.Name()
		if err != nil {
			log.Printf("Failed to create temp file for %s: %s", rwc.RemoteAddr(), err)
			return
		}
		defer func() { _ = f.Close() }()
		_, err = f.Write(req.ProgData)
		if err != nil {
			log.Printf("Failed to write temp file for %s: %s", rwc.RemoteAddr(), err)
		}
		req.Env = append(req.Env, fmt.Sprintf("CEXE_FILE=%#v", name))

		if req.Cmd == "$CEXE_FILE" {
			req.Cmd = name
		}
		for i, v := range req.Args {
			if v == "$CEXE_FILE" {
				req.Args[i] = name
			}
		}
	}

	var stdin io.Reader

	if req.Stdin {
		r, w := io.Pipe()
		defer func() { _ = r.Close() }()
		stdin = r

		go func() {
			for {
				var req ReqMessage
				if err := d.Decode(&req); err != nil {
					break
				}

				switch req.Kind {
				case ReqMessageTypeStdin:
					_, _ = w.Write(req.Text)
				case ReqMessageTypeWait:
					_, _ = w.Write(req.Text)
					_ = w.Close()
					return
				case ReqMessageTypeSignal:
					// TODO: handle signal
					_ = w.Close()
					return
				}
			}
		}()
	}

	cmd := cexec.Cmd{
		Cmd:  req.Cmd,
		Args: req.Args,
		Env:  req.Env,
		Dir:  req.Dir,

		Root:                 req.Root,
		IsolateHostAndDomain: req.IsolateHostAndDomain,
		IsolatePids:          req.IsolatePids,
		IsolateIPC:           req.IsolateIPC,
		IsolateNetwork:       req.IsolateNetwork,
		IsolateUsers:         req.IsolateUsers,

		Stdin:  stdin,
		Stdout: &messageWriter{e, MessageTypeStdout},
		Stderr: &messageWriter{e, MessageTypeStderr},
	}

	if logCommands {
		log.Printf("%s start %v: %s %s\n", rwc.RemoteAddr(), executor.Name(), cmd.Cmd, strings.Join(cmd.Args, " "))
	}

	if err := executor.Run(&cmd); err != nil {
		if cmd.State == nil || cmd.State.ExitCode == 0 {
			log.Printf("error for %s: %s", rwc.RemoteAddr(), err)
			m := Message{
				Kind:     MessageTypeError,
				Text:     []byte(fmt.Sprintf("error: %s", err)),
				ExitCode: 254,
			}
			if err := e.Encode(&m); err != nil {
				log.Printf("error for %s: failed encoding value: %s", rwc.RemoteAddr(), err)
			}
			return
		}
		if cmd.State.ExitCode != 0 {
			log.Printf("error for %s: %s", rwc.RemoteAddr(), err)
			m := Message{
				Kind:     MessageTypeError,
				Text:     []byte(fmt.Sprintf("error: %s", err)),
				ExitCode: cmd.State.ExitCode,
			}
			if err := e.Encode(&m); err != nil {
				log.Printf("error for %s: failed encoding value: %s", rwc.RemoteAddr(), err)
			}
			return
		}
	}

	if cmd.State == nil {
		log.Printf("error for %s: executor did not fill State", rwc.RemoteAddr())
		m := Message{
			Kind:     MessageTypeError,
			Text:     []byte("Internal executor error"),
			ExitCode: 255,
		}
		if err := e.Encode(&m); err != nil {
			log.Printf("error for %s: failed encoding value: %s", rwc.RemoteAddr(), err)
		}
		return
	}

	m := Message{
		Kind:     MessageTypeExited,
		Text:     nil,
		ExitCode: cmd.State.ExitCode,
	}
	if err := e.Encode(&m); err != nil {
		log.Printf("error for %s: %s", rwc.RemoteAddr(), err)
	}

	if logCommands {
		log.Printf("%s exited %d: %s %s\n", rwc.RemoteAddr(), cmd.State.ExitCode, cmd.Cmd, strings.Join(cmd.Args, " "))
	}
}

type ReqCmd struct {
	Cmd  string
	Args []string
	Env  []string
	Dir  string

	ProgData []byte

	Root                 string
	IsolateHostAndDomain bool
	IsolatePids          bool
	IsolateIPC           bool
	IsolateNetwork       bool
	IsolateUsers         bool
	Stdin                bool
}

type ReqMessageType int

const (
	ReqMessageTypeStdin ReqMessageType = iota
	ReqMessageTypeWait
	ReqMessageTypeSignal
)

type ReqMessage struct {
	Kind   ReqMessageType
	Text   []byte
	Signal int
}
