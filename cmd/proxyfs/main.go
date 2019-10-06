package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/user"
	"time"

	"git.sr.ht/~jeffh/cfs/fs"
	"git.sr.ht/~jeffh/cfs/ninep"
)

func main() {
	var (
		// client
		srcAddr string

		usr   string
		mount string

		timeout int

		// server
		addr    string
		trace   bool
		errLog  bool
		tracefs bool

		certFile string
		keyFile  string

		err error
	)

	// client
	flag.StringVar(&usr, "user", "", "Username to connect as, defaults to current system user")
	flag.StringVar(&mount, "mount", "", "Default access path, defaults to empty string")
	flag.IntVar(&timeout, "timeout", 5, "Timeout in seconds for client requests")

	// server
	flag.StringVar(&addr, "addr", "localhost:564", "The address and port to listen the 9p server. Defaults to 'localhost:564'.")
	flag.BoolVar(&trace, "trace", false, "Print trace of 9p server to stdout")
	flag.BoolVar(&tracefs, "tracefs", false, "Print trace of 9p FileSystem interface to stdout")
	flag.BoolVar(&errLog, "err", false, "Print errors of 9p server to stderr")
	flag.StringVar(&certFile, "certfile", "", "Accept only TLS wrapped connections. Also needs to specify keyfile flag.")
	flag.StringVar(&keyFile, "keyfile", "", "Accept only TLS wrapped connections. Also needs to specify certfile flag.")

	flag.Usage = func() {
		fmt.Fprintf(flag.CommandLine.Output(), "Usage: %s [OPTIONS] FORWARD_ADDR\n", os.Args[0])
		flag.PrintDefaults()
	}

	flag.Parse()

	if flag.NArg() == 0 {
		flag.Usage()
		os.Exit(1)
	}
	srcAddr = flag.Arg(0)

	var (
		srvTraceLogger, srvErrLogger ninep.Logger
		cltTraceLogger, cltErrLogger ninep.Logger
	)

	if trace {
		srvTraceLogger = log.New(os.Stdout, "[server] ", log.LstdFlags)
		cltTraceLogger = log.New(os.Stdout, "[client] ", log.LstdFlags)
	}
	if errLog {
		srvErrLogger = log.New(os.Stderr, "[server] ", log.LstdFlags)
		cltErrLogger = log.New(os.Stderr, "[client] ", log.LstdFlags)
	}

	var fsys ninep.FileSystem
	{
		if usr == "" {
			u, err := user.Current()
			if err != nil {
				usr = "9puser"
			}
			usr = u.Username
		}
		clt := ninep.Client{
			Timeout: time.Duration(timeout) * time.Second,
			Loggable: ninep.Loggable{
				ErrorLog: cltErrLogger,
				TraceLog: cltTraceLogger,
			},
		}
		err = clt.Connect(srcAddr)
		if err != nil {
			fmt.Printf("Error: %s", err)
			os.Exit(1)
		}
		fsys, err = clt.Fs(usr, mount)
		if err != nil {
			fmt.Printf("Error: %s", err)
			os.Exit(1)
		}
	}

	if tracefs {
		fsys = fs.TraceFileSystem{
			Fs:       fsys,
			Loggable: ninep.Loggable{log.New(os.Stdout, "[tracefs] ", log.LstdFlags), log.New(os.Stdout, "[tracefs] ", log.LstdFlags)},
		}
	}

	srv := ninep.NewServer(fsys, srvErrLogger, srvTraceLogger)
	if certFile != "" && keyFile != "" {
		err = srv.ListenAndServeTLS(addr, certFile, keyFile)
	} else {
		err = srv.ListenAndServe(addr)
	}
	fmt.Printf("Error: %s", err)
}
