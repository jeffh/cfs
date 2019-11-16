package cli

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/user"
	"time"

	"github.com/jeffh/cfs/ninep"
)

func MainClient(fn func(c *ninep.Client, fs *ninep.FileSystemProxy) error) {
	var (
		trace  bool
		errLog bool

		usr   string
		mount string

		timeout int

		err error
	)

	flag.StringVar(&usr, "user", "", "Username to connect as, defaults to current system user")
	flag.StringVar(&mount, "mount", "", "Default access path, defaults to empty string")
	flag.IntVar(&timeout, "timeout", 5, "Timeout in seconds for client requests")
	flag.BoolVar(&trace, "trace", false, "Print trace of 9p server to stdout")
	flag.BoolVar(&errLog, "err", false, "Print errors of 9p server to stderr")

	flag.Parse()

	if flag.NArg() == 0 {
		flag.Usage()
		os.Exit(1)
	}

	addr := flag.Arg(0)

	if usr == "" {
		u, err := user.Current()
		if err != nil {
			usr = "9puser"
		}
		usr = u.Username
	}

	var traceLogger, errLogger ninep.Logger

	if trace {
		traceLogger = log.New(os.Stdout, "", log.LstdFlags)
	}
	if errLog {
		errLogger = log.New(os.Stderr, "", log.LstdFlags)
	}

	clt := ninep.Client{
		Timeout: time.Duration(timeout) * time.Second,
		Loggable: ninep.Loggable{
			ErrorLog: errLogger,
			TraceLog: traceLogger,
		},
	}

	if err = clt.Connect(addr); err != nil {
		fmt.Printf("Failed to connect to 9p server: %s\n", err)
		os.Exit(1)
	}
	defer clt.Close()

	fs, err := clt.Fs(usr, mount)
	if err != nil {
		fmt.Printf("Failed to attach to 9p server: %s\n", err)
		os.Exit(1)
	}

	err = fn(&clt, fs)
	if err != nil {
		fmt.Printf("Failed: %s\n", err)
		os.Exit(1)
	}
}
