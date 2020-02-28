package cli

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/user"
	"runtime"
	"time"

	"github.com/jeffh/cfs/ninep"
)

type ClientConfig struct {
	PrintTraceMessages bool
	PrintErrorMessages bool
	UseRecoverClient   bool

	PrintPrefix string

	User  string
	Mount string

	TimeoutInSeconds int
}

func (c *ClientConfig) SetFlags(f Flags) {
	if f == nil {
		f = &StdFlags{}
	}
	f.StringVar(&c.User, "user", "", "Username to connect as, defaults to current system user")
	f.StringVar(&c.Mount, "mount", "", "Default access path, defaults to empty string")
	f.IntVar(&c.TimeoutInSeconds, "timeout", 5, "Timeout in seconds for client requests")
	f.BoolVar(&c.PrintTraceMessages, "trace", false, "Print trace of 9p client to stdout")
	f.BoolVar(&c.PrintErrorMessages, "err", false, "Print errors of 9p client to stderr")
	f.BoolVar(&c.UseRecoverClient, "recover", false, "Use recover client for talking over flaky/unreliable networks")
}

func (c *ClientConfig) user() string {
	if c.User == "" {
		u, err := user.Current()
		if err != nil {
			c.User = "9puser"
		}
		c.User = u.Username
	}
	return c.User
}

func (c *ClientConfig) CreateClient(addr string) (ninep.Client, error) {
	var err error
	usr := c.user()
	var traceLogger, errLogger ninep.Logger

	if c.PrintTraceMessages {
		traceLogger = log.New(os.Stdout, c.PrintPrefix, log.LstdFlags)
	}
	if c.PrintErrorMessages {
		errLogger = log.New(os.Stderr, c.PrintPrefix, log.LstdFlags)
	}

	if c.UseRecoverClient {
		if traceLogger != nil {
			traceLogger.Printf("Using recover client\n")
		}
		clt := ninep.RecoverClient{
			BasicClient: ninep.BasicClient{
				Timeout: time.Duration(c.TimeoutInSeconds) * time.Second,
				Loggable: ninep.Loggable{
					ErrorLog: errLogger,
					TraceLog: traceLogger,
				},
			},
			User:  usr,
			Mount: c.Mount,
		}

		if err = clt.Connect(addr); err != nil {
			return nil, fmt.Errorf("Failed to connect to 9p server: %s\n", err)
		}
		return &clt, nil
	} else {
		clt := ninep.BasicClient{
			Timeout: time.Duration(c.TimeoutInSeconds) * time.Second,
			Loggable: ninep.Loggable{
				ErrorLog: errLogger,
				TraceLog: traceLogger,
			},
		}

		if err = clt.Connect(addr); err != nil {
			return nil, fmt.Errorf("Failed to connect to 9p server: %s\n", err)
		}
		return &clt, nil
	}
}

func (c *ClientConfig) CreateFs(addr string) (ninep.Client, *ninep.FileSystemProxy, error) {
	client, err := c.CreateClient(addr)
	if err != nil {
		return nil, nil, err
	}
	if c.UseRecoverClient {
		clt := client.(*ninep.RecoverClient)
		fs, err := clt.Fs()
		if err != nil {
			clt.Close()
			return nil, nil, fmt.Errorf("Failed to attach to 9p server: %s\n", err)
		}
		return clt, fs, nil
	} else {
		clt := client.(*ninep.BasicClient)
		fs, err := clt.Fs(c.user(), c.Mount)
		if err != nil {
			clt.Close()
			return nil, nil, fmt.Errorf("Failed to attach to 9p server: %s\n", err)
		}
		return clt, fs, nil
	}
}

func MainClient(fn func(c ninep.Client, fs *ninep.FileSystemProxy) error) {
	var (
		cfg ClientConfig

		exitCode int

		err error
	)

	defer func() {
		os.Exit(exitCode)
	}()

	cfg.SetFlags(nil)

	flag.Parse()

	if flag.NArg() == 0 {
		flag.Usage()
		exitCode = 1
		runtime.Goexit()
	}

	addr := flag.Arg(0)

	clt, fs, err := cfg.CreateFs(addr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %s\n", err)
		exitCode = 1
		runtime.Goexit()
	}
	defer clt.Close()

	err = fn(clt, fs)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed: %s\n", err)
		exitCode = 1
		runtime.Goexit()
	}
}
