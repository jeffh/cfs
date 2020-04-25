package cli

import (
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"os/user"
	"path/filepath"
	"runtime"

	"github.com/jeffh/cfs/fs"
	"github.com/jeffh/cfs/fs/proxy"
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

func (c *ClientConfig) FSMount(mnt *proxy.FileSystemMountConfig) (proxy.FileSystemMount, error) {
	switch mnt.Addr {
	case ":memory":
		return proxy.FileSystemMount{&fs.Mem{}, mnt.Prefix, nil, nil}, nil
	case ":tmp":
		dir, err := ioutil.TempDir("", "*")
		if err != nil {
			fmt.Fprintf(os.Stderr, "Failed to create temp directory: %s", err)
		}
		clean := func() error {
			if err := os.RemoveAll(dir); err != nil {
				fmt.Fprintf(os.Stderr, "Failed to clean temp directory: %s: %s", dir, err)
				return err
			}
			return nil
		}
		return proxy.FileSystemMount{fs.Dir(dir), mnt.Prefix, nil, clean}, nil
	case "", ".":
		return proxy.FileSystemMount{fs.Dir(filepath.Join(mnt.Addr, mnt.Prefix)), "", nil, nil}, nil
	default:
		client, fs, err := c.CreateFs(mnt.Addr)
		if err != nil {
			return proxy.FileSystemMount{}, fmt.Errorf("Failed connecting to %s/%s: %w", mnt.Addr, mnt.Prefix, err)
		}
		return proxy.FileSystemMount{fs, mnt.Prefix, client, nil}, nil
	}
}

func (c *ClientConfig) FSMountMany(cfgs []proxy.FileSystemMountConfig) ([]proxy.FileSystemMount, error) {
	mnts := make([]proxy.FileSystemMount, 0, len(cfgs))
	for _, cfg := range cfgs {
		m, err := c.FSMount(&cfg)
		if err != nil {
			for _, m := range mnts {
				m.Close()
			}
			return nil, err
		}
		mnts = append(mnts, m)
	}
	return mnts, nil
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

	loggable := ninep.Loggable{
		ErrorLog: errLogger,
		TraceLog: traceLogger,
	}

	var transport ninep.ClientTransport

	if c.UseRecoverClient {
		if traceLogger != nil {
			traceLogger.Printf("Using retry client\n")
		}
		transport = &ninep.SerialRetryClientTransport{}
	} else {
		if traceLogger != nil {
			traceLogger.Printf("Using basic client\n")
		}
		transport = &ninep.SerialClientTransport{}
	}

	clt := ninep.BasicClient{
		Transport: transport,
		Loggable:  loggable,
		User:      usr,
		Mount:     c.Mount,
	}

	if err = clt.Connect(addr); err != nil {
		return nil, fmt.Errorf("Failed to connect to 9p server: %s\n", err)
	}
	return &clt, nil
}

func (c *ClientConfig) CreateFs(addr string) (ninep.Client, *ninep.FileSystemProxy, error) {
	client, err := c.CreateClient(addr)
	if err != nil {
		return nil, nil, err
	}
	fs, err := client.Fs()
	if err != nil {
		client.Close()
		return nil, nil, fmt.Errorf("Failed to attach to 9p server: %s\n", err)
	}
	return client, fs, nil
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
