package cli

import (
	"fmt"
	"log"
	"os"
	"os/signal"
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

	User string
	Root string

	TimeoutInSeconds int

	f Flags
}

func (c *ClientConfig) SetFlags(f Flags) {
	if f == nil {
		f = &StdFlags{}
	}
	f.StringVar(&c.User, "user", "", "Username to connect as, defaults to current system user")
	f.StringVar(&c.Root, "clientroot", "", "Default access path, defaults to empty string")
	f.IntVar(&c.TimeoutInSeconds, "timeout", 5, "Timeout in seconds for client requests")
	f.BoolVar(&c.PrintTraceMessages, "trace", false, "Print trace of 9p client to stdout")
	f.BoolVar(&c.PrintErrorMessages, "err", false, "Print errors of 9p client to stderr")
	f.BoolVar(&c.UseRecoverClient, "recover", false, "Use recover client for talking over flaky/unreliable networks")
	c.f = f
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
		return proxy.FileSystemMount{&fs.Mem{}, mnt.Prefix, nil, mnt.Addr, nil}, nil
	case ":tmp":
		dir, err := os.MkdirTemp("", "*")
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
		return proxy.FileSystemMount{fs.Dir(dir), mnt.Prefix, nil, mnt.Addr, clean}, nil
	case "", ".":
		fpath := filepath.Join(mnt.Addr, mnt.Prefix)
		if mnt.Addr == "" {
			fpath = "/" + fpath
		}
		prefix := ""
		info, err := os.Stat(fpath)
		if err == nil {
			if !info.IsDir() {
				prefix = filepath.Base(fpath)
				fpath = filepath.Dir(fpath)
			}
		}

		return proxy.FileSystemMount{fs.Dir(fpath), prefix, nil, mnt.Addr, nil}, nil
	default:
		client, fs, err := c.CreateFs(mnt.Addr)
		if err != nil {
			return proxy.FileSystemMount{}, fmt.Errorf("Failed connecting to %s/%s: %w", mnt.Addr, mnt.Prefix, err)
		}
		return proxy.FileSystemMount{fs, mnt.Prefix, client, mnt.Addr, nil}, nil
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
		Mount:     c.Root,
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

func OnInterrupt(f func()) {
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	go func() {
		<-c
		f()
	}()
}

func MainClient(fn func(cfg *ClientConfig, m proxy.FileSystemMount) error) {
	var (
		cfg ClientConfig

		exitCode int

		err error
	)

	defer func() {
		os.Exit(exitCode)
	}()

	cfg.SetFlags(nil)
	args, _ := cfg.f.Parse()

	if len(args) == 0 {
		cfg.f.Usage()
		exitCode = 1
		runtime.Goexit()
	}

	mntCfg, ok := proxy.ParseMount(args[0])
	if !ok {
		fmt.Fprintf(os.Stderr, "Invalid path: %v\n", args[0])
		fmt.Fprintf(os.Stderr, "Format should be IP:PORT/PATH format.\n")
		exitCode = 1
		runtime.Goexit()
	}

	mnt, err := cfg.FSMount(&mntCfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error connecting to destination fs: %s\n", err)
		exitCode = 1
		runtime.Goexit()
	}
	defer mnt.Close()

	err = fn(&cfg, mnt)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed: %s\n", err)
		exitCode = 1
		runtime.Goexit()
	}
}
