package cli

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"log/slog"
	"os"
	"runtime"
	"runtime/pprof"
	"time"

	"github.com/jeffh/cfs/fs"
	"github.com/jeffh/cfs/ninep"
)

type ServerConfig struct {
	Addr string

	LogLevel string

	CertFile string
	KeyFile  string

	ReadTimeoutInSeconds          int
	MaxInflightRequestsPerSession int

	Dialer ninep.Dialer // defaults to net

	Stdout io.Writer
	Stderr io.Writer

	PrintHelp bool

	MemProfile string
	CpuProfile string

	fCpuProfile *os.File

	PrintPrefix string
	Logger      *slog.Logger
}

func (c *ServerConfig) SetFlags(f Flags) {
	if f == nil {
		f = &StdFlags{}
	}
	f.IntVar(&c.ReadTimeoutInSeconds, "rtimeout", 0, "Timeout in seconds for client requests")
	f.IntVar(&c.MaxInflightRequestsPerSession, "parallel", ninep.DefaultMaxInflightRequestsPerSession, "The number of parallel requests per client allowed. Increases per-client throughput at a server resource cost.")
	f.StringVar(&c.LogLevel, "srv-log", "error", "The log level for the server. Can be one of debug, info, warn, error.")
	f.BoolVar(&c.PrintHelp, "help", false, "Prints this help")
	f.StringVar(&c.CertFile, "certfile", "", "Accept only TLS wrapped connections. Also needs to specify keyfile flag.")
	f.StringVar(&c.KeyFile, "keyfile", "", "Accept only TLS wrapped connections. Also needs to specify certfile flag.")
	f.StringVar(&c.Addr, "addr", "localhost:564", "The address and port for the 9p server to listen to")
	f.StringVar(&c.MemProfile, "memprofile", "", "File to store memory profile")
	f.StringVar(&c.CpuProfile, "cpuprofile", "", "File to store cpu profile")
}

func (c *ServerConfig) CreateServer(createfs func(L *slog.Logger) ninep.FileSystem) *ninep.Server {
	if c.CpuProfile != "" {
		f, err := os.Create(c.CpuProfile)
		if err != nil {
			log.Fatal("could not create CPU profile: ", err)
		}
		if err := pprof.StartCPUProfile(f); err != nil {
			log.Fatal("could not start CPU profile: ", err)
		}
		c.fCpuProfile = f
	}

	if c.Stdout == nil {
		c.Stdout = os.Stdout
	}
	if c.Stderr == nil {
		c.Stderr = os.Stderr
	}

	c.Logger = ninep.CreateLogger(c.LogLevel, c.PrintPrefix, c.Logger)

	var fsys ninep.FileSystem = createfs(c.Logger)

	if c.Logger.Enabled(context.Background(), slog.LevelDebug) {
		fsys = fs.TraceFs(
			fsys,
			c.Logger,
		)
	}

	srv := ninep.NewServer(fsys, c.Logger)
	srv.ReadTimeout = time.Duration(c.ReadTimeoutInSeconds) * time.Second
	srv.MaxInflightRequestsPerSession = c.MaxInflightRequestsPerSession
	return srv
}

func (c *ServerConfig) Close() {
	if c.fCpuProfile != nil {
		pprof.StopCPUProfile()
		c.fCpuProfile.Close()
	}

	if c.MemProfile != "" {
		f, err := os.Create(c.MemProfile)
		if err != nil {
			log.Fatal("could not create memory profile: ", err)
		}
		defer f.Close() // error handling omitted for example
		runtime.GC()    // get up-to-date statistics
		if err := pprof.WriteHeapProfile(f); err != nil {
			log.Fatal("could not write memory profile: ", err)
		}
	}
}

func (c *ServerConfig) ListenAndServe(srv *ninep.Server) error {
	network, addr := ninep.ParseAddr(c.Addr)
	ln, err := srv.Listener(network, addr, c.Dialer)
	if err != nil {
		return err
	}
	fmt.Fprintf(c.Stderr, "Listening on %s\n", ln.Addr())
	if c.CertFile != "" && c.KeyFile != "" {
		return srv.ServeTLS(ln, c.CertFile, c.KeyFile)
	} else {
		return srv.Serve(ln)
	}
}

func (c *ServerConfig) CreateServerAndListen(createfs func(L *slog.Logger) ninep.FileSystem) error {
	srv := c.CreateServer(createfs)
	return c.ListenAndServe(srv)
}

// ServiceMainWithLogger starts a ninep.Server from a constructor of a ninep.FileSystem.
// The server takes responsibility for calling Close() on ninep.FileSystem on shutdown.
// Logger is the configured logger available from cli args
func ServiceMainWithLogger(createfs func(L *slog.Logger) ninep.FileSystem) {
	cfg := ServerConfig{
		Stdout: os.Stdout,
		Stderr: os.Stderr,
	}

	cfg.SetFlags(nil)

	flag.Parse()

	if cfg.PrintHelp {
		flag.Usage()
		os.Exit(1)
	}

	err := cfg.CreateServerAndListen(createfs)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %s\n", err)
		os.Exit(1)
	}
}

// ServiceMain starts a ninep.Server from a constructor of a ninep.FileSystem.
// The server takes responsibility for calling Close() on ninep.FileSystem on shutdown.
func ServiceMain(createfs func() ninep.FileSystem) {
	ServiceMainWithLogger(func(L *slog.Logger) ninep.FileSystem {
		return createfs()
	})
}

func ServiceMainWithFactory(createcfg func(stdout, stderr io.Writer) ServerConfig, createfs func() ninep.FileSystem) {
	conf := createcfg(os.Stdout, os.Stderr)
	srv := conf.CreateServer(func(*slog.Logger) ninep.FileSystem { return createfs() })
	err := conf.ListenAndServe(srv)
	if err != nil {
		fmt.Fprintf(conf.Stdout, "Error: %s\n", err)
	}
}
