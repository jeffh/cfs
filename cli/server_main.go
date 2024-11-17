package cli

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"runtime"
	"runtime/pprof"
	"time"

	"github.com/jeffh/cfs/fs"
	"github.com/jeffh/cfs/ninep"
	"github.com/kardianos/service"
)

type ServerConfig struct {
	Addr string

	PrintTraceMessages   bool
	PrintTraceFSMessages bool
	PrintErrorMessages   bool

	PrintPrefix string

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
}

func (c *ServerConfig) SetFlags(f Flags) {
	if f == nil {
		f = &StdFlags{}
	}
	f.IntVar(&c.ReadTimeoutInSeconds, "rtimeout", 0, "Timeout in seconds for client requests")
	f.IntVar(&c.MaxInflightRequestsPerSession, "parallel", ninep.DefaultMaxInflightRequestsPerSession, "The number of parallel requests per client allowed. Increases per-client throughput at a server resource cost.")
	f.BoolVar(&c.PrintTraceMessages, "srv-trace", false, "Print trace of 9p server to stdout")
	f.BoolVar(&c.PrintTraceFSMessages, "srv-tracefs", false, "Print trace of 9p server to stdout")
	f.BoolVar(&c.PrintErrorMessages, "srv-err", false, "Print errors of 9p server to stderr")
	f.BoolVar(&c.PrintHelp, "help", false, "Prints this help")
	f.StringVar(&c.CertFile, "certfile", "", "Accept only TLS wrapped connections. Also needs to specify keyfile flag.")
	f.StringVar(&c.KeyFile, "keyfile", "", "Accept only TLS wrapped connections. Also needs to specify certfile flag.")
	f.StringVar(&c.Addr, "addr", "localhost:564", "The address and port for the 9p server to listen to")
	f.StringVar(&c.MemProfile, "memprofile", "", "File to store memory profile")
	f.StringVar(&c.CpuProfile, "cpuprofile", "", "File to store cpu profile")
}

func (c *ServerConfig) CreateServer(createfs func() ninep.FileSystem) *ninep.Server {
	var traceLogger, errLogger ninep.Logger

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

	if c.PrintTraceMessages {
		traceLogger = log.New(c.Stdout, c.PrintPrefix, log.LstdFlags)
	}
	if c.PrintErrorMessages {
		errLogger = log.New(c.Stderr, c.PrintPrefix, log.LstdFlags)
	}

	var fsys ninep.FileSystem = createfs()

	if c.PrintTraceFSMessages {
		fsys = fs.TraceFs(
			fsys,
			ninep.Loggable{log.New(os.Stdout, "[err] ", log.LstdFlags), log.New(os.Stdout, "[trace] ", log.LstdFlags)},
		)
	}

	srv := ninep.NewServer(fsys, errLogger, traceLogger)
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
	var d ninep.Dialer = c.Dialer
	var err error
	if c.CertFile != "" && c.KeyFile != "" {
		err = srv.ListenAndServeTLS(network, addr, c.CertFile, c.KeyFile, d)
	} else {
		err = srv.ListenAndServe(network, addr, d)
	}
	return err
}

func (c *ServerConfig) CreateServerAndListen(createfs func() ninep.FileSystem) error {
	srv := c.CreateServer(createfs)
	return c.ListenAndServe(srv)
}

func BasicServerMain(createfs func() ninep.FileSystem) {
	var cfg ServerConfig

	cfg.SetFlags(nil)

	flag.Parse()

	if cfg.PrintHelp {
		flag.Usage()
		os.Exit(1)
	}

	err := cfg.CreateServerAndListen(createfs)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %s\n", err)
	}
}

func ServiceMain(cfg *service.Config, createfs func() ninep.FileSystem) {
	createcfg := func(stdout, stderr io.Writer) ServerConfig {
		cfg := ServerConfig{
			Stdout: stdout,
			Stderr: stderr,
		}

		cfg.SetFlags(nil)
		flag.Parse()

		if cfg.PrintHelp {
			flag.Usage()
			os.Exit(1)
		}

		return cfg
	}

	ServiceMainWithFactory(cfg, createcfg, createfs)
}

func ServiceMainWithFactory(cfg *service.Config, createcfg func(stdout, stderr io.Writer) ServerConfig, createfs func() ninep.FileSystem) {
	prg := &srv{}
	s, err := service.New(prg, cfg)
	if err != nil {
		log.Fatal(err)
	}
	prg.logger, err = s.Logger(nil)
	if err != nil {
		log.Fatal(err)
	}

	prg.Main = srvMainWithCfg(
		&proxyInfoLogger{prg.logger},
		&proxyErrorLogger{prg.logger},
		createcfg,
		createfs,
	)

	if prg.Main == nil {
		log.Fatal("Failed to service")
	}

	err = s.Run()
	if err != nil {
		prg.logger.Error(err)
	}
}

type proxyInfoLogger struct{ logger service.Logger }

func (l *proxyInfoLogger) Write(p []byte) (int, error) {
	err := l.logger.Info(string(p))
	return len(p), err
}

type proxyErrorLogger struct{ logger service.Logger }

func (l *proxyErrorLogger) Write(p []byte) (int, error) {
	err := l.logger.Error(string(p))
	return len(p), err
}

type srv struct {
	Main       func(context.Context)
	logger     service.Logger
	runningCtx context.Context
	cancel     context.CancelFunc
}

func (p *srv) Start(s service.Service) error {
	p.runningCtx, p.cancel = context.WithCancel(context.Background())
	p.logger.Infof("Starting")
	go p.run()
	return nil
}
func (p *srv) Stop(s service.Service) error {
	p.logger.Infof("Stopping")
	p.cancel()
	time.Sleep(1 * time.Second)
	return nil
}
func (p *srv) run() error {
	p.Main(p.runningCtx)
	<-p.runningCtx.Done()
	return nil
}

func srvMainWithCfg(stdout, stderr io.Writer, mkCfg func(stdout, stderr io.Writer) ServerConfig, createfs func() ninep.FileSystem) (start func(ctx context.Context)) {
	cfg := mkCfg(stdout, stderr)

	srv := cfg.CreateServer(createfs)
	return func(parentCtx context.Context) {
		ctx, cancel := context.WithCancel(parentCtx)
		go func() {
			defer cancel()
			err := cfg.ListenAndServe(srv)
			if err != nil {
				fmt.Fprintf(cfg.Stdout, "Error: %s\n", err)
			}
		}()
		<-ctx.Done()
		cfg.Close()
	}
}

func srvMain(stdout, stderr io.Writer, createfs func() ninep.FileSystem) (start func(ctx context.Context)) {
	return srvMainWithCfg(stdout, stderr, func(stdout, stderr io.Writer) ServerConfig {
		cfg := ServerConfig{
			Stdout: stdout,
			Stderr: stderr,
		}

		cfg.SetFlags(nil)
		flag.Parse()

		if cfg.PrintHelp {
			flag.Usage()
			os.Exit(1)
		}
		return cfg
	}, createfs)
}
