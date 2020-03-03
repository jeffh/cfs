package cli

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
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

	ReadTimeoutInSeconds int

	Dialer ninep.Dialer // defaults to net

	Stdout io.Writer
	Stderr io.Writer
}

func (c *ServerConfig) SetFlags(f Flags) {
	if f == nil {
		f = &StdFlags{}
	}
	f.IntVar(&c.ReadTimeoutInSeconds, "rtimeout", 5, "Timeout in seconds for client requests")
	f.BoolVar(&c.PrintTraceMessages, "srv-trace", false, "Print trace of 9p server to stdout")
	f.BoolVar(&c.PrintTraceFSMessages, "srv-tracefs", false, "Print trace of 9p server to stdout")
	f.BoolVar(&c.PrintErrorMessages, "srv-err", false, "Print errors of 9p server to stderr")
	f.StringVar(&c.CertFile, "certfile", "", "Accept only TLS wrapped connections. Also needs to specify keyfile flag.")
	f.StringVar(&c.KeyFile, "keyfile", "", "Accept only TLS wrapped connections. Also needs to specify certfile flag.")
	f.StringVar(&c.Addr, "addr", "localhost:6666", "The address and port for the 9p server to listen to")
}

func (c *ServerConfig) CreateServer(createfs func() ninep.FileSystem) *ninep.Server {
	var traceLogger, errLogger ninep.Logger

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
	return srv
}

func (c *ServerConfig) ListenAndServe(srv *ninep.Server) error {
	var d ninep.Dialer = c.Dialer
	var err error
	if c.CertFile != "" && c.KeyFile != "" {
		err = srv.ListenAndServeTLS(c.Addr, c.CertFile, c.KeyFile, d)
	} else {
		err = srv.ListenAndServe(c.Addr, d)
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

	err := cfg.CreateServerAndListen(createfs)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %s\n", err)
	}
}

func ServiceMain(cfg *service.Config, createfs func() ninep.FileSystem) {
	prg := &srv{}
	s, err := service.New(prg, cfg)
	if err != nil {
		log.Fatal(err)
	}
	prg.logger, err = s.Logger(nil)
	if err != nil {
		log.Fatal(err)
	}

	prg.Main = srvMain(
		&proxyInfoLogger{prg.logger},
		&proxyErrorLogger{prg.logger},
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
	Main   func(exit chan struct{})
	logger service.Logger
	exit   chan struct{}
}

func (p *srv) Start(s service.Service) error {
	p.exit = make(chan struct{})
	p.logger.Infof("Starting")
	go p.run()
	return nil
}
func (p *srv) Stop(s service.Service) error {
	p.logger.Infof("Stopping")
	close(p.exit)
	return nil
}
func (p *srv) run() error {
	p.Main(p.exit)
	return nil
}

func srvMain(stdout, stderr io.Writer, createfs func() ninep.FileSystem) (start func(exit chan struct{})) {
	cfg := ServerConfig{
		Stdout: stdout,
		Stderr: stderr,
	}

	cfg.SetFlags(nil)

	flag.Parse()

	srv := cfg.CreateServer(createfs)
	return func(exit chan struct{}) {
		ctx, cancel := context.WithCancel(context.Background())
		go func() {
			<-exit
			cancel()
		}()
		go func() {
			defer cancel()
			err := cfg.ListenAndServe(srv)
			if err != nil {
				fmt.Fprintf(cfg.Stdout, "Error: %s\n", err)
			}
		}()
		<-ctx.Done()
	}
}
