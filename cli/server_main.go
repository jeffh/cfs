package cli

import (
	"context"
	"flag"
	"io"
	"log"
	"os"
	"time"

	"github.com/jeffh/cfs/fs"
	"github.com/jeffh/cfs/ninep"
	"github.com/kardianos/service"
)

func BasicServerMain(createfs func() ninep.FileSystem) {
	var (
		addr    string
		trace   bool
		errLog  bool
		tracefs bool

		certFile string
		keyFile  string

		readTimeout int
	)

	flag.StringVar(&addr, "addr", "localhost:564", "The address and port to listen the 9p server. Defaults to 'localhost:564'.")
	flag.BoolVar(&trace, "trace", false, "Print trace of 9p server to stdout")
	flag.BoolVar(&tracefs, "tracefs", false, "Print trace of 9p FileSystem interface to stdout")
	flag.BoolVar(&errLog, "err", false, "Print errors of 9p server to stderr")
	flag.StringVar(&certFile, "certfile", "", "Accept only TLS wrapped connections. Also needs to specify keyfile flag.")
	flag.StringVar(&keyFile, "keyfile", "", "Accept only TLS wrapped connections. Also needs to specify certfile flag.")
	flag.IntVar(&readTimeout, "timeout", 0, "Timeout for reading from client connections in seconds. Defaults to 30 minutes")

	flag.Parse()

	var traceLogger, errLogger ninep.Logger

	if trace {
		traceLogger = log.New(os.Stdout, "", log.LstdFlags)
	}
	if errLog {
		errLogger = log.New(os.Stderr, "", log.LstdFlags)
	}

	var fsys ninep.FileSystem = createfs()

	if tracefs {
		fsys = fs.TraceFs(
			fsys,
			ninep.Loggable{log.New(os.Stdout, "[err] ", log.LstdFlags), log.New(os.Stdout, "[trace] ", log.LstdFlags)},
		)
	}

	srv := ninep.NewServer(fsys, errLogger, traceLogger)
	srv.ReadTimeout = time.Duration(readTimeout) * time.Second
	var d ninep.Dialer
	var err error
	if certFile != "" && keyFile != "" {
		err = srv.ListenAndServeTLS(addr, certFile, keyFile, d)
	} else {
		err = srv.ListenAndServe(addr, d)
	}
	if errLogger != nil {
		errLogger.Printf("Error: %s", err)
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
	var (
		addr    string
		trace   bool
		errLog  bool
		tracefs bool

		certFile string
		keyFile  string

		readTimeout int
	)

	flag.StringVar(&addr, "addr", "localhost:564", "The address and port to listen the 9p server. Defaults to 'localhost:564'.")
	flag.BoolVar(&trace, "trace", false, "Print trace of 9p server to stdout")
	flag.BoolVar(&tracefs, "tracefs", false, "Print trace of 9p FileSystem interface to stdout")
	flag.BoolVar(&errLog, "err", false, "Print errors of 9p server to stderr")
	flag.StringVar(&certFile, "certfile", "", "Accept only TLS wrapped connections. Also needs to specify keyfile flag.")
	flag.StringVar(&keyFile, "keyfile", "", "Accept only TLS wrapped connections. Also needs to specify certfile flag.")
	flag.IntVar(&readTimeout, "timeout", 0, "Timeout for reading from client connections in seconds. Defaults to 30 minutes")

	flag.Parse()

	var traceLogger, errLogger ninep.Logger

	if trace {
		traceLogger = log.New(stdout, "", log.LstdFlags)
	}
	if errLog {
		errLogger = log.New(stderr, "", log.LstdFlags)
	}

	var fsys ninep.FileSystem = createfs()

	if tracefs {
		fsys = fs.TraceFs(
			fsys,
			ninep.Loggable{log.New(os.Stdout, "[err] ", log.LstdFlags), log.New(os.Stdout, "[trace] ", log.LstdFlags)},
		)
	}

	srv := ninep.NewServer(fsys, errLogger, traceLogger)
	srv.ReadTimeout = time.Duration(readTimeout) * time.Second
	return func(exit chan struct{}) {
		var d ninep.Dialer
		ctx, cancel := context.WithCancel(context.Background())
		go func() {
			<-exit
			cancel()
		}()
		go func() {
			var err error
			defer cancel()
			if certFile != "" && keyFile != "" {
				err = srv.ListenAndServeTLS(addr, certFile, keyFile, d)
			} else {
				err = srv.ListenAndServe(addr, d)
			}
			if errLogger != nil {
				errLogger.Printf("Error: %s", err)
			}
		}()
		<-ctx.Done()
	}
}
