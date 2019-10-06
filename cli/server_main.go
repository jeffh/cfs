package cli

import (
	"flag"
	"log"
	"os"

	"github.com/jeffh/cfs/fs"
	"github.com/jeffh/cfs/ninep"

	"fmt"
)

func BasicServerMain(createfs func() ninep.FileSystem) {
	var (
		addr    string
		trace   bool
		errLog  bool
		tracefs bool

		certFile string
		keyFile  string

		err error
	)

	flag.StringVar(&addr, "addr", "localhost:564", "The address and port to listen the 9p server. Defaults to 'localhost:564'.")
	flag.BoolVar(&trace, "trace", false, "Print trace of 9p server to stdout")
	flag.BoolVar(&tracefs, "tracefs", false, "Print trace of 9p FileSystem interface to stdout")
	flag.BoolVar(&errLog, "err", false, "Print errors of 9p server to stderr")
	flag.StringVar(&certFile, "certfile", "", "Accept only TLS wrapped connections. Also needs to specify keyfile flag.")
	flag.StringVar(&keyFile, "keyfile", "", "Accept only TLS wrapped connections. Also needs to specify certfile flag.")

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
		fsys = fs.TraceFileSystem{
			Fs:       fsys,
			Loggable: ninep.Loggable{log.New(os.Stdout, "[err] ", log.LstdFlags), log.New(os.Stdout, "[trace] ", log.LstdFlags)},
		}
	}

	srv := ninep.NewServer(fsys, errLogger, traceLogger)
	if certFile != "" && keyFile != "" {
		err = srv.ListenAndServeTLS(addr, certFile, keyFile)
	} else {
		err = srv.ListenAndServe(addr)
	}
	fmt.Printf("Error: %s", err)
}
