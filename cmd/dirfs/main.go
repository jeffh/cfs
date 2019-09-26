package main

import (
	"flag"
	"log"
	"os"

	"git.sr.ht/~jeffh/cfs/ninep"

	"fmt"
)

func main() {
	var (
		root    string
		addr    string
		trace   bool
		errLog  bool
		tracefs bool

		certFile string
		keyFile  string

		err error
	)

	flag.StringVar(&root, "root", ".", "The root directory to serve files from. Defaults the current working directory.")
	flag.StringVar(&addr, "addr", "localhost:564", "The address and port to listen the 9p server. Defaults to 'localhost:564'.")
	flag.BoolVar(&trace, "trace", false, "Print trace of 9p server to stdout")
	flag.BoolVar(&tracefs, "tracefs", false, "Print trace of 9p FileSystem interface to stdout")
	flag.BoolVar(&errLog, "err", false, "Print errors of 9p server to stderr")
	flag.StringVar(&certFile, "certfile", "", "Accept only TLS wrapped connections. Also needs to specify keyfile flag.")
	flag.StringVar(&keyFile, "keyfile", "", "Accept only TLS wrapped connections. Also needs to specify certfile flag.")

	flag.Parse()

	fmt.Printf("Serving: %v\n", root)

	var traceLogger, errLogger ninep.Logger

	if trace {
		traceLogger = log.New(os.Stdout, "", log.LstdFlags)
	}
	if errLog {
		errLogger = log.New(os.Stderr, "", log.LstdFlags)
	}

	var fs ninep.FileSystem = ninep.Dir(root)

	if tracefs {
		fs = ninep.TraceFileSystem{
			Fs:       fs,
			Loggable: ninep.Loggable{log.New(os.Stdout, "[err] ", log.LstdFlags), log.New(os.Stdout, "[trace] ", log.LstdFlags)},
		}
	}

	srv := ninep.NewServer(fs, errLogger, traceLogger)
	if certFile != "" && keyFile != "" {
		err = srv.ListenAndServeTLS(addr, certFile, keyFile)
	} else {
		err = srv.ListenAndServe(addr)
	}
	fmt.Printf("Error: %s", err)
}
