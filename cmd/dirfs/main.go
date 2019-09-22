package main

import (
	"flag"
	"log"
	"os"

	"git.sr.ht/~jeffh/cfs/ninep"

	"fmt"
)

func main() {
	var root string
	var addr string
	var trace bool
	var errLog bool

	flag.StringVar(&root, "root", ".", "The root directory to serve files from. Defaults the current working directory.")
	flag.StringVar(&addr, "addr", "localhost:564", "The address and port to listen the 9p server. Defaults to 'localhost:564'.")
	flag.BoolVar(&trace, "trace", false, "Print trace of 9p server to stdout")
	flag.BoolVar(&errLog, "err", false, "Print errors of 9p server to stderr")

	flag.Parse()

	fmt.Printf("Serving: %v\n", root)

	var traceLogger, errLogger ninep.Logger

	if trace {
		traceLogger = log.New(os.Stdout, "", log.LstdFlags)
	}
	if errLog {
		errLogger = log.New(os.Stderr, "", log.LstdFlags)
	}

	srv := ninep.Server{
		Handler: &ninep.UnauthenticatedHandler{
			Fs: ninep.Dir(root),
			Loggable: ninep.Loggable{
				ErrorLog: errLogger,
				TraceLog: traceLogger,
			},
		},
		ErrorLog: errLogger,
		TraceLog: traceLogger,
	}
	err := srv.ListenAndServe(addr)
	fmt.Printf("Error: %s", err)
}
