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

	flag.StringVar(&root, "root", ".", "The root directory to serve files from. Defaults the current working directory.")
	flag.StringVar(&addr, "addr", "localhost:564", "The address and port to listen the 9p server. Defaults to 'localhost:564'.")

	flag.Parse()

	fmt.Printf("Serving: %v\n", root)

	logger := log.New(os.Stdout, "", log.LstdFlags)
	srv := ninep.Server{
		Handler: &ninep.UnauthenticatedHandler{
			Fs:       ninep.Dir(root),
			ErrorLog: logger,
			TraceLog: logger,
		},
		ErrorLog: logger,
		TraceLog: logger,
	}
	err := srv.ListenAndServe(addr)
	fmt.Printf("Error: %s", err)
}
