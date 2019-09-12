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

	flag.StringVar(&root, "root", ".", "The root directory to serve files from")

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
	err := srv.ListenAndServe("")
	fmt.Printf("Error: %s", err)
}
