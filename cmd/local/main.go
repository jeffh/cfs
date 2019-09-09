package main

import (
	"log"
	"os"

	"git.sr.ht/~jeffh/cfs/ninep"

	"fmt"
)

func main() {
	logger := log.New(os.Stdout, "", log.LstdFlags)
	srv := ninep.Server{
		Handler: &ninep.UnauthenticatedHandler{
			Fs:       ninep.Dir("."),
			ErrorLog: logger,
			TraceLog: logger,
			Qids:     ninep.NewQidPool(),
			Fids:     ninep.NewFidTracker(),
		},
		ErrorLog: logger,
		TraceLog: logger,
	}
	err := srv.ListenAndServe("")
	fmt.Printf("Error: %s", err)
}
