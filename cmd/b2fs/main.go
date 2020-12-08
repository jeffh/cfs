package main

import (
	"flag"
	"os"

	"github.com/jeffh/cfs/cli"
	"github.com/jeffh/cfs/fs/b2fs"
	"github.com/jeffh/cfs/ninep"
	"github.com/kardianos/service"
)

func main() {
	cfg := &service.Config{
		Name:        "b2fs",
		DisplayName: "B2 File System Service",
		Description: "Provides a 9p file system that connects to Backblaze's B2 service",
	}

	var accountID, accountKey string

	flag.StringVar(&accountID, "b2-accountid", "", "The B2 Account ID to use. Defaults to reading from B2_ACCOUNT_ID env var.")
	flag.StringVar(&accountKey, "b2-accountkey", "", "The B2 Account Key to use. Defaults to reading from B2_ACCOUNT_KEY env var.")

	if accountID == "" {
		accountID = os.Getenv("B2_ACCOUNT_ID")
	}

	if accountKey == "" {
		accountKey = os.Getenv("B2_ACCOUNT_KEY")
	}

	cli.ServiceMain(cfg, func() ninep.FileSystem {
		fs := b2fs.New(accountID, accountKey)
		return fs
	})
}
