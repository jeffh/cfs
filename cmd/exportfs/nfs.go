package main

import (
	"context"
	"fmt"
	iofs "io/fs"
	"net"

	"github.com/jeffh/cfs/fs/proxy"
	"github.com/smallfz/libnfs-go/auth"
	"github.com/smallfz/libnfs-go/backend"
	"github.com/smallfz/libnfs-go/fs"
	"github.com/smallfz/libnfs-go/log"
	"github.com/smallfz/libnfs-go/memfs"
	"github.com/smallfz/libnfs-go/server"
)

func runServer(ctx context.Context, listen string, mnt proxy.FileSystemMount, mountpoint string) error {
	c, err := net.Listen("tcp", listen)
	if err != nil {
		return err
	}
	defer c.Close()

	mfs := memfs.NewMemFS()

	// We don't need to create a new fs for each connection as memfs is opaque towards SetCreds.
	// If the file system would depend on SetCreds, make sure to generate a new fs.FS for each connection.
	backend := backend.New(func() fs.FS { return mfs }, auth.Null)

	mfs.MkdirAll("/mount", iofs.FileMode(0o755))
	mfs.MkdirAll("/test", iofs.FileMode(0o755))
	mfs.MkdirAll("/test2", iofs.FileMode(0o755))
	mfs.MkdirAll("/many", iofs.FileMode(0o755))

	perm := iofs.FileMode(0o755)
	for i := 0; i < 256; i++ {
		mfs.MkdirAll(fmt.Sprintf("/many/sub-%d", i+1), perm)
	}

	svr, err := server.NewServer(c, backend)
	if err != nil {
		log.Errorf("server.NewServerTCP: %v", err)
		return err
	}

	go svr.Serve()
	// exec.Command("mount", "-t", "nfs", "-o", "vers=4.0,noacl,tcp", listen+":/", mountpoint).Run()
	<-ctx.Done()
	c.Close()
	return nil
}
