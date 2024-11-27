package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"iter"
	"os"
	"path"
	"runtime"

	"github.com/jeffh/cfs/cli"
	"github.com/jeffh/cfs/fs/proxy"
	"github.com/jeffh/cfs/ninep"
	_ "go.uber.org/automaxprocs"
)

func main() {
	var (
		recursive bool
		noAttrs   bool // disable copying stat
		allAttrs  bool // also copy timestamps, if noAttrs is false

		exitCode int
	)

	flag.BoolVar(&recursive, "r", false, "Recursively copy directories")
	flag.BoolVar(&noAttrs, "n", false, "Don't copy file mode attributes")
	flag.BoolVar(&allAttrs, "a", false, "copy all file timestamps")

	flag.Usage = func() {
		w := flag.CommandLine.Output()
		fmt.Fprintf(w, "Usage: %s [OPTIONS] SRC_HOST/SRC_PATH DEST_HOST/DEST_PATH\n\n", os.Args[0])
		fmt.Fprintf(w, "cp for CFS - Copy files and directories\n\n")
		fmt.Fprintf(w, "OPTIONS:\n")
		flag.PrintDefaults()
	}

	ctx := context.Background()

	cfg := cli.ClientConfig{}
	cfg.SetFlags(nil)

	flag.Parse()

	if flag.NArg() < 2 {
		flag.Usage()
		os.Exit(1)
	}

	defer func() { os.Exit(exitCode) }()

	srcMntCfg, ok := proxy.ParseMount(flag.Arg(0))
	if !ok {
		fmt.Fprintf(os.Stderr, "Invalid source path: %v\n", flag.Arg(0))
		exitCode = 2
		runtime.Goexit()
	}
	dstMntCfg, ok := proxy.ParseMount(flag.Arg(1))
	if !ok {
		fmt.Fprintf(os.Stderr, "Invalid destination path: %v\n", flag.Arg(1))
		exitCode = 2
		runtime.Goexit()
	}

	cfg.PrintPrefix = "[src] "

	srcMnt, err := cfg.FSMount(&srcMntCfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error connecting to source fs: %s\n", err)
		exitCode = 2
		runtime.Goexit()
	}
	defer srcMnt.Close()

	cfg.PrintPrefix = "[dst] "
	dstMnt, err := cfg.FSMount(&dstMntCfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error connecting to destination fs: %s\n", err)
		exitCode = 3
		runtime.Goexit()
	}
	defer dstMnt.Close()

	srcNode, err := srcMnt.FS.Traverse(ctx, srcMnt.Prefix)
	{

		if errors.Is(err, fs.ErrNotExist) {
			fmt.Fprintf(os.Stderr, "Path does not exist on source fs: %s %s\n", srcMntCfg.Addr, srcMnt.Prefix)
			exitCode = 2
			runtime.Goexit()
		}
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error accessing path on source fs: %s\n", err)
			exitCode = 2
			runtime.Goexit()
		}

		if srcNode.Type().IsDir() && !recursive {
			srcNode.Close()
			fmt.Fprintf(os.Stderr, "Use -r to copy directories\n")
			exitCode = 2
			runtime.Goexit()
		}
	}
	defer srcNode.Close()

	dstNode, err := dstMnt.FS.Traverse(ctx, dstMnt.Prefix)
	dstIsParent := false
	{
		path := dstMnt.Prefix
		if errors.Is(err, fs.ErrNotExist) {
			path = ninep.Dirname(path)
			dstNode, err = dstMnt.FS.Traverse(ctx, path)
			dstIsParent = true
		}
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error accessing path on destination fs: %s: %s\n", path, err)
			exitCode = 3
			runtime.Goexit()
		}
	}
	defer dstNode.Close()

	// technically '-r' is not very plan9 like, but we don't have the luxury of being the host os' file system
	// plan9 systems would normally do `(cd DIR && tar c .) | (cd DIR && tar x)` aliased as `dircp`.
	if srcNode.Type().IsDir() {
		fmt.Fprintf(os.Stderr, "Unsupported: directory copy\n")
		exitCode = 4
		runtime.Goexit()

		if dstIsParent {
			dstNode, err = dstNode.MakeDir(ninep.Basename(dstMnt.Prefix), 0755)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Error creating dir on destination fs: %s: %s\n", dstMnt.Prefix, err)
				exitCode = 3
				runtime.Goexit()
			}
		}

		if !dstNode.Type().IsDir() {
			fmt.Fprintf(os.Stderr, "Error opening dir on destination fs: %s: %s\n", dstMnt.Prefix, err)
			exitCode = 3
			runtime.Goexit()
		}

		next, stop := iter.Pull2(srcNode.ListDir())
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error opening dir on source fs: %s/%s: %s\n", srcMntCfg.Addr, srcMnt.Prefix, err)
			exitCode = 2
			runtime.Goexit()
		}
		defer stop()

		type stackNode struct {
			next func() (fs.FileInfo, error, bool)
			src  ninep.TraversableFile
			dst  ninep.TraversableFile
		}

		stack := []stackNode{
			stackNode{next, srcNode, dstNode},
		}

		for len(stack) > 0 {
			last := stack[len(stack)-1]
			fi, err, more := next()
			if err != nil {
				fmt.Fprintf(os.Stderr, "Error read path at prefix: %s: %s\n", srcMnt.Prefix, err)
				continue
			}
			name := fi.Name()
			src, er := last.src.Traverse(ctx, name)
			if er != nil {
				fmt.Fprintf(os.Stderr, "Error read path on source fs: %s: %s\n", path.Join(srcMnt.Prefix, name), er)
				continue
			}

			if src.Type().IsDir() {
			} else {
				// dst, err := last.dst.Traverse(name)
				// if errors.Is(err, fs.ErrNotExist) {
				// 	dst, err := last.dst.Traverse(ninep.Dirname(name))
				// }
			}

			if !more {
				stack = stack[:len(stack)-1]
			}
		}

	} else {
		if dstIsParent {
			dstNode, err = dstNode.Create(ninep.Basename(dstMnt.Prefix), ninep.OWRITE|ninep.OTRUNC, 0644)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Error creating file on destination fs: %s: %s\n", dstMnt.Prefix, err)
				exitCode = 3
				runtime.Goexit()
			}
		} else {
			if err = dstNode.Open(ninep.OWRITE | ninep.OTRUNC); err != nil {
				fmt.Fprintf(os.Stderr, "Error opening file on destination fs: %s: %s\n", dstMnt.Prefix, err)
				exitCode = 3
				runtime.Goexit()
			}
		}

		if err = srcNode.Open(ninep.OREAD); err != nil {
			fmt.Fprintf(os.Stderr, "Error opening file on source fs: %s: %s\n", srcMnt.Prefix, err)
			exitCode = 2
			runtime.Goexit()
		}

		srcSt, err := srcNode.Stat()
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error stat-ing file on source fs: %s: %s\n", srcMnt.Prefix, err)
			exitCode = 2
			runtime.Goexit()
		}

		w := ninep.Writer(dstNode)
		r := ninep.Reader(srcNode)
		_, err = io.Copy(w, r)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error while copying: %s", err)
			if dstIsParent {
				err = dstNode.Delete()
				fmt.Fprintf(os.Stderr, "Failed to delete file since copying failed: %s/%s\n", dstMntCfg.Addr, dstMnt.Prefix)
			}

			exitCode = 4
			runtime.Goexit()
		}

		if !noAttrs || allAttrs {
			st := ninep.SyncStat()
			if !noAttrs {
				st.SetMode(srcSt.Mode())
			}
			if allAttrs {
				st.SetAtime(srcSt.Atime())
				st.SetMtime(srcSt.Mtime())
			}
			err = dstNode.WriteStat(st)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Failed to copy file attributes: %s/%s\n", dstMntCfg.Addr, dstMnt.Prefix)
				exitCode = 3
				runtime.Goexit()
			}
		}
	}
}
