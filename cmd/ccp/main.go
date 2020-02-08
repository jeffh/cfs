package main

import (
	"flag"
	"fmt"
	"io"
	"os"
	"path"
	"runtime"

	"github.com/jeffh/cfs/cli"
	"github.com/jeffh/cfs/ninep"
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
		fmt.Fprintf(flag.CommandLine.Output(), "cp for CFS - Copy files and directories\n")
		fmt.Fprintf(flag.CommandLine.Output(), "Usage: %s [OPTIONS] SRC_HOST SRC_PATH DEST_HOST DEST_PATH \n", os.Args[0])
		fmt.Fprintf(flag.CommandLine.Output(), "\n")
		fmt.Fprintf(flag.CommandLine.Output(), "Where SRC_HOST & DEST_HOST are host addr or 'LOCAL' for local file system\n")
		flag.PrintDefaults()
	}

	cfg := cli.ClientConfig{}
	cfg.SetFlags(nil)

	flag.Parse()

	if flag.NArg() < 4 {
		flag.Usage()
		os.Exit(1)
	}

	defer func() { os.Exit(exitCode) }()

	srcAddr, srcPath := flag.Arg(0), flag.Arg(1)
	dstAddr, dstPath := flag.Arg(2), flag.Arg(3)

	cfg.PrintPrefix = "[src] "

	srcClt, srcFs, err := cfg.CreateFs(srcAddr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error connecting to source fs: %s\n", err)
		exitCode = 2
		runtime.Goexit()
	}
	defer srcClt.Close()

	cfg.PrintPrefix = "[dst] "
	dstClt, dstFs, err := cfg.CreateFs(dstAddr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error connecting to destination fs: %s\n", err)
		exitCode = 3
		runtime.Goexit()
	}
	defer dstClt.Close()

	srcNode, err := srcFs.Traverse(srcPath)
	{
		if os.IsNotExist(err) {
			fmt.Fprintf(os.Stderr, "Path does not exist on source fs: %s\n", srcPath)
			exitCode = 2
			runtime.Goexit()
		}
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error accessing path on source fs: %s\n", err)
			exitCode = 2
			runtime.Goexit()
		}

		if srcNode.IsDir() && !recursive {
			srcNode.Close()
			fmt.Fprintf(os.Stderr, "Use -r to copy directories\n")
			exitCode = 2
			runtime.Goexit()
		}
	}
	defer srcNode.Close()

	dstNode, err := dstFs.Traverse(dstPath)
	dstIsParent := false
	{
		if os.IsNotExist(err) {
			dstNode, err = dstFs.Traverse(ninep.Dirname(dstPath))
			dstIsParent = true
		}
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error accessing path on destination fs: %s: %s\n", dstPath, err)
			exitCode = 3
			runtime.Goexit()
		}
	}
	defer dstNode.Close()

	// technically '-r' is not very plan9 like, but we don't have the luxury of being the host os' file system
	// plan9 systems would normally do `(cd DIR && tar c .) | (cd DIR && tar x)` aliased as `dircp`.
	if srcNode.IsDir() {
		fmt.Fprintf(os.Stderr, "Unsupported: directory copy\n")
		exitCode = 4
		runtime.Goexit()

		if dstIsParent {
			dstNode, err = dstNode.CreateDir(ninep.Basename(dstPath), 0755)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Error creating dir on destination fs: %s: %s\n", dstPath, err)
				exitCode = 3
				runtime.Goexit()
			}
		}

		if !dstNode.IsDir() {
			fmt.Fprintf(os.Stderr, "Error opening dir on destination fs: %s: %s\n", dstPath, err)
			exitCode = 3
			runtime.Goexit()
		}

		it, err := srcDir.ListDirStat()
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error opening dir on source fs: %s: %s\n", srcPath, err)
			exitCode = 2
			runtime.Goexit()
		}
		defer it.Close()

		type stackNode struct {
			it  StatIterator
			src *FileProxy
			dst *FileProxy
		}

		stack := make([]stackNode)
		stack = append(stack, stackNode{it, srcNode, dstNode})

		for len(stack) > 0 {
			last := stack[len(stack)-1]
			fi, err := it.NextFileInfo()
			name := fi.Name()
			src, er := last.src.Traverse(name)
			if er != nil {
				fmt.Fprintf(os.Stderr, "Error read path on source fs: %s: %s\n", path.Join(srcPath, name), er)
				continue
			}

			if src.IsDir() {
			} else {
				dst, err := last.dst.Traverse(name)
				if os.IsNotExist(err) {
					dst, err := last.dst.Traverse(ninep.Dirname(name))
				}
			}

			if err == io.EOF {
				stack = stack[:len(stack)-1]
			}
		}

	} else {
		if dstIsParent {
			dstNode, err = dstNode.Create(ninep.Basename(dstPath), ninep.OWRITE|ninep.OTRUNC, 0644)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Error creating file on destination fs: %s: %s\n", dstPath, err)
				exitCode = 3
				runtime.Goexit()
			}
		} else {
			if err = dstNode.Open(ninep.OWRITE | ninep.OTRUNC); err != nil {
				fmt.Fprintf(os.Stderr, "Error opening file on destination fs: %s: %s\n", dstPath, err)
				exitCode = 3
				runtime.Goexit()
			}
		}

		if err = srcNode.Open(ninep.OREAD); err != nil {
			fmt.Fprintf(os.Stderr, "Error opening file on source fs: %s: %s\n", srcPath, err)
			exitCode = 2
			runtime.Goexit()
		}

		srcSt, err := srcNode.Stat()
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error stat-ing file on source fs: %s: %s\n", srcPath, err)
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
				fmt.Fprintf(os.Stderr, "Failed to delete file since copying failed: %s%s\n", dstAddr, dstPath)
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
				fmt.Fprintf(os.Stderr, "Failed to copy file attributes: %s%s\n", dstAddr, dstPath)
				exitCode = 3
				runtime.Goexit()
			}
		}
	}
}
