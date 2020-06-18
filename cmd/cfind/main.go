package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/jeffh/cfs/cli"
	"github.com/jeffh/cfs/fs/proxy"
	"github.com/jeffh/cfs/ninep"
)

var expr *regexp.Regexp
var kindPredicate func(os.FileInfo) bool
var printOut chan string

func isFile(info os.FileInfo) bool { return !info.IsDir() }
func isDir(info os.FileInfo) bool  { return info.IsDir() }
func isAny(info os.FileInfo) bool  { return true }

func traverse(ctx context.Context, fs ninep.TraversableFileSystem, path string) chan error {
	result := make(chan error, 1)
	go func() {
		infos, err := fs.ListDir(context.Background(), path)
		if err != nil {
			result <- err
			return
		}
		defer infos.Close()

		for {
			info, err := infos.NextFileInfo()
			if info != nil {
				name := info.Name()
				fullpath := filepath.Join(path, name)
				if !strings.HasPrefix(name, ".") {
					if (expr == nil || expr.MatchString(fullpath)) && kindPredicate(info) {
						printOut <- fullpath
					}
					if info.IsDir() {
						if err := <-traverse(ctx, fs, fullpath); err != nil {
							result <- err
							return
						}
					}
				}
			}
			if err == io.EOF {
				break
			} else if err != nil {
				result <- err
				return
			}
		}
		close(result)
	}()
	return result
}

func main() {
	var exitCode int
	var posixRE bool
	var kind string
	var includePrefix bool
	var includeHost bool

	flag.BoolVar(&posixRE, "e", false, "Use POSIX Egrep syntax for expressions")
	flag.BoolVar(&includePrefix, "p", false, "Include mount prefix when printing files")
	flag.BoolVar(&includeHost, "h", false, "Include mount host when printing files")
	flag.StringVar(&kind, "type", "a", "Filter files by type ('d' = directory, 'f' = non-directory, 'a' = anything)")

	flag.Usage = func() {
		fmt.Fprintf(flag.CommandLine.Output(), "find for CFS\n")
		fmt.Fprintf(flag.CommandLine.Output(), "Usage: %s [OPTIONS] ADDR/PATH EXPRESSION\n", os.Args[0])
		flag.PrintDefaults()
	}

	defer func() {
		os.Exit(exitCode)
	}()

	cli.MainClient(func(cfg *cli.ClientConfig, mnt proxy.FileSystemMount) error {
		switch kind {
		case "d":
			kindPredicate = isDir
		case "f":
			kindPredicate = isFile
		case "", "a":
			kindPredicate = isAny
		default:
			exitCode = 1
			return fmt.Errorf("Unknown -type filter: %#v", kind)
		}

		args := flag.Args()
		var err error
		if len(args) >= 2 {
			if posixRE {
				expr, err = regexp.CompilePOSIX(args[1])
			} else {
				expr, err = regexp.Compile(args[1])
			}
		}
		if err != nil {
			return err
		}

		printOut = make(chan string, 64)
		closed := make(chan struct{}, 1)

		go func() {
			for {
				select {
				case m, ok := <-printOut:
					if ok {
						switch {
						case includePrefix && includeHost:
							fmt.Println(filepath.Join(mnt.Addr, mnt.Prefix, m))
						case includePrefix:
							fmt.Println(filepath.Join(mnt.Prefix, m))
						case includeHost:
							fmt.Println(filepath.Join(mnt.Addr, m))
						default:
							fmt.Println(m)
						}
					} else {
						close(closed)
						return
					}
				default:
					continue
				}
			}
		}()

		err = <-traverse(context.Background(), mnt.FS, mnt.Prefix)
		close(printOut)
		<-closed
		return err
	})
}
