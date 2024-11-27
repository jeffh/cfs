package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/jeffh/cfs/cli"
	"github.com/jeffh/cfs/fs/proxy"
	"github.com/jeffh/cfs/ninep"
	_ "go.uber.org/automaxprocs"
)

var expr *regexp.Regexp
var kindPredicate func(os.FileInfo) bool
var printOut chan string
var serial bool

func isFile(info os.FileInfo) bool { return !info.IsDir() }
func isDir(info os.FileInfo) bool  { return info.IsDir() }
func isAny(info os.FileInfo) bool  { return true }

func traverse(ctx context.Context, fs ninep.TraversableFileSystem, path string) chan error {
	result := make(chan error, 1)
	work := func() {
		for info, err := range fs.ListDir(context.Background(), path) {
			if err != nil {
				result <- err
				return
			}
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
		}
		close(result)
	}

	if serial {
		work()
	} else {
		go work()
	}

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
	flag.BoolVar(&includeHost, "print-addr", false, "Include mount host when printing files")
	flag.BoolVar(&serial, "serial", false, "Don't parallelize 9p requests to speed up fetching, for deterministic ordering")
	flag.StringVar(&kind, "type", "a", "Filter files by type ('d' = directory, 'f' = non-directory, 'a' = anything)")

	flag.Usage = func() {
		w := flag.CommandLine.Output()
		fmt.Fprintf(w, "Usage: %s [OPTIONS] ADDR/PATH EXPRESSION\n\n", os.Args[0])
		fmt.Fprintf(w, "find for CFS\n\n")
		fmt.Fprintf(w, "OPTIONS:\n")
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
