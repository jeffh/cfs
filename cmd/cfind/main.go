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

func isFile(info os.FileInfo) bool { return !info.IsDir() }
func isDir(info os.FileInfo) bool  { return info.IsDir() }
func isAny(info os.FileInfo) bool  { return true }

func traverse(ctx context.Context, fs ninep.TraversableFileSystem, path string) error {
	infos, err := fs.ListDir(context.Background(), path)
	if err != nil {
		return err
	}
	defer infos.Close()

	for {
		info, err := infos.NextFileInfo()
		if info != nil {
			name := info.Name()
			fullpath := filepath.Join(path, name)
			if !strings.HasPrefix(name, ".") {
				if (expr == nil || expr.MatchString(fullpath)) && kindPredicate(info) {
					fmt.Println(fullpath)
				}
				if info.IsDir() {
					if err := traverse(ctx, fs, fullpath); err != nil {
						return err
					}
				}
			}
		}
		if err == io.EOF {
			break
		} else if err != nil {
			return err
		}
	}
	return nil
}

func main() {
	var exitCode int
	var posixRE bool
	var kind string

	flag.BoolVar(&posixRE, "e", false, "Use POSIX Egrep syntax for expressions")
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

		return traverse(context.Background(), mnt.FS, mnt.Prefix)
	})
}
