package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/fatih/color"
	"github.com/jeffh/cfs/cli"
	"github.com/jeffh/cfs/fs/proxy"
	"github.com/jeffh/cfs/ninep"
)

var (
	dirColor           = color.BlueString
	irregularFileColor = color.MagentaString
)

const (
	KB = 1024
	MB = KB * 1024
	GB = MB * 1024
	TB = GB * 1024
	PB = TB * 1024
)

var nocolors bool
var humanSizes bool

func printInfo(w io.Writer, info os.FileInfo, replacedName string, namePrefix string) {
	usr, gid, muid, _ := ninep.FileUsers(info)
	size := info.Size()
	var sizeStr string
	if humanSizes {
		if size > PB {
			sizeStr = fmt.Sprintf("%.2fPB", float64(size)/PB)
		} else if size > TB {
			sizeStr = fmt.Sprintf("%.2fTB", float64(size)/TB)
		} else if size > GB {
			sizeStr = fmt.Sprintf("%.2fGB", float64(size)/GB)
		} else if size > MB {
			sizeStr = fmt.Sprintf("%.2fMB", float64(size)/MB)
		} else if size > KB {
			sizeStr = fmt.Sprintf("%.2fKB", float64(size)/KB)
		} else {
			sizeStr = fmt.Sprintf("%dB", size)
		}
	} else {
		sizeStr = fmt.Sprintf("%d", size)
	}
	n := info.Name()
	if replacedName != "" {
		n = replacedName
	}
	if info.IsDir() {
		n = dirColor(n)
	} else if !info.Mode().IsRegular() {
		n = irregularFileColor(n)
	}
	if namePrefix != "" {
		n = namePrefix + n
	}

	fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\t%s\t %s\n", info.Mode(), usr, gid, muid, sizeStr, info.ModTime().Format(time.RFC822), n)
}

func main() {
	var all bool
	var list bool
	var nocols bool
	var includeHost bool

	flag.BoolVar(&all, "a", false, "list info of current directory and parent directory and any dot-prefixed files")
	flag.BoolVar(&list, "l", false, "list long format stats about each file")
	flag.BoolVar(&includeHost, "print-addr", false, "Include mount host when printing files")
	flag.BoolVar(&humanSizes, "h", false, "list file sizes in human-readable formats")
	flag.BoolVar(&nocols, "nocols", false, "avoids printing in nice columns, useful to verify streaming behavior")
	flag.BoolVar(&nocolors, "nocolor", false, "Disable color for terminal output")

	cli.SupportsColor(nocolors)

	flag.Usage = func() {
		w := flag.CommandLine.Output()
		fmt.Fprintf(w, "Usage: %s [OPTIONS] ADDR/PATH\n\n", os.Args[0])
		fmt.Fprintf(w, "ls for CFS\n\n")
		fmt.Fprintf(w, "OPTIONS:\n")
		flag.PrintDefaults()
	}

	cli.MainClient(func(cfg *cli.ClientConfig, mnt proxy.FileSystemMount) error {
		namePrefix := ""
		if includeHost {
			namePrefix = mnt.Addr + "/"
		}

		if list {
			var w io.Writer
			if nocols {
				w = os.Stdout
			} else {
				tw := tabwriter.NewWriter(os.Stdout, 2, 1, 1, ' ', tabwriter.AlignRight|tabwriter.DiscardEmptyColumns)
				defer tw.Flush()
				w = tw
			}
			if all {
				info, err := mnt.FS.Stat(context.Background(), mnt.Prefix)
				if err == nil && info != nil {
					printInfo(w, info, ".", namePrefix)
				}
				info, err = mnt.FS.Stat(context.Background(), filepath.Join(mnt.Prefix, ".."))
				if err == nil && info != nil {
					printInfo(w, info, "..", namePrefix)
				}
			}
			for info, err := range mnt.FS.ListDir(context.Background(), mnt.Prefix) {
				if err != nil {
					return err
				}
				if info != nil {
					if all || !strings.HasPrefix(info.Name(), ".") {
						printInfo(w, info, "", namePrefix)
					}
				}
			}
		} else {
			if all {
				fmt.Println(dirColor(namePrefix + "."))
				fmt.Println(dirColor(namePrefix + ".."))
			}
			for info, err := range mnt.FS.ListDir(context.Background(), mnt.Prefix) {
				if err != nil {
					return err
				}
				if info != nil {
					n := info.Name()
					if all || !strings.HasPrefix(n, ".") {
						n = namePrefix + n
						if info.IsDir() {
							n = dirColor(n)
						} else if !info.Mode().IsRegular() {
							n = irregularFileColor(n)
						}
						fmt.Println(n)
					}
				}
			}
		}
		return nil
	})
}
