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

	"github.com/jeffh/cfs/cli"
	"github.com/jeffh/cfs/fs/proxy"
	"github.com/jeffh/cfs/ninep"
)

const (
	KB = 1024
	MB = KB * 1024
	GB = MB * 1024
	TB = GB * 1024
	PB = TB * 1024
)

var humanSizes bool

func printInfo(w io.Writer, info os.FileInfo, replacedName string) {
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
	fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\t%s\t %s\n", info.Mode(), usr, gid, muid, sizeStr, info.ModTime().Format(time.RFC822), n)
}

func main() {
	var all bool
	var list bool
	var nocols bool

	flag.BoolVar(&all, "a", false, "list info of current directory and parent directory and any dot-prefixed files")
	flag.BoolVar(&list, "l", false, "list long format stats about each file")
	flag.BoolVar(&humanSizes, "h", false, "list file sizes in human-readable formats")
	flag.BoolVar(&nocols, "nocols", false, "avoids printing in nice columns, useful to verify streaming behavior")

	flag.Usage = func() {
		fmt.Fprintf(flag.CommandLine.Output(), "ls for CFS\n")
		fmt.Fprintf(flag.CommandLine.Output(), "Usage: %s [OPTIONS] ADDR/PATH\n", os.Args[0])
		flag.PrintDefaults()
	}

	cli.MainClient(func(cfg *cli.ClientConfig, mnt proxy.FileSystemMount) error {
		infos, err := mnt.FS.ListDir(context.Background(), mnt.Prefix)
		if err != nil {
			return err
		}
		defer infos.Close()

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
					printInfo(w, info, ".")
				}
				info, err = mnt.FS.Stat(context.Background(), filepath.Join(mnt.Prefix, ".."))
				if err == nil && info != nil {
					printInfo(w, info, "..")
				}
			}
			for {
				info, err := infos.NextFileInfo()
				if info != nil {
					if all || !strings.HasPrefix(info.Name(), ".") {
						printInfo(w, info, "")
					}
				}
				if err == io.EOF {
					break
				} else if err != nil {
					return err
				}
			}
		} else {
			if all {
				fmt.Println(".")
				fmt.Println("..")
			}
			for {
				info, err := infos.NextFileInfo()
				if info != nil {
					if all || !strings.HasPrefix(info.Name(), ".") {
						fmt.Println(info.Name())
					}
				}
				if err == io.EOF {
					break
				} else if err != nil {
					return err
				}
			}
		}
		return nil
	})
}
