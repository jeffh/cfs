package main

import (
	"flag"
	"fmt"
	"io"
	"os"
	"text/tabwriter"

	"github.com/jeffh/cfs/cli"
	"github.com/jeffh/cfs/ninep"
)

const (
	KB = 1024
	MB = KB * 1024
	GB = MB * 1024
	TB = GB * 1024
	PB = TB * 1024
)

func main() {
	var path string
	var list bool
	var nocols bool
	var humanSizes bool

	flag.BoolVar(&list, "l", false, "list long format stats about each file")
	flag.BoolVar(&humanSizes, "h", false, "list file sizes in human-readable formats")
	flag.BoolVar(&nocols, "nocols", false, "avoids printing in nice columns, useful to verify streaming behavior")

	flag.Usage = func() {
		fmt.Fprintf(flag.CommandLine.Output(), "ls for CFS\n")
		fmt.Fprintf(flag.CommandLine.Output(), "Usage: %s [OPTIONS] ADDR [PATH]\n", os.Args[0])
		flag.PrintDefaults()
	}

	cli.MainClient(func(c ninep.Client, fs *ninep.FileSystemProxy) error {
		if flag.NArg() == 1 {
			path = ""
		} else {
			path = flag.Arg(1)
		}

		path = flag.Arg(1)
		infos, err := fs.ListDir(path)
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
			for {
				info, err := infos.NextFileInfo()
				if info != nil {
					usr, gid, muid, _ := ninep.FileUsers(info)
					size := info.Size()
					var sizeStr string
					if humanSizes {
						if size > PB {
							sizeStr = fmt.Sprintf("%.2f PB", float64(size)/PB)
						} else if size > TB {
							sizeStr = fmt.Sprintf("%.2f TB", float64(size)/TB)
						} else if size > GB {
							sizeStr = fmt.Sprintf("%.2f GB", float64(size)/GB)
						} else if size > MB {
							sizeStr = fmt.Sprintf("%.2f MB", float64(size)/MB)
						} else if size > KB {
							sizeStr = fmt.Sprintf("%.2f KB", float64(size)/KB)
						} else {
							sizeStr = fmt.Sprintf("%d", size)
						}
					} else {
						sizeStr = fmt.Sprintf("%d", size)
					}
					fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\t%s\t %s\n", info.Mode(), usr, gid, muid, sizeStr, info.ModTime(), info.Name())
				}
				if err == io.EOF {
					break
				} else if err != nil {
					return err
				}
			}
		} else {
			for {
				info, err := infos.NextFileInfo()
				if info != nil {
					fmt.Println(info.Name())
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
