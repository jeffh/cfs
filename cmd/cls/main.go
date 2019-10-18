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

func main() {
	var path string
	var list bool

	flag.BoolVar(&list, "l", false, "list long format stats about each file")

	flag.Usage = func() {
		fmt.Fprintf(flag.CommandLine.Output(), "ls for CFS\n")
		fmt.Fprintf(flag.CommandLine.Output(), "Usage: %s [OPTIONS] ADDR [PATH]\n", os.Args[0])
		flag.PrintDefaults()
	}

	cli.MainClient(func(c *ninep.Client, fs ninep.FileSystem) error {
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
			w := tabwriter.NewWriter(os.Stdout, 2, 1, 1, ' ', tabwriter.AlignRight|tabwriter.DiscardEmptyColumns)
			for {
				info, err := infos.NextFileInfo()
				if info != nil {
					usr, gid, muid, _ := ninep.FileUsers(info)
					fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%d\t%s\t %s\n", info.Mode(), usr, gid, muid, info.Size(), info.ModTime(), info.Name())
				}
				if err == io.EOF {
					break
				} else if err != nil {
					return err
				}
			}
			w.Flush()
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
