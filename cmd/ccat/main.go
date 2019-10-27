package main

import (
	"flag"
	"fmt"
	"io"
	"os"

	"github.com/jeffh/cfs/cli"
	"github.com/jeffh/cfs/ninep"
)

func main() {
	var path string
	var numlines int
	var writeFromStdin bool

	flag.IntVar(&numlines, "n", 0, "number of lines to print")
	flag.BoolVar(&writeFromStdin, "stdin", false, "writes data read from stdin before reading from the 9p file")

	flag.Usage = func() {
		fmt.Fprintf(flag.CommandLine.Output(), "cat for CFS\n")
		fmt.Fprintf(flag.CommandLine.Output(), "Usage: %s [OPTIONS] ADDR [PATH]\n", os.Args[0])
		flag.PrintDefaults()
	}

	cli.MainClient(func(c *ninep.Client, fs ninep.FileSystem) error {
		if flag.NArg() == 1 {
			path = ""
		} else {
			path = flag.Arg(1)
		}

		mode := ninep.OpenMode(ninep.OREAD)

		if writeFromStdin {
			mode = ninep.ORDWR
		}

		path = flag.Arg(1)
		h, err := fs.OpenFile(path, mode)
		if err != nil {
			return err
		}
		defer h.Close()

		if writeFromStdin {
			wr := ninep.Writer(h)
			n, err := io.Copy(wr, os.Stdin)
			fmt.Fprintf(os.Stderr, "# wrote %d bytes\n", n)
			if err != nil {
				return err
			}
		}

		rdr := ninep.Reader(h)
		if numlines == 0 {
			io.Copy(os.Stdout, rdr)
		} else {
			buf := make([]byte, 128*1024)
			line := 0
			for {
				_, err := io.ReadFull(rdr, buf)
				b := buf
				for i, r := range b {
					if r == '\n' {
						_, err := os.Stdout.Write(b[:i+1])
						if err != nil {
							return err
						}
						b = b[i+1:]
						line++

						if line >= numlines {
							return nil
						}
					}
				}

				_, err = os.Stdout.Write(b)
				if err != nil {
					return err
				}
				b = nil

				if err != nil {
					if err == io.EOF {
						err = nil
					}
					return err
				}
			}
		}

		return nil
	})
}
