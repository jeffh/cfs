package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"

	"github.com/jeffh/cfs/cli"
	"github.com/jeffh/cfs/fs/proxy"
	"github.com/jeffh/cfs/ninep"
)

func main() {
	var numlines int
	var writeFromStdin bool
	var writeFromFile string
	var newline bool
	var printFilename bool

	flag.IntVar(&numlines, "n", 0, "number of lines to print")
	flag.BoolVar(&writeFromStdin, "stdin", false, "writes data read from stdin before reading from the 9p file")
	flag.StringVar(&writeFromFile, "stdin-file", "", "writes data read from a file before reading from the 9p file")
	flag.BoolVar(&newline, "newline", false, "print a newline at the end")
	flag.BoolVar(&printFilename, "filename", false, "prints the filename prior to writing its contents")

	flag.Usage = func() {
		w := flag.CommandLine.Output()
		_, _ = fmt.Fprintf(w, "Usage: %s [OPTIONS] ADDR/PATH\n\n", os.Args[0])
		_, _ = fmt.Fprintf(w, "cat for CFS\n\n")
		_, _ = fmt.Fprintf(w, "OPTIONS:\n")
		flag.PrintDefaults()
	}

	cli.MainClient(func(cfg *cli.ClientConfig, mnt proxy.FileSystemMount) error {
		mode := ninep.OpenMode(ninep.OREAD)

		if writeFromStdin {
			mode = ninep.ORDWR
		}

		h, err := mnt.FS.OpenFile(context.Background(), mnt.Prefix, mode)
		if err != nil {
			return err
		}
		defer func() { _ = h.Close() }()

		if printFilename {
			fmt.Printf("=== %s ===\n", mnt.Prefix)
		}

		if writeFromFile != "" {
			wr := ninep.Writer(h)
			f, err := os.Open(writeFromFile)
			if err != nil {
				_, _ = fmt.Fprintf(os.Stderr, "failed opening file: %s: %s\n", writeFromFile, err)
				return err
			}
			n, err := io.Copy(wr, f)
			_, _ = fmt.Fprintf(os.Stderr, "# wrote %d bytes\n", n)
			_ = f.Close()
			if err != nil {
				return err
			}
		} else if writeFromStdin {
			wr := ninep.Writer(h)
			n, err := io.Copy(wr, os.Stdin)
			_, _ = fmt.Fprintf(os.Stderr, "# wrote %d bytes\n", n)
			if err != nil {
				return err
			}
		}

		rdr := ninep.Reader(h)
		if numlines == 0 {
			if _, err := io.Copy(os.Stdout, rdr); err != nil {
				return err
			}
		} else {
			buf := make([]byte, 128*1024)
			line := 0
			for {
				if _, err := io.ReadFull(rdr, buf); err != nil {
					if err == io.EOF || err == io.ErrUnexpectedEOF {
						return nil
					}
					return err
				}
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
			}
		}

		if newline {
			if _, err := os.Stdout.Write([]byte("\n")); err != nil {
				return err
			}
		}

		return nil
	})
}
