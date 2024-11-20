package main

import (
	"embed"
	"flag"
	"fmt"
	"image"
	"image/color"
	"image/draw"
	"image/gif"
	"log"
	"os"

	"github.com/jeffh/cfs/cli"
	"github.com/jeffh/cfs/fs/einkfs"
	"github.com/jeffh/cfs/ninep"
	"golang.org/x/image/font"
	"golang.org/x/image/font/opentype"
	"periph.io/x/host/v3"
)

//go:embed fonts
var fontFiles embed.FS

func main() {
	var nullDevice bool
	var gifMode bool
	var rotation int
	flag.IntVar(&rotation, "rotation", 0, "set the starting rotation of the display to n (0, 1, 2, 3); only applies to text rendering")
	flag.BoolVar(&nullDevice, "null", false, "use a null eink device")
	flag.BoolVar(&gifMode, "gif", false, "use gif mode")

	flag.Usage = func() {
		w := flag.CommandLine.Output()
		fmt.Fprintf(w, "Usage: %s [OPTIONS]\n\n", os.Args[0])
		fmt.Fprintf(w, "Exposes an eink display as a 9p file server on a raspberry pi with waveshare 2in13v2 display.\n\n")
		fmt.Fprintf(w, "The layout of the filesystem is:\n")
		fmt.Fprintf(w, "\t/display   a gif image file of the current display that can be read or written to. Zeros /text on write.\n")
		fmt.Fprintf(w, "\t/text      a string file that is displayed on the display. Can be written to to change the text.\n")
		fmt.Fprintf(w, "\t/metadata  a file with metadata about the display\n")
		fmt.Fprintf(w, "\t/rotate    a file with the current rotation of the display; can be written to to change the rotation\n")
		fmt.Fprintf(w, "\t/ctl       a device file to control settings of the display\n")
		fmt.Fprintf(w, "\n/ctl commands:\n")
		fmt.Fprintf(w, "\tclear       clear the display\n")
		fmt.Fprintf(w, "\trotate=n    set the rotation of the display to n (0, 1, 2, 3); only applies to text rendering\n")
		fmt.Fprintf(w, "\tfont_size=n set the font size to n\n")
		fmt.Fprintf(w, "\n")
		fmt.Fprintf(w, "OPTIONS:\n")
		flag.PrintDefaults()
	}
	cli.ServiceMain(func() ninep.FileSystem {
		if _, err := host.Init(); err != nil {
			log.Fatal(err)
		}
		fset := einkfs.LoadFonts(
			fontFiles,
			[]string{"fonts/Inconsolata_SemiCondensed-ExtraBold.ttf"},
			&opentype.FaceOptions{Size: 14, DPI: 130, Hinting: font.HintingFull},
		)
		if gifMode {
			img := einkfs.NewImage1bitVerticalLSB(image.Rectangle{Max: image.Point{122, 250}})
			draw.Draw(img, img.Bounds(), image.NewUniform(color.White), image.Point{}, draw.Src)
			fset.RenderText(img, "hello\nworld", nil)
			f, err := os.Create("test.gif")
			if err != nil {
				log.Fatal(err)
			}
			opts := &gif.Options{NumColors: 4}
			gif.Encode(f, img, opts)
			f.Close()
			log.Fatal("dump image")
		}

		if nullDevice {
			dev := einkfs.NewNullEinkDevice(image.Point{122, 250})
			return einkfs.NewFS(dev, einkfs.NewImage1bitVerticalLSB, log.Default(), fset, rotation)
		}

		f, closer, err := einkfs.NewWaveshare2in13v2FS(fset, log.Default(), rotation)
		if err != nil {
			log.Fatal(err)
		}
		// we just leak memory here, but it's not a big deal?
		_ = closer
		return f
	})
}
