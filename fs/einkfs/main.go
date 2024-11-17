package einkfs

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"image"
	"image/draw"
	"image/png"
	"io"
	"io/fs"
	"iter"
	"log"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/disintegration/imaging"
	"github.com/jeffh/cfs/ninep"
	"github.com/jeffh/cfs/ninep/kvp"
	"golang.org/x/image/font"
	"golang.org/x/image/font/basicfont"
	"golang.org/x/image/math/fixed"
	"periph.io/x/conn/v3/spi/spireg"
	"periph.io/x/devices/v3/ssd1306/image1bit"
	"periph.io/x/devices/v3/waveshare2in13v2"
)

type EinkDevice interface {
	Draw(dstRect image.Rectangle, src image.Image, srcPts image.Point) error
	Bounds() image.Rectangle
}

var mx = ninep.NewMux().
	Define().Path("/").As("root").
	Define().Path("/display").As("display").
	Define().Path("/text").As("text").
	Define().Path("/ctl").As("ctl").
	Define().Path("/metadata").As("metadata")

type fsys struct {
	logger *log.Logger
	dev    EinkDevice

	mu       sync.Mutex
	display  *image1bit.VerticalLSB
	text     string
	fnt      font.Face
	rotation int
}

func NewWaveshare2in13v2FS(logger *log.Logger) (f ninep.FileSystem, close func(), err error) {
	b, err := spireg.Open("")
	if err != nil {
		return nil, nil, err
	}
	dev, err := waveshare2in13v2.NewHat(b, &waveshare2in13v2.EPD2in13v2)
	if err != nil {
		b.Close()
		return nil, nil, err
	}
	err = dev.Init()
	if err != nil {
		b.Close()
		return nil, nil, err
	}
	closer := func() { b.Close() }
	return NewFS(dev, logger, nil, 0), closer, nil
}

func NewFS(dev EinkDevice, logger *log.Logger, fnt font.Face, rotation int) ninep.FileSystem {
	return &fsys{
		logger:   logger,
		dev:      dev,
		display:  image1bit.NewVerticalLSB(dev.Bounds()),
		fnt:      fnt,
		rotation: rotation,
	}
}

func (f *fsys) MakeDir(ctx context.Context, path string, mode ninep.Mode) error {
	return ninep.ErrWriteNotAllowed
}

func (f *fsys) CreateFile(ctx context.Context, path string, flag ninep.OpenMode, mode ninep.Mode) (ninep.FileHandle, error) {
	return nil, fs.ErrPermission
}

func (f *fsys) OpenFile(ctx context.Context, path string, flag ninep.OpenMode) (ninep.FileHandle, error) {
	var res ninep.Match
	ok := mx.Match(path, &res)
	if !ok {
		return nil, fs.ErrNotExist
	}

	switch res.Id {
	case "display":
		return f.displayHandle(flag)
	case "text":
		return f.textHandle(flag)
	case "ctl":
		return f.ctlHandle(flag)
	case "metadata":
		return f.metadataHandle(flag)
	default:
		return nil, fs.ErrNotExist
	}
}

func (f *fsys) ListDir(ctx context.Context, path string) iter.Seq2[fs.FileInfo, error] {
	var res ninep.Match
	ok := mx.Match(path, &res)
	if !ok {
		return ninep.FileInfoErrorIterator(fs.ErrNotExist)
	}

	if res.Id != "root" {
		return ninep.FileInfoErrorIterator(fs.ErrNotExist)
	}

	now := time.Now()
	files := []fs.FileInfo{
		ninep.DevFileInfo("display"),
		ninep.DevFileInfo("text"),
		ninep.DevFileInfo("ctl"),
		&ninep.SimpleFileInfo{
			FIName:    "metadata",
			FIMode:    ninep.Readable,
			FIModTime: now,
		},
	}

	return func(yield func(fs.FileInfo, error) bool) {
		for _, file := range files {
			if !yield(file, nil) {
				return
			}
		}
	}
}

func (f *fsys) Stat(ctx context.Context, path string) (fs.FileInfo, error) {
	var res ninep.Match
	ok := mx.Match(path, &res)
	if !ok {
		return nil, fs.ErrNotExist
	}

	now := time.Now()
	switch res.Id {
	case "root":
		return &ninep.SimpleFileInfo{
			FIName:    ".",
			FIMode:    fs.ModeDir | ninep.Readable | ninep.Executable,
			FIModTime: now,
		}, nil
	case "display", "text", "ctl":
		return ninep.DevFileInfo(res.Id), nil
	case "metadata":
		return &ninep.SimpleFileInfo{
			FIName:    "metadata",
			FIMode:    ninep.Readable,
			FIModTime: now,
		}, nil
	default:
		return nil, fs.ErrNotExist
	}
}

func (f *fsys) WriteStat(ctx context.Context, path string, stat ninep.Stat) error {
	return fs.ErrPermission
}

func (f *fsys) Delete(ctx context.Context, path string) error {
	return fs.ErrPermission
}

func (f *fsys) displayHandle(flag ninep.OpenMode) (ninep.FileHandle, error) {
	h, r, w := ninep.DeviceHandle(flag)
	if w != nil {
		go func() {
			f.mu.Lock()
			displayCopy := image.NewRGBA(f.display.Bounds())
			draw.Draw(displayCopy, displayCopy.Bounds(), f.display, image.Point{}, draw.Src)
			f.mu.Unlock()
			err := png.Encode(w, displayCopy)
			if err != nil {
				w.CloseWithError(err)
				return
			}
			f.text = "" // clear text when display is written
			w.Close()
		}()
	}
	if r != nil {
		go func() {
			img, _, err := image.Decode(r)
			if err != nil {
				r.CloseWithError(err)
				return
			}
			f.blit(img)
		}()
	}
	return h, nil
}

func (f *fsys) blit(img image.Image) {
	f.mu.Lock()
	defer f.mu.Unlock()
	switch f.rotation {
	case 0:
		// do nothing
	case 1:
		img = imaging.Rotate90(img)
	case 2:
		img = imaging.Rotate180(img)
	case 3:
		img = imaging.Rotate270(img)
	}
	err := f.dev.Draw(f.dev.Bounds(), img, image.Point{})
	if err != nil {
		return
	}
	draw.Draw(f.display, f.display.Bounds(), img, image.Point{}, draw.Src)
}

func (f *fsys) textHandle(flag ninep.OpenMode) (ninep.FileHandle, error) {
	h, r, w := ninep.DeviceHandle(flag)
	if w != nil {
		var buf bytes.Buffer
		go func() {
			if _, err := io.Copy(&buf, r); err != nil {
				w.CloseWithError(err)
				return
			}
			defer w.Close()
			txt := buf.String()
			f.mu.Lock()
			f.text = txt
			f.mu.Unlock()

			f.drawText(txt)
		}()
	}
	if r != nil {
		go func() {
			defer r.Close()
			f.mu.Lock()
			text := f.text
			f.mu.Unlock()
			w.Write([]byte(text))
		}()
	}
	return h, nil
}

func (f *fsys) drawText(txt string) {
	lines := strings.Split(txt, "\n")
	for i, line := range lines {
		if f.logger != nil {
			f.logger.Printf("text[%d]: %q", i, line)
		}
	}
	img := image1bit.NewVerticalLSB(f.display.Bounds())
	draw.Draw(img, img.Bounds(), image.White, image.Point{}, draw.Src)
	f.mu.Lock()
	fnt := f.fnt
	f.mu.Unlock()

	if fnt == nil {
		fnt = basicfont.Face7x13
	}

	metrics := fnt.Metrics()
	drawer := font.Drawer{
		Dst:  img,
		Src:  image.Black,
		Face: fnt,
		Dot:  fixed.P(0, metrics.Ascent.Ceil()),
	}
	for i, line := range lines {
		drawer.DrawString(line)
		drawer.Dot.X = fixed.I(0)
		drawer.Dot.Y = metrics.Ascent + (fixed.I(i).Mul(metrics.Height))
		drawer.DrawString(line)
	}
	f.blit(img)
}

func (f *fsys) ctlHandle(flag ninep.OpenMode) (ninep.FileHandle, error) {
	h, r, w := ninep.DeviceHandle(flag)
	if w != nil {
		go func() {
			defer r.Close()
			scanner := bufio.NewScanner(r)
			for scanner.Scan() {
				kv, err := kvp.ParseKeyValues(scanner.Text())
				if err != nil {
					continue
				}
				if rotateStr := kv.GetOne("rotate"); rotateStr != "" {
					if rotate, err := strconv.Atoi(rotateStr); err == nil && rotate >= 0 && rotate <= 3 {
						f.mu.Lock()
						f.rotation = rotate
						f.mu.Unlock()
					}
				}
				if kv.Has("clear") {
					f.clear()
				}
			}
		}()
	}
	return h, nil
}

func (f *fsys) clear() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.text = ""
	draw.Draw(f.display, f.display.Bounds(), &image.Uniform{image1bit.On}, image.Point{}, draw.Src)
	f.dev.Draw(f.dev.Bounds(), f.display, image.Point{})
}

func (f *fsys) metadataHandle(flag ninep.OpenMode) (ninep.FileHandle, error) {
	if flag&ninep.OWRITE != 0 {
		return nil, fs.ErrPermission
	}

	bounds := f.dev.Bounds()
	metadata := [...]string{
		"type=waveshare2in13v2",
		"color_model=1bitVerticalLSB",
		fmt.Sprintf("width=%d", bounds.Max.X),
		fmt.Sprintf("height=%d", bounds.Max.Y),
	}

	return &ninep.ReadOnlyMemoryFileHandle{
		Contents: []byte(strings.Join(metadata[:], "\n")),
	}, nil
}
