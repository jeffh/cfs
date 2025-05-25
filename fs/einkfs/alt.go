package einkfs

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"image"
	"image/draw"
	"image/gif"
	"io"
	"io/fs"
	"log"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/jeffh/cfs/fs/muxfs"
	"github.com/jeffh/cfs/ninep"
	"github.com/jeffh/cfs/ninep/kvp"
	"periph.io/x/devices/v3/ssd1306/image1bit"
)

func NewFS(dev EinkDevice, mkimg func(r image.Rectangle) draw.Image, logger *log.Logger, fset FontSet, rotation int) ninep.FileSystem {
	state := internalState{
		dev:      dev,
		mkimg:    mkimg,
		fset:     fset,
		display:  mkimg(dev.Bounds()),
		rotation: rotation,
	}

	displayNode := muxfs.DevName("display")
	textNode := muxfs.File(&ninep.SimpleFileInfo{
		FIName: "text",
		FIMode: ninep.Readable | ninep.Writeable,
	})
	ctlNode := muxfs.DevName("ctl")
	metadataNode := muxfs.DevName("metadata")
	rotateNode := muxfs.File(&ninep.SimpleFileInfo{
		FIName: "rotate",
		FIMode: ninep.Readable | ninep.Writeable,
	})

	displayNode.OpenFile = func(ctx context.Context, m ninep.MatchWith[muxfs.Node], flag ninep.OpenMode) (ninep.FileHandle, error) {
		return displayHandle(&state, flag)
	}
	textNode.OpenFile = func(ctx context.Context, m ninep.MatchWith[muxfs.Node], flag ninep.OpenMode) (ninep.FileHandle, error) {
		return textHandle(&state, flag)
	}
	ctlNode.OpenFile = func(ctx context.Context, m ninep.MatchWith[muxfs.Node], flag ninep.OpenMode) (ninep.FileHandle, error) {
		return ctlHandle(&state, flag)
	}
	metadataNode.OpenFile = func(ctx context.Context, m ninep.MatchWith[muxfs.Node], flag ninep.OpenMode) (ninep.FileHandle, error) {
		return metadataHandle(&state, flag)
	}
	rotateNode.OpenFile = func(ctx context.Context, m ninep.MatchWith[muxfs.Node], flag ninep.OpenMode) (ninep.FileHandle, error) {
		return rotateHandle(&state, flag)
	}

	mx := muxfs.NewMux().
		Define().Path("/display").With(displayNode).As("display").
		Define().Path("/text").With(textNode).As("text").
		Define().Path("/ctl").With(ctlNode).As("ctl").
		Define().Path("/metadata").With(metadataNode).As("metadata").
		Define().Path("/rotate").With(rotateNode).As("rotate")

	return muxfs.New(mx)
}

type internalState struct {
	dev   EinkDevice
	mkimg func(r image.Rectangle) draw.Image
	fset  FontSet

	mu          sync.Mutex
	lastUpdated time.Time
	display     draw.Image
	text        string
	rotation    int
}

func (f *internalState) unsafeBlit(img image.Image) {
	zero := image.Point{}
	draw.Draw(f.display, f.display.Bounds(), img, zero, draw.Src)
	if time.Since(f.lastUpdated) >= 24*time.Hour {
		if err := f.dev.Clear(image.White); err != nil {
			log.Printf("clear display: %v", err)
		}
		f.lastUpdated = time.Now()
	}
	err := f.dev.Draw(f.dev.Bounds(), f.display, zero)
	if err != nil {
		return
	}
}
func (f *internalState) blit(img image.Image, text string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.text = text // clear text when display is written
	f.unsafeBlit(img)
}
func (f *internalState) textImage(txt string) image.Image {
	txt = strings.TrimSpace(txt)
	img := f.mkimg(rotateBounds(f.display.Bounds(), f.rotation))
	draw.Draw(img, img.Bounds(), image.White, image.Point{}, draw.Src)
	f.fset.RenderText(img, txt, nil)
	switch f.rotation {
	case 1:
		img = rotate270(img)
	case 3:
		img = rotate90(img)
	default:
	}
	return img
}

func (f *internalState) drawText(txt string) {
	f.mu.Lock()
	text := f.text
	f.mu.Unlock()
	if text != txt {
		f.blit(f.textImage(txt), txt)
	}
}

func (f *internalState) clear() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.text = ""
	draw.Draw(f.display, f.display.Bounds(), &image.Uniform{image1bit.On}, image.Point{}, draw.Src)
	f.lastUpdated = time.Now()
	f.dev.Clear(image.White)
	f.dev.Draw(f.dev.Bounds(), f.display, image.Point{})
}

func displayHandle(state *internalState, flag ninep.OpenMode) (ninep.FileHandle, error) {
	h, r, w := ninep.DeviceHandle(flag)
	if w != nil {
		go func() {
			opts := &gif.Options{NumColors: 4}
			state.mu.Lock()
			err := gif.Encode(w, state.display, opts)
			state.mu.Unlock()
			if err != nil {
				w.CloseWithError(err)
				return
			}
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
			state.mu.Lock()
			rotate := state.rotation
			state.mu.Unlock()
			switch rotate {
			case 0:
				// do nothing
			case 1:
				img = rotate90(img)
			case 2:
				img = rotate180(img)
			case 3:
				img = rotate270(img)
			}
			state.mu.Lock()
			defer state.mu.Unlock()
			state.text = ""
			state.unsafeBlit(img)
		}()
	}
	return h, nil
}

func textHandle(state *internalState, flag ninep.OpenMode) (ninep.FileHandle, error) {
	h, r, w := ninep.DeviceHandle(flag)
	if w != nil {
		go func() {
			state.mu.Lock()
			text := state.text
			state.mu.Unlock()
			_, err := w.Write([]byte(text))
			if err != nil {
				w.CloseWithError(err)
			}
			w.Close()
		}()
	}
	if r != nil {
		go func() {
			var buf bytes.Buffer
			if _, err := io.Copy(&buf, r); err != nil {
				r.CloseWithError(err)
				return
			}
			defer r.Close()
			txt := buf.String()
			txt = strings.TrimSpace(txt)
			state.drawText(txt)
		}()
	}
	return h, nil
}

func ctlHandle(state *internalState, _ ninep.OpenMode) (ninep.FileHandle, error) {
	h, r := ninep.WriteOnlyDeviceHandle()
	if r != nil {
		go func() {
			defer r.Close()
			scanner := bufio.NewScanner(r)
			for scanner.Scan() {
				kv, err := kvp.ParseKeyValues(scanner.Text())
				if err != nil {
					continue
				}
				if rotateStr := kv.GetOne("rotate"); rotateStr != "" {
					if rotate, err := strconv.Atoi(rotateStr); err == nil && rotate >= 0 {
						rotate = rotate % 4
						state.mu.Lock()
						if rotate != state.rotation {
							state.rotation = rotate
							state.unsafeBlit(state.textImage(state.text))
						}
						state.mu.Unlock()
					}
				}
				if fontSizeStr := kv.GetOne("font_size"); fontSizeStr != "" {
					if fontSize, err := strconv.ParseFloat(fontSizeStr, 64); err == nil {
						state.fset.Options.Size = fontSize
						state.fset.Reset(state.fset.Options)
					}
				}
				if kv.Has("clear") {
					state.clear()
				}
			}
		}()
	}
	return h, nil
}

func metadataHandle(state *internalState, flag ninep.OpenMode) (ninep.FileHandle, error) {
	if flag&ninep.OWRITE != 0 {
		return nil, fs.ErrPermission
	}

	bounds := state.dev.Bounds()
	metadata := [...]string{
		"color_model=1bitVerticalLSB",
		fmt.Sprintf("width=%d", bounds.Max.X),
		fmt.Sprintf("height=%d", bounds.Max.Y),
	}

	return &ninep.ReadOnlyMemoryFileHandle{
		Contents: []byte(strings.Join(metadata[:], "\n")),
	}, nil
}

func rotateHandle(state *internalState, flag ninep.OpenMode) (ninep.FileHandle, error) {
	h, r, w := ninep.DeviceHandle(flag)
	if r != nil {
		go func() {
			defer r.Close()
			var buf bytes.Buffer
			io.Copy(&buf, r)
			rotate, err := strconv.Atoi(strings.TrimSpace(buf.String()))
			if err != nil || rotate < 0 || rotate > 3 {
				r.CloseWithError(fmt.Errorf("invalid rotation value: must be 0-3"))
				return
			}
			state.mu.Lock()
			if rotate != state.rotation {
				state.rotation = rotate
				state.unsafeBlit(state.textImage(state.text))
			}
			state.mu.Unlock()
		}()
	}
	if w != nil {
		go func() {
			defer w.Close()
			state.mu.Lock()
			rotation := state.rotation
			state.mu.Unlock()
			fmt.Fprintf(w, "%d\n", rotation)
			w.Close()
		}()
	}
	return h, nil
}

func rotateBounds(r image.Rectangle, rotation int) image.Rectangle {
	if rotation%2 == 1 {
		return image.Rect(r.Min.Y, r.Min.X, r.Max.Y, r.Max.X)
	}
	return r
}
