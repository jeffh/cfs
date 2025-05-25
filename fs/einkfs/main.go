package einkfs

import (
	"image"
	"image/color"
	"image/draw"
	"log"

	"github.com/jeffh/cfs/ninep"
	"periph.io/x/conn/v3/spi/spireg"
	"periph.io/x/devices/v3/ssd1306/image1bit"
	"periph.io/x/devices/v3/waveshare2in13v2"
)

type EinkDevice interface {
	Draw(dstRect image.Rectangle, src image.Image, srcPts image.Point) error
	Bounds() image.Rectangle
	Clear(color.Color) error
}

func NewNullEinkDevice(size image.Point) EinkDevice {
	return NullEinkDevice{B: image.Rectangle{Max: size}}
}

type NullEinkDevice struct {
	B image.Rectangle
}

func (d NullEinkDevice) Draw(dstRect image.Rectangle, src image.Image, srcPts image.Point) error {
	return nil
}
func (d NullEinkDevice) Bounds() image.Rectangle { return d.B }
func (d NullEinkDevice) Clear(color.Color) error { return nil }

func NewImage1bitVerticalLSB(r image.Rectangle) draw.Image {
	return image1bit.NewVerticalLSB(r)
}

func NewWaveshare2in13v2FS(fset FontSet, logger *log.Logger, rotation int) (f ninep.FileSystem, close func(), err error) {
	b, err := spireg.Open("")
	if err != nil {
		return nil, nil, err
	}
	dev, err := waveshare2in13v2.NewHat(b, &waveshare2in13v2.EPD2in13v2)
	if err != nil {
		_ = b.Close()
		return nil, nil, err
	}
	err = dev.Init()
	if err != nil {
		_ = b.Close()
		return nil, nil, err
	}
	_ = dev.SetUpdateMode(waveshare2in13v2.Partial)
	closer := func() { _ = b.Close() }
	return NewFS(dev, NewImage1bitVerticalLSB, logger, fset, rotation), closer, nil
}
