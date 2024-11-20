package einkfs

import (
	"image"
)

func rotate90(img image.Image) *image.NRGBA {
	bounds := img.Bounds()
	newWidth := bounds.Dy()
	newHeight := bounds.Dx()
	dst := image.NewNRGBA(image.Rect(0, 0, newWidth, newHeight))

	for y := bounds.Min.Y; y < bounds.Max.Y; y++ {
		for x := bounds.Min.X; x < bounds.Max.X; x++ {
			// In 90° rotation: new_x = old_y, new_y = width - old_x - 1
			dstX := y - bounds.Min.Y
			dstY := bounds.Max.X - x - 1
			dst.Set(dstX, dstY, img.At(x, y))
		}
	}
	return dst
}

func rotate180(img image.Image) *image.NRGBA {
	bounds := img.Bounds()
	dst := image.NewNRGBA(bounds)

	for y := bounds.Min.Y; y < bounds.Max.Y; y++ {
		for x := bounds.Min.X; x < bounds.Max.X; x++ {
			// In 180° rotation: new_x = width - old_x - 1, new_y = height - old_y - 1
			dstX := bounds.Max.X - x - 1
			dstY := bounds.Max.Y - y - 1
			dst.Set(dstX, dstY, img.At(x, y))
		}
	}
	return dst
}

func rotate270(img image.Image) *image.NRGBA {
	bounds := img.Bounds()
	newWidth := bounds.Dy()
	newHeight := bounds.Dx()
	dst := image.NewNRGBA(image.Rect(0, 0, newWidth, newHeight))

	for y := bounds.Min.Y; y < bounds.Max.Y; y++ {
		for x := bounds.Min.X; x < bounds.Max.X; x++ {
			// In 270° rotation: new_x = height - old_y - 1, new_y = old_x
			dstX := bounds.Max.Y - y - 1
			dstY := x - bounds.Min.X
			dst.Set(dstX, dstY, img.At(x, y))
		}
	}
	return dst
}
