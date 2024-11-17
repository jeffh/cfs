package einkfs

import (
	"os"

	"github.com/flopp/go-findfont"
	"golang.org/x/image/font"
	"golang.org/x/image/font/basicfont"
	"golang.org/x/image/font/opentype"
)

func FindFont(name string) (font.Face, error) {
	fontPath, err := findfont.Find(name)
	var f font.Face
	if err == nil {
		file, err := os.ReadFile(fontPath)
		if err != nil {
			return nil, err
		}
		fnt, err := opentype.Parse(file)
		if err != nil {
			return nil, err
		}
		f, err = opentype.NewFace(fnt, nil)
		if err != nil {
			return nil, err
		}
	} else {
		f = basicfont.Face7x13
	}
	return f, nil
}
