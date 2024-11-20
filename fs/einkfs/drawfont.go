package einkfs

import (
	"image"
	"image/draw"
	"io/fs"
	"log"
	"log/slog"
	"os"
	"strings"

	"github.com/flopp/go-findfont"
	"golang.org/x/image/font"
	"golang.org/x/image/font/opentype"
	"golang.org/x/image/font/sfnt"
	"golang.org/x/image/math/fixed"
)

// fallbackFonts is a list of fonts that are used to render text when the
// requested font is not available. This useful to fallback to a renderable
// glyph set.
var fallbackFonts = []string{
	// misc
	"SF-UI-Display-Bold", "SourceCodePro-Bold",
	// macOS
	"AppleSDGothicNeo", "Apple Symbols", "Apple Color Emoji", "Apple Braille",
	// windows
	"Consolas", "Segoe UI", "Segoe UI Emoji", "Segoe UI Symbol", "Segoe UI Historic",
}

func AppendFallbackFonts(names []string) []string {
	return append(names, fallbackFonts...)
}

type FontSet struct {
	Faces       []font.Face
	Fonts       []*sfnt.Font
	Collections []*sfnt.Collection
	Names       []string

	Options *opentype.FaceOptions
}

// RenderText renders the given text to the destination image using the loaded
// fonts. The start function is used to determine the starting point for the
// text.
func (s FontSet) RenderText(dst draw.Image, text string, start func(font.Metrics) fixed.Point26_6) {
	if len(text) == 0 {
		return
	}
	metrics := s.Faces[0].Metrics()
	drawer := font.Drawer{
		Dst: dst,
		Src: image.Black,
		Dot: fixed.P(0, metrics.Ascent.Ceil()),
	}
	if start != nil {
		drawer.Dot = start(metrics)
	}

	startX := drawer.Dot.X
	for _, line := range strings.Split(text, "\n") {
		drawer.Dot.X = startX
		for _, r := range line {
			_, _, face := s.glphAdvance(r)
			// idx, advance, face := fonts.glphAdvance(r)
			// fmt.Printf("Draw(%q) => %s %s %#v\n", string(r), advance.String(), fonts.Names[idx], drawer.Dot)
			if face != nil {
				drawer.Face = face
				drawer.DrawString(string(r))
			}
		}
		drawer.Dot.Y += metrics.Height
	}
}

// LoadFonts loads a set of named fonts from the given file system. The fonts
// are used in order to render to text based on each font's available glyphs.
//
// Any invalid fonts are silently ignored.
func LoadFonts(f fs.FS, names []string, opt *opentype.FaceOptions) FontSet {
	if f == nil {
		f = os.DirFS("/")
	}
	fonts := make([]*sfnt.Font, 0, len(names))
	collections := make([]*sfnt.Collection, 0, len(names))
	faces := make([]font.Face, 0, len(names))
	for _, name := range names {
		fnts, coll, err := loadFont(f, name)
		if err != nil {
			slog.Error("error loading font", slog.String("name", name), slog.String("error", err.Error()))
		} else {
			fonts = append(fonts, fnts...)
			collections = append(collections, coll)
			for _, fnt := range fnts {
				face, err := opentype.NewFace(fnt, opt)
				if err == nil {
					faces = append(faces, face)
				} else {
					slog.Error("error reading font face", slog.String("name", name), slog.String("error", err.Error()))
				}
			}
		}
	}
	return FontSet{Faces: faces, Fonts: fonts, Collections: collections, Names: names, Options: opt}
}

func (s *FontSet) Reset(opts *opentype.FaceOptions) {
	for i, fnt := range s.Fonts {
		face, err := opentype.NewFace(fnt, opts)
		if err == nil {
			s.Faces[i] = face
		}
	}
	s.Options = opts
}

func loadFont(f fs.FS, fontName string) ([]*sfnt.Font, *sfnt.Collection, error) {
	fontPath, err := findfont.Find(fontName)
	var file []byte
	if err != nil {
		file, err = fs.ReadFile(f, fontName)
		if err != nil {
			return nil, nil, err
		}
	} else {
		file, err = os.ReadFile(fontPath)
	}
	if err != nil {
		return nil, nil, err
	}
	if strings.HasSuffix(fontPath, ".ttf") {
		fnt, err := opentype.Parse(file)
		if err != nil {
			return nil, nil, err
		}
		return []*sfnt.Font{fnt}, nil, nil
	} else {
		fnt, err := opentype.ParseCollection(file)
		if err != nil {
			return nil, nil, err
		}
		fs := make([]*sfnt.Font, 0, fnt.NumFonts())
		for i := 0; i < fnt.NumFonts(); i++ {
			f, err := fnt.Font(0)
			if err == nil {
				fs = append(fs, f)
			}
		}
		return fs, fnt, err
	}
}

func (fonts FontSet) glphAdvance(r rune) (int, fixed.Int26_6, font.Face) {
	for i, f := range fonts.Faces {
		amt, ok := f.GlyphAdvance(r)
		if ok {
			return i, amt, f
		}
	}
	log.Printf("no glyph for %q\n", string(r))
	return -1, fixed.I(0), nil
}
