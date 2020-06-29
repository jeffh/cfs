package b2fs

import (
	"bytes"
	"errors"
	"io"
	"io/ioutil"
	"testing"
)

func must(err error) {
	if err != nil {
		panic(err)
	}
}

func TestBufTempFileBehavesLikeABytesBuffer(t *testing.T) {
	expected := bytes.NewBuffer(nil)
	actual := &bufTempFile{}

	t.Run("Testing Writes", func(t *testing.T) {
		words := []string{
			"the ",
			"quick ",
			"brown fox ",
			"jump over the lazy dog",
			"",
		}
		for i, word := range words {
			eN, err := expected.Write([]byte(word))
			must(err)
			aN, err := actual.Write([]byte(word))
			must(err)

			if eN != aN {
				t.Fatalf("[%d] Expected Write() to return the same values (%#v != %#v)", i, eN, aN)
			}
		}

		if expected.String() != actual.String() {
			t.Fatalf("Expected String() to return the same values (%#v != %#v)", expected.String(), actual.String())
		}
	})

	t.Run("Testing seek to beginning", func(t *testing.T) {
		n, err := actual.Seek(0, io.SeekStart)
		must(err)
		if n != 0 {
			t.Fatalf("Expected Seek() to set offset to zero, got %v", n)
		}
	})

	t.Run("Testing Read", func(t *testing.T) {
		text := expected.String()
		expected = bytes.NewBuffer(expected.Bytes())
		eBuf := make([]byte, 5)
		aBuf := make([]byte, 5)
		for i := 0; i < len(text); i += len(eBuf) {
			eN, eErr := expected.Read(eBuf)
			aN, aErr := actual.Read(aBuf)

			if !(eErr == aErr || errors.Is(eErr, aErr)) {
				t.Fatalf("Expected ReadAt(_, %v) to return the same values: error: (%s != %s)", i, eErr, aErr)
			}

			if eN != aN {
				t.Fatalf("Expected Read() to return the same values: length: (%#v != %#v)", eN, aN)
			}

			if !bytes.Equal(eBuf, aBuf) {
				t.Fatalf("Expected Read() to return the same values: bytes: (%#v != %#v)", eBuf, aBuf)
			}
		}
	})
}

func TestBufTempFileBehavesLikeAFileWithAtReadsAndWrites(t *testing.T) {
	f, err := ioutil.TempFile("", "")
	must(err)
	defer f.Close()

	expected := f
	actual := &bufTempFile{}

	must := func(err error) {
		if err != nil {
			panic(err)
		}
	}

	t.Run("Testing Writes", func(t *testing.T) {
		words := []string{
			"jump over the lazy dog",
			"brown fox ",
			"the ",
			"quick ",
			"",
		}
		for i, word := range words {
			eN, err := expected.WriteAt([]byte(word), int64(i))
			must(err)
			aN, err := actual.WriteAt([]byte(word), int64(i))
			must(err)

			if eN != aN {
				t.Fatalf("[%d] Expected WriteAt(%#v, %v) to return the same values (%#v != %#v)", i, word, i, eN, aN)
			}
		}

		_, err := expected.Seek(0, io.SeekStart)
		must(err)
		expectedBytes, err := ioutil.ReadAll(expected)
		must(err)
		expectedString := string(expectedBytes)
		if expectedString != actual.String() {
			t.Fatalf("Expected String() to return the same values (%#v != %#v)", expectedString, actual.String())
		}
	})

	t.Run("Testing ReadAt", func(t *testing.T) {
		text := actual.String()
		eBuf := make([]byte, 5)
		aBuf := make([]byte, 5)
		for i := 0; i < len(text)-1; i += len(eBuf) {
			eN, eErr := expected.ReadAt(eBuf, int64(i))
			aN, aErr := actual.ReadAt(aBuf, int64(i))

			if !(eErr == aErr || errors.Is(eErr, aErr)) {
				t.Fatalf("Expected ReadAt(_, %v) to return the same values: error: (%s != %s)", i, eErr, aErr)
			}

			if eN != aN {
				t.Fatalf("Expected ReadAt(_, %v) to return the same values: length: (%#v != %#v)", i, eN, aN)
			}

			if !bytes.Equal(eBuf, aBuf) {
				t.Fatalf("Expected ReadAt(_, %v) to return the same values: bytes: (%#v != %#v)", i, eBuf, aBuf)
			}
		}
	})
}
