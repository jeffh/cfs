package b2fs

import (
	"errors"
	"io"
)

var errWhence = errors.New("Seek: invalid whence")
var errOffset = errors.New("Seek: invalid offset")

type bufTempFile struct {
	d []byte
	o int64
}

func (b *bufTempFile) Bytes() []byte  { return b.d }
func (b *bufTempFile) String() string { return string(b.d) }

func (b *bufTempFile) ReadAt(p []byte, off int64) (n int, err error) {
	n = copy(p, b.d[off:])
	if n < len(p) {
		err = io.EOF
	}
	return
}

func (b *bufTempFile) Read(p []byte) (n int, err error) {
	n = copy(p, b.d[b.o:])
	b.o += int64(n)
	if n == 0 {
		err = io.EOF
	}
	return
}

func (b *bufTempFile) WriteAt(p []byte, off int64) (n int, err error) {
	n = copy(b.d[off:], p)
	if n < len(p) {
		b.d = append(b.d, p[n:]...)
		n = len(p)
	}
	return
}

func (b *bufTempFile) Write(p []byte) (n int, err error) {
	n = copy(b.d[b.o:], p)
	if n < len(p) {
		b.d = append(b.d, p[n:]...)
		n = len(p)
	}
	b.o += int64(n)
	return
}

func (b *bufTempFile) Seek(offset int64, whence int) (int64, error) {
	switch whence {
	default:
		return 0, errWhence
	case io.SeekStart:
		b.o = offset
	case io.SeekCurrent:
		b.o += offset
	case io.SeekEnd:
		b.o = int64(len(b.d)) - offset
	}
	if offset < 0 {
		return 0, errOffset
	}
	if offset > int64(len(b.d)) {
		return int64(len(b.d)), errOffset
	}
	b.o = offset
	return offset, nil
}

func (b *bufTempFile) Sync() error  { return nil }
func (b *bufTempFile) Close() error { return nil }
