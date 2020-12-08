package b2fs

import (
	"context"
	"io"
	"io/ioutil"

	"github.com/jeffh/b2client/b2"
	"github.com/jeffh/cfs/ninep"
)

type TempStorage interface {
	Store(r io.Reader) (TempFile, int64, error)
}

var globalTempStorage = LocalTempStorage{Prefix: "b2fs-"}

type MemTempStorage struct{}

func (ts *MemTempStorage) Store(r io.Reader) (TempFile, int64, error) {
	buf := &bufTempFile{}
	n, err := io.Copy(buf, r)
	if err != nil {
		return nil, n, err
	}
	return buf, n, err
}

type LocalTempStorage struct {
	Dir, Prefix string
}

func (ts *LocalTempStorage) Store(r io.Reader) (TempFile, int64, error) {
	f, err := ioutil.TempFile(ts.Dir, ts.Prefix)
	if err != nil {
		return nil, 0, err
	}

	var n int64
	if r != nil {
		n, err = io.Copy(f, r)
		if err != nil {
			return nil, n, err
		}

		_, err = f.Seek(0, io.SeekStart)
	}
	return f, n, err
}

type TempFile interface {
	io.ReadWriteCloser
	io.ReaderAt
	io.WriterAt
	io.Seeker
	Sync() error
}

type handle struct {
	C        *b2.RetryClient
	f        TempFile
	bucketID string
	key      string
}

var _ ninep.FileHandle = (*handle)(nil)

func (h *handle) ReadAt(p []byte, off int64) (n int, err error)  { return h.f.ReadAt(p, off) }
func (h *handle) WriteAt(p []byte, off int64) (n int, err error) { return h.f.WriteAt(p, off) }

func (h *handle) Sync() error {
	if err := h.f.Sync(); err != nil {
		return err
	}
	if _, err := h.f.Seek(0, io.SeekStart); err != nil {
		return err
	}
	// TODO: don't double write to files
	ctx := context.Background()
	_, err := h.C.UploadFile(ctx, h.bucketID, b2.UploadFileOptions{
		FileName:      h.key,
		ContentLength: b2.ContentLengthDetermineUsingTempStorage,
		Body:          h.f,
	})
	return err
}

func (h *handle) Close() error {
	if err := h.Sync(); err != nil {
		return err
	}
	return h.f.Close()
}
