package dockerfs

import (
	"io"
)

// PipeHandle implements a file handle that uses io.Pipe for streaming
type PipeHandle struct {
	r *io.PipeReader
	w *io.PipeWriter
}

func NewPipeHandle(r *io.PipeReader, w *io.PipeWriter) *PipeHandle {
	return &PipeHandle{r: r, w: w}
}

func (h *PipeHandle) ReadAt(p []byte, off int64) (n int, err error) {
	return h.r.Read(p)
}

func (h *PipeHandle) WriteAt(p []byte, off int64) (n int, err error) {
	return h.w.Write(p)
}

func (h *PipeHandle) Close() error {
	h.r.Close()
	return h.w.Close()
}

func (h *PipeHandle) Sync() error {
	return nil
}
