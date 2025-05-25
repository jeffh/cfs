package cachefs

import (
	"context"
	"errors"
	"io"

	"github.com/jeffh/cfs/ninep"
)

type writeRequest struct {
	offset int64
	data   []byte
}

type handle struct {
	fs          *fsys
	h           ninep.FileHandle
	open        func() (ninep.FileHandle, error)
	path        string
	isWriteable bool
	cancel      context.CancelFunc
	closed      chan error
	wq          chan *writeRequest
}

func (h *handle) handle() (ninep.FileHandle, error) {
	var err error
	if h.h == nil {
		h.h, err = h.open()
	}
	return h.h, err
}

func (h *handle) ReadAt(p []byte, off int64) (int, error) {
	key := dataKey{path: h.path}
	left := p
	var n int
	cached, ok := h.fs.dataCache.Get(key)
	if ok {
		var needsRead bool
		var err error
		n, needsRead, err = cached.ReadAt(p, int64(off))
		if h.fs.logger != nil {
			h.fs.logger.Debug("CacheFS.handle.ReadAt.cacheHit", "path", h.path, "offset", off, "size", len(p), "read", n, "needsRead", needsRead, "err", err)
		}
		if !needsRead {
			return n, err
		}
		if err != nil {
			return n, err
		}
		left = p[n:]
	} else {
		cached = &dataValue{}
	}
	if h.fs.logger != nil {
		h.fs.logger.Debug("CacheFS.handle.ReadAt.cacheMiss", "path", h.path, "offset", off, "size", len(p))
	}
	var buf [4096]byte

	for len(left) > 0 {
		hdl, err := h.handle()
		if err != nil {
			return n, err
		}
		start := off + int64(n)
		size, err := hdl.ReadAt(buf[:], start)
		n += size
		copy(left, buf[:])
		left = left[size:]

		if size > 0 {
			block := block{offset: int64(start), data: make([]byte, size)}
			copy(block.data, buf[:size])
			if h.fs.logger != nil {
				h.fs.logger.Debug("CacheFS.handle.ReadAt.addBlock", "offset", block.offset, "len", len(block.data), "data", string(block.data))
			}
			cached.Add(block)
		}
		if errors.Is(err, io.EOF) {
			block := block{offset: int64(start), data: nil}
			if h.fs.logger != nil {
				h.fs.logger.Debug("CacheFS.handle.ReadAt.addBlock.EOF", "offset", block.offset)
			}
			cached.Add(block)
		}

		if err != nil {
			h.fs.dataCache.Add(key, cached)
			return n, err
		}
	}
	h.fs.dataCache.Add(key, cached)
	return n, nil
}

func (h *handle) WriteAt(p []byte, off int64) (int, error) {
	if cached, ok := h.fs.dataCache.Get(dataKey{path: h.path}); ok {
		if h.fs.logger != nil {
			h.fs.logger.Info("CacheFS.handle.WriteAt.fromCache", "path", h.path)
		}
		cached.Add(block{offset: int64(off), data: p})
	}
	hdl, err := h.handle()
	if err != nil {
		return 0, err
	}
	if h.isWriteable && h.fs.writesAsync {
		if h.cancel == nil {
			ctx, cancel := context.WithCancel(context.Background())
			h.cancel = cancel
			closed := make(chan error, 8)
			wq := make(chan *writeRequest, 8)
			go func() {
				for {
					select {
					case <-ctx.Done():
						close(wq)
						closed <- nil
						close(closed)
						return
					case req := <-wq:
						if req.data == nil {
							if err := hdl.Sync(); err != nil {
								closed <- err
							}
							continue
						}
						_, err := hdl.WriteAt(req.data, req.offset)
						if err != nil {
							closed <- err
						}
					}
				}
			}()
			h.wq = wq
			h.closed = closed
		}
		req := &writeRequest{offset: off, data: make([]byte, len(p))}
		copy(req.data, p)
		h.wq <- req
		return len(p), nil
	} else {
		n, err := hdl.WriteAt(p, off)
		return n, err
	}
}

func (h *handle) Sync() error {
	if h.h == nil {
		return nil
	}
	// we're making an educated guess that if it's read only, sync doesn't really matter much
	if !h.isWriteable {
		go func() { _ = h.h.Sync() }()
		return nil
	}
	if h.fs.writesAsync && h.cancel != nil {
		h.wq <- &writeRequest{data: nil}
		return <-h.closed
	}
	return h.h.Sync()
}

func (h *handle) Close() error {
	if h.h == nil {
		return nil
	}
	// we're making an educated guess that if it's read only, close doesn't really matter much
	if !h.isWriteable {
		go func() { _ = h.h.Close() }()
		return nil
	}
	if h.fs.writesAsync && h.cancel != nil {
		h.cancel()
		return <-h.closed
	}
	return h.h.Close()
}
