package cachefs

import (
	"io"
	"log/slog"
	"sync"
)

type dataKey struct {
	path string
}

type dataValue struct {
	mu     sync.Mutex
	blocks []block
}

func (db *dataValue) Truncate() {
	db.mu.Lock()
	defer db.mu.Unlock()
	db.blocks = db.blocks[:1]
	db.blocks[0].data = nil
	db.blocks[0].offset = 0
}

func (db *dataValue) Add(blk block) {
	db.mu.Lock()
	defer db.mu.Unlock()
	if blk.data == nil {
		if len(db.blocks) > 0 && db.blocks[len(db.blocks)-1].data == nil {
			return
		}
		db.blocks = append(db.blocks, blk)
		return
	}
	data := blk.data
	offset := blk.offset
	for _, b := range db.blocks {
		if b.offset <= offset {
			if offset <= b.offset+int64(len(b.data)) {
				copy(b.data[offset-b.offset:], data)
				slog.Info("CacheFS.dataValue.Add.extendBlock", "offset", offset, "len", len(data))
			}
		}
	}
	if len(data) > 0 {
		slog.Info("CacheFS.dataValue.Add.newBlock", "offset", offset, "len", len(data))
		b := block{offset, make([]byte, len(data))}
		copy(b.data, data)
		db.blocks = append(db.blocks, b)
	}
}

type block struct {
	offset int64
	data   []byte
}

func (dv *dataValue) ReadAt(p []byte, offset int64) (n int, needsUnderlyingRead bool, err error) {
	dv.mu.Lock()
	defer dv.mu.Unlock()
	// abort if offset is beyond the last readable block (denoted by a nil data pointer to indicate EOF)
	lastBlock := len(dv.blocks) - 1
	if lastBlock > 0 && dv.blocks[lastBlock].data == nil && offset >= dv.blocks[lastBlock].offset {
		return 0, false, io.EOF
	}
	fill := p
	for _, blk := range dv.blocks {
		if offset >= blk.offset && offset < blk.offset+int64(len(blk.data)) || len(fill) != len(p) {
			if blk.data == nil {
				return n, false, io.EOF
			}
			if offset < blk.offset+int64(len(blk.data)) {
				start := offset - blk.offset
				m := copy(fill, blk.data[start:])
				n += m
				fill = fill[m:]
				offset += int64(m)
				if len(fill) == 0 {
					break
				}
			} else {
				m := copy(fill, blk.data)
				n += m
				if m < len(blk.data) {
					return n, false, nil
				}
				fill = fill[m:]
				offset += int64(m)
				if len(fill) == 0 {
					break
				}
			}
		}
	}
	if n == 0 {
		return 0, false, io.EOF
	}
	if len(fill) > 0 {
		// partial read: we need to read more from the underlying file
		return n, true, nil
	}
	return n, false, nil
}
