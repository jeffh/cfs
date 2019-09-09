package ninep

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"sync"
)

var (
	errWriteNotAllowed = errors.New("not allowed to write")
	errSeekNotAllowed  = errors.New("seeking is not allowed")
)

type FileSystem interface {
	OpenFile(path string, flag OpenMode, mode Mode) (FileHandle, error)
	ListDir(path string) ([]os.FileInfo, error)
	Stat(path string) (os.FileInfo, error)
}

////////////////////////////////////////////////

type Dir string

func (d Dir) OpenFile(path string, flag OpenMode, mode Mode) (FileHandle, error) {
	fullPath := filepath.Join(string(d), path)
	return os.OpenFile(fullPath, flag.ToOsFlags(), mode.ToOsMode())
}

func (d Dir) ListDir(path string) ([]os.FileInfo, error) {
	fullPath := filepath.Join(string(d), path)
	return ioutil.ReadDir(fullPath)
}

func (d Dir) Stat(path string) (os.FileInfo, error) {
	fullPath := filepath.Join(string(d), path)
	fmt.Printf("Stat(%#v)\n", fullPath)
	return os.Stat(fullPath)
}

////////////////////////////////////////////////

type directoryHandle struct {
	fs       FileSystem
	offset   int64
	index    int
	allFiles []Stat
	qids     *QidPool
	path     string
	fetched  bool
}

func (h *directoryHandle) ReadAt(p []byte, offset int64) (int, error) {
	fmt.Printf("ReadAt([]byte{size=%d}, offset=%d)\n", len(p), offset)
	if offset == 0 {
		// reset
		h.offset = 0
		h.index = 0
		h.allFiles = h.allFiles[:0]
		h.fetched = false
	}
	if h.offset != offset {
		return 0, errSeekNotAllowed
	}
	if !h.fetched {
		entries, err := h.fs.ListDir(h.path)
		if err != nil {
			return 0, err
		}
		h.allFiles = make([]Stat, len(entries))
		for i, info := range entries {
			subpath := filepath.Join(h.path, info.Name())
			q := h.qids.Put(subpath, ModeFromOS(info.Mode()).QidType())
			st := fileInfoToStat(q, info)
			h.allFiles[i] = st
		}
		h.fetched = true
	}
	if h.index >= len(h.allFiles) {
		return 0, io.EOF
	}
	next := h.allFiles[h.index]
	size := next.Nbytes()
	if len(p) < size {
		return 0, errors.New("buffer too small")
	}
	copy(p, next.Bytes())

	h.offset += int64(size)
	h.index++
	return size, nil
}

func (h *directoryHandle) WriteAt(p []byte, offset int64) (int, error) {
	return 0, errWriteNotAllowed
}

func (h *directoryHandle) Close() error {
	// reset
	h.offset = 0
	h.index = 0
	h.allFiles = nil
	h.fetched = false
	return nil
}

////////////////////////////////////////////////
type readOnlyMemoryBuffer struct{ bytes.Reader }

func (b *readOnlyMemoryBuffer) WriteAt(p []byte, off int64) (n int, err error) {
	return 0, errWriteNotAllowed
}

func (b *readOnlyMemoryBuffer) Close() error { return nil }

////////////////////////////////////////////////

type FileHandle interface {
	io.ReaderAt
	io.WriterAt
	io.Closer
}

type file struct {
	name  string
	user  string
	flags OpenMode
	mode  Mode
	h     FileHandle
}

type UnauthenticatedHandler struct {
	Fs FileSystem

	ErrorLog, TraceLog Logger

	Qids *QidPool
	Fids *FidTracker
}

func prepareFileInfoForStat(info os.FileInfo) (size int, name, uid, gid, muid string) {
	ownerInfo := func(v interface{}) (uid, gid, muid string, ok bool) {
		type hasUid interface{ Uid() string }
		type hasGid interface{ Gid() string }
		type hasMuid interface{ Muid() string }

		satisfactory := false
		if v, ok := v.(hasUid); ok {
			satisfactory = true
			uid = v.Uid()
			muid = uid
		} else {
			return uid, gid, muid, satisfactory
		}
		if v, ok := v.(hasGid); ok {
			gid = v.Gid()
		}
		if v, ok := v.(hasMuid); ok {
			muid = v.Muid()
		}

		return uid, gid, muid, satisfactory
	}

	var ok bool
	if uid, gid, muid, ok = ownerInfo(info); !ok {
		uid, gid, muid, ok = ownerInfo(info.Sys())
	}

	if info.Name() == "/" {
		name = "."
	} else {
		name = info.Name()
	}
	size = statSize(name, uid, gid, muid)
	return size, name, uid, gid, muid
}

func fillStatFromFileInfo(s Stat, qid Qid, info os.FileInfo) {
	size, name, uid, gid, muid := prepareFileInfoForStat(info)
	if len(s) < size {
		panic("Given a smaller buffer than needed")
	}
	s.fill(name, uid, gid, muid)

	s.SetQid(qid)
	s.SetMode(ModeFromOS(info.Mode()))
	s.SetAtime(uint32(info.ModTime().Unix()))
	s.SetMtime(uint32(info.ModTime().Unix()))
	s.SetLength(uint64(info.Size()))
}

func fileInfoToStat(qid Qid, info os.FileInfo) Stat {
	_, name, uid, gid, muid := prepareFileInfoForStat(info)
	s := NewStat(name, uid, gid, muid)
	fillStatFromFileInfo(s, qid, info)
	return s
}

func cleanPath(path string) string {
	path = filepath.Clean(path)
	if !filepath.IsAbs(path) {
		path = filepath.Clean(string(os.PathSeparator) + path)
		// this can't fail: we're relative to root
		var err error
		path, err = filepath.Rel(string(os.PathSeparator), path)
		if err != nil {
			panic("unreachable")
		}
	}
	return filepath.Clean(path)
}

func (h *UnauthenticatedHandler) errorf(format string, values ...interface{}) {
	if h.ErrorLog != nil {
		h.ErrorLog.Printf(format, values...)
	}
}

func (h *UnauthenticatedHandler) tracef(format string, values ...interface{}) {
	if h.TraceLog != nil {
		h.TraceLog.Printf(format, values...)
	}
}

func (h *UnauthenticatedHandler) qidForFid(f Fid) (q Qid, found bool) {
	file, ok := h.Fids.Get(f)
	if !ok {
		found = false
		return
	}
	q, ok = h.Qids.Get(file.name)
	if !ok {
		found = false
	}
	return
}

func (h *UnauthenticatedHandler) Handle9P(ctx context.Context, m Message, w Replier) {
	switch m := m.(type) {
	case Tattach:
		if m.Afid() != NO_FID {
			h.tracef("local: Tattach: reject auth request")
			w.Rerror("no authentication")
			return
		}

		// associate fid to root
		h.tracef("local: Tattach: %v", m.Fid())
		h.Fids.Put(m.Fid(), file{
			name:  "",
			user:  m.Uname(),
			flags: OREAD,
			mode:  M_DIR,
		})
		w.Rattach(h.Qids.Put("", QT_DIR))

	case Topen:
		fil, ok := h.Fids.Get(m.Fid())
		if !ok {
			h.errorf("local: Topen: unknown fid: %v", m.Fid())
			w.Rerror("unknown fid %d", m.Fid())
			return
		}
		fullPath := cleanPath(fil.name)
		info, err := h.Fs.Stat(fullPath)
		if err != nil {
			h.errorf("local: Topen: failed to call stat on %v: %s", fullPath, err)
			w.Rerror("file does not exist: %s", fullPath)
			return
		}
		if info.IsDir() {
			// From Topen docs:
			//   "It is illegal to write a directory, truncate it, or attempt to remove it on close"
			if m.Mode()&ORCLOSE != 0 {
				h.tracef("local: Topen: client error: requested dir with ORCLOSE")
				w.Rerror("dir cannot have ORCLOSE set")
				return
			} else if m.Mode()&OTRUNC != 0 {
				h.tracef("local: Topen: client error: requested dir with OTRUNC")
				w.Rerror("dir cannot have OTRUNC set")
				return
			}
			fil.h = &directoryHandle{
				fs:   h.Fs,
				qids: h.Qids,
				path: fullPath,
			}
		} else {
			h.tracef("local: Topen: %v | %v (isDir=%v)", m.Fid(), fullPath, info.IsDir())
			f, err := h.Fs.OpenFile(fullPath, fil.flags, fil.mode)
			if err != nil {
				h.tracef("local: Topen: error opening file %v: %s", fullPath, err)
				w.Rerror("cannot open: %s", err)
				return
			}
			fil.h = f
		}
		h.tracef("local: Topen: %v -> %v %#v", m.Fid(), fil.name, fil.h)
		h.Fids.Put(m.Fid(), fil)
		q := h.Qids.Put(fil.name, ModeFromOS(info.Mode()).QidType())
		w.Ropen(q, 0) // TODO: would be nice to support iounit

	case Twalk:
		fil, ok := h.Fids.Get(m.Fid())
		if !ok {
			h.errorf("local: Twalk: unknown fid: %v", m.Fid())
			w.Rerror("unknown fid %d", m.Fid())
			return
		}
		if m.Fid() != m.NewFid() {
			if _, found := h.Fids.Get(m.NewFid()); found {
				h.errorf("local: Twalk: %v wanted new fid %v which is already taken", m.Fid(), m.NewFid())
				w.Rerror("conflict with newfid")
				return
			}
		}

		walkedQids := make([]Qid, 0, int(m.NumWname()))
		path := cleanPath(fil.name)
		for i, size := 0, int(m.NumWname()); i < size; i++ {
			// TODO: Wname(i) is O(n) which we could optimize for this case
			path = filepath.Join(path, cleanPath(m.Wname(i)))
			info, err := h.Fs.Stat(path)
			if err != nil {
				h.errorf("local: Twalk: failed to call stat on %v for fid %v", path, m.Fid())
				break
			}

			q := h.Qids.Put(fil.name, ModeFromOS(info.Mode()).QidType())
			walkedQids = append(walkedQids, q)
		}
		// From 9p docs:
		//   "If the full sequence of nwname elements is walked successfully,
		//   newfid will represent the file that results. If not, newfid (and
		//   fid) will be unaffected"
		if len(walkedQids) == int(m.NumWname()) {
			h.Fids.Put(m.NewFid(), file{
				name:  path,
				user:  fil.user,
				flags: OREAD,
				mode:  M_DIR,
			})
		}
		h.tracef("local: Twalk: %v: %v -> %v", m.Fid(), m.NumWname(), len(walkedQids))
		w.Rwalk(walkedQids)

	case Tstat:
		fil, ok := h.Fids.Get(m.Fid())
		if !ok {
			h.errorf("local: Tstat: unknown fid: %v", m.Fid())
			w.Rerror("unknown fid %d", m.Fid())
			return
		}
		fullPath := cleanPath(fil.name)
		info, err := h.Fs.Stat(fullPath)
		if err != nil {
			h.errorf("local: Tstat: failed to call stat on %v: %s", fullPath, err)
			w.Rerror("file does not exist: %s", fullPath)
			return
		}
		h.Fids.Put(m.Fid(), file{
			name:  fil.name,
			user:  fil.user,
			flags: fil.flags,
			mode:  ModeFromOS(info.Mode()),
			h:     fil.h,
		})
		qid := h.Qids.Put(fil.name, ModeFromOS(info.Mode()).QidType())
		stat := fileInfoToStat(qid, info)
		h.tracef("local: Tstat %v %v -> %s", m.Fid(), fil.name, stat)
		w.Rstat(stat)

	case Tread:
		fil, ok := h.Fids.Get(m.Fid())
		if !ok {
			h.errorf("local: Tread: unknown fid: %v", m.Fid())
			w.Rerror("unknown fid %d", m.Fid())
			return
		}

		if fil.h == nil {
			h.errorf("local: Tread: file not opened: %v", m.Fid())
			w.Rerror("fid %d wasn't opened", m.Fid())
			return
		}
		data := w.RreadBuffer()
		// TODO: handle overflow of converting uint64 -> int64
		// TODO: handle retriable errors
		n, err := fil.h.ReadAt(data, int64(m.Offset()))
		if n == 0 {
			if err == io.EOF {
				w.Rread(nil)
				return
			}
			h.errorf("local: Tread: error: fid %d couldn't read: %s", m.Fid(), err)
			w.Rerror("failed to read: %s", err)
			return
		} else if err != nil && err != io.EOF {
			h.tracef("local: Tread: warn: fid %d couldn't read full buffer (only %d bytes): %s", m.Fid(), n, err)
		}
		h.tracef("local: Tread: fid %d (offset=%d, bytes=%d)", m.Fid(), m.Offset(), n)
		w.Rread(data[:n])

	case Tclunk:
		fil, ok := h.Fids.Get(m.Fid())
		// TODO: handle if Delete on Close is set
		if ok {
			h.Fids.Delete(m.Fid())
		}
		w.Rclunk()
		h.tracef("local: Tclunk %v %v", m.Fid(), fil.name)

	default:
		break // let default behavior run
	}
}

type FidTracker struct {
	m    sync.Mutex
	fids map[Fid]file
}

func NewFidTracker() *FidTracker {
	return &FidTracker{
		fids: make(map[Fid]file),
	}
}

func (t *FidTracker) Get(f Fid) (h file, found bool) {
	t.m.Lock()
	h, found = t.fids[f]
	t.m.Unlock()
	return
}

func (t *FidTracker) Put(f Fid, h file) Fid {
	t.m.Lock()
	t.fids[f] = h
	t.m.Unlock()
	return f
}

func (t *FidTracker) Delete(f Fid) {
	t.m.Lock()
	delete(t.fids, f)
	t.m.Unlock()
}

///////////////////////////////////////////////////////

type QidPool struct {
	m        sync.Mutex
	pool     map[string]Qid
	nextPath uint64
}

func NewQidPool() *QidPool {
	return &QidPool{
		pool: make(map[string]Qid),
	}
}

func (p *QidPool) Get(name string) (q Qid, found bool) {
	p.m.Lock()
	q, found = p.pool[name]
	p.m.Unlock()
	return
}

func (p *QidPool) Put(name string, t QidType) Qid {
	var qid Qid
	p.m.Lock()
	if existing, ok := p.pool[name]; ok {
		qid = existing
	} else {
		qid = NewQid().Fill(t, 0, p.nextPath)
		p.nextPath++
		p.pool[name] = qid
	}
	p.m.Unlock()
	return qid
}

func (p *QidPool) Delete(name string) {
	p.m.Lock()
	delete(p.pool, name)
	p.m.Unlock()
}
