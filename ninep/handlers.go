package ninep

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

var (
	ErrWriteNotAllowed = errors.New("not allowed to write")
	ErrSeekNotAllowed  = errors.New("seeking is not allowed")
	ErrUnsupported     = errors.New("unsupported")
	ErrNotImplemented  = errors.New("not implemented")

	ErrChangeUidNotAllowed = errors.New("changing uid is not allowed by protocol")
	ErrBufferTooSmall      = errors.New("buffer too small")
)

////////////////////////////////////////////////

type directoryHandle struct {
	fs       FileSystem
	offset   int64
	index    int
	allFiles []Stat
	session  *Session
	path     string
	fetched  bool
}

func (h *directoryHandle) ReadAt(p []byte, offset int64) (int, error) {
	// TODO: we can return more than one directory entry if we know the maxMsgSize < stat sizes
	if offset == 0 {
		// reset
		h.offset = 0
		h.index = 0
		h.allFiles = h.allFiles[:0]
		h.fetched = false
	}
	if h.offset != offset {
		return 0, ErrSeekNotAllowed
	}
	if !h.fetched {
		entries, err := h.fs.ListDir(h.path)
		if err != nil {
			return 0, err
		}
		h.allFiles = make([]Stat, len(entries))
		for i, info := range entries {
			subpath := filepath.Join(h.path, info.Name())
			q := h.session.PutQid(subpath, ModeFromFileInfo(info).QidType(), versionFromFileInfo(info))
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
		fmt.Printf("%d < %d\n", len(p), size)
		// TODO: return nil one time to indicate too small of a read
		return 0, ErrBufferTooSmall
	}
	copy(p, next.Bytes())

	h.offset += int64(size)
	h.index++
	return size, nil
}

func (h *directoryHandle) WriteAt(p []byte, offset int64) (int, error) {
	return 0, ErrWriteNotAllowed
}

func (h *directoryHandle) Sync() error {
	return nil
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

func prepareFileInfoForStat(qid Qid, info os.FileInfo) (size int, name, uid, gid, muid string) {
	ownerInfo := func(v interface{}) (uid, gid, muid string, ok bool) {
		satisfactory := false
		if v, ok := v.(FileInfoUid); ok {
			satisfactory = true
			uid = v.Uid()
			muid = uid
		} else {
			return uid, gid, muid, satisfactory
		}
		if v, ok := v.(FileInfoGid); ok {
			gid = v.Gid()
		}
		if v, ok := v.(FileInfoMuid); ok {
			muid = v.Muid()
		}

		return uid, gid, muid, satisfactory
	}

	var ok bool
	if uid, gid, muid, ok = ownerInfo(info); !ok {
		uid, gid, muid, ok = ownerInfo(info.Sys())
	}

	// TODO: fix qidPath to not hard code to 0 as root
	if info.Name() == "/" || qid.Path() == 0 {
		name = "."
	} else {
		name = info.Name()
	}
	size = statSize(name, uid, gid, muid)
	return size, name, uid, gid, muid
}

func fillStatFromFileInfo(s Stat, qid Qid, info os.FileInfo) {
	size, name, uid, gid, muid := prepareFileInfoForStat(qid, info)
	if len(s) < size {
		panic("Given a smaller buffer than needed")
	}
	s.fill(name, uid, gid, muid)

	s.SetQid(qid)
	s.SetMode(ModeFromFileInfo(info))
	s.SetAtime(uint32(info.ModTime().Unix()))
	s.SetMtime(uint32(info.ModTime().Unix()))
	s.SetLength(uint64(info.Size()))
}

func fileInfoToStat(qid Qid, info os.FileInfo) Stat {
	_, name, uid, gid, muid := prepareFileInfoForStat(qid, info)
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

////////////////////////////////////////////////

type DefaultHandler struct {
	Fs   FileSystem
	Auth Authorizer
	st   SessionTracker

	Loggable
}

func (h *DefaultHandler) Connected(addr string) {
	h.st.Add(addr)
}

func (h *DefaultHandler) Disconnected(addr string) {
	h.Tracef("local: disconnect: %s", addr)
	h.st.Remove(addr)
}

func (h *DefaultHandler) Handle9P(ctx context.Context, m Message, w Replier) {
	session := h.st.Lookup(w.RemoteAddr())
	if session == nil {
		h.Errorf("No previous session for %s", w.RemoteAddr())
		return
	}
	switch m := m.(type) {
	case Tauth:
		if h.Auth == nil {
			h.Tracef("local: Tattach: unsupported")
			w.Rerror(ErrUnsupported)
		} else {
			handle, err := h.Auth.Auth(ctx, w.RemoteAddr(), m.Uname(), m.Aname())
			if err != nil {
				h.Tracef("local: Tattach: reject auth request: authorizer error: %s", err)
				w.Rerror(err)
				return
			}

			fil := serverFile{
				Name: fmt.Sprintf(".auth.%v", m.Afid()),
				User: m.Uname(),
				Flag: ORDWR | ORCLOSE,
				Mode: M_AUTH,
				H:    handle,
			}
			// fil.IncRef()
			session.PutFid(m.Afid(), fil)
			qid := session.PutQid(fil.Name, fil.Mode.QidType(), NoQidVersion)

			h.Tracef("local: Tattach: offer auth file: %v", fil.Name)
			w.Rauth(qid)
		}
		return

	case Tattach:
		if h.Auth != nil {
			if m.Afid() != NO_FID {
				fil, found := session.FileForFid(m.Afid())
				if !found {
					h.Tracef("local: Tattach: reject auth request")
					w.Rerrorf("no authentication")
					return
				}

				afid, ok := fil.H.(AuthFileHandle)
				if !ok {
					h.Tracef("local: Tattach: invalid afid (not auth file)")
					w.Rerrorf("unauthorized afid")
					return
				}

				if !afid.Authorized(m.Uname(), m.Aname()) {
					h.Tracef("local: Tattach: invalid afid (not authorized yet)")
					w.Rerrorf("unauthorized afid")
					return
				}

				h.Tracef("local: Tattach: authorized afid (%s)", afid)
			} else {
				h.Tracef("local: Tattach: reject auth request")
				w.Rerrorf("authentication required")
				return
			}
		} else {
			if m.Afid() != NO_FID {
				h.Tracef("local: Tattach: reject auth request")
				w.Rerrorf("no authentication")
				return
			} else {
				// we're ok
			}
		}

		// associate fid to root
		h.Tracef("local: Tattach: %s", m.Fid())
		session.PutFid(m.Fid(), serverFile{
			Name: "/",
			User: m.Uname(),
			Flag: OREAD,
			Mode: M_DIR,
		})
		w.Rattach(session.PutQid("", QT_DIR, NoQidVersion))

	case Topen:
		fil, ok := session.FileForFid(m.Fid())
		if !ok {
			h.Errorf("local: Topen: unknown %s", m.Fid())
			w.Rerrorf("unknown fid %d", m.Fid())
			return
		}
		fullPath := cleanPath(fil.Name)
		info, err := h.Fs.Stat(fullPath)
		if err != nil {
			h.Errorf("local: Topen: failed to call stat on %v: %s", fullPath, err)
			w.Rerrorf("file does not exist: %s", fullPath)
			return
		}
		q := session.PutQid(fil.Name, ModeFromFileInfo(info).QidType(), versionFromFileInfo(info))
		if info.IsDir() {
			// From Topen docs:
			//   "It is illegal to write a directory, truncate it, or attempt to remove it on close"
			if m.Mode()&ORCLOSE != 0 {
				h.Tracef("local: Topen: client error: requested dir with ORCLOSE")
				w.Rerrorf("dir cannot have ORCLOSE set")
				return
			} else if m.Mode()&OTRUNC != 0 {
				h.Tracef("local: Topen: client error: requested dir with OTRUNC")
				w.Rerrorf("dir cannot have OTRUNC set")
				return
			}
			fil.Mode = ModeFromFileInfo(info)
			fil.H = &directoryHandle{
				fs:      h.Fs,
				session: session,
				path:    fullPath,
			}
			// fil.IncRef()
		} else {
			f, err := h.Fs.OpenFile(fullPath, m.Mode())
			if err != nil || f == nil {
				h.Tracef("local: Topen: error opening file %v: %s %s", fullPath, err, m.Fid)
				w.Rerrorf("cannot open: %s", err)
				return
			}
			fil.Mode = ModeFromFileInfo(info)
			fil.Flag = m.Mode()
			fil.H = f
			// fil.IncRef()
			session.PutFileHandle(q, f)
		}
		h.Tracef("local: Topen: %s -> %v (isDir=%v, mode=%v, qid=%v)", m.Fid(), fullPath, info.IsDir(), info.Mode(), q)
		session.PutFid(m.Fid(), fil)
		w.Ropen(q, 0) // TODO: would be nice to support iounit

	case Twalk:
		fil, ok := session.FileForFid(m.Fid())
		if !ok {
			h.Errorf("local: Twalk: unknown %s", m.Fid())
			w.Rerrorf("unknown fid %d", m.Fid())
			return
		}
		if m.Fid() != m.NewFid() {
			if _, found := session.FileForFid(m.NewFid()); found {
				h.Errorf("local: Twalk: %s wanted new %s which is already taken", m.Fid(), m.NewFid())
				w.Rerrorf("conflict with newfid (fid=%d, newfid=%d)", m.Fid(), m.NewFid())
				return
			}
		}
		if fil.Mode&M_DIR == 0 && m.NumWname() > 0 {
			h.Errorf("local: Twalk: failed to because file is not a dir %s: %v", m.Fid(), fil.Name)
			w.Rerrorf("walk in non-directory")
			return
		}

		walkedQids := make([]Qid, 0, int(m.NumWname()))
		path := cleanPath(fil.Name)
		var info os.FileInfo
		for i, size := 0, int(m.NumWname()); i < size; i++ {
			// TODO: Wname(i) is O(n) which we could optimize for this case
			name := cleanPath(m.Wname(i))
			if name == "/" {
				path = "/"
			} else if strings.Contains(name, string(os.PathSeparator)) {
				h.Errorf("local: Twalk: invalid walk element for %s: %v", m.Fid(), name)
				w.Rerrorf("invalid walk element: %v", name)
				return
			} else {
				path = filepath.Join(path, name)
			}
			var err error
			info, err = h.Fs.Stat(path)
			if err != nil {
				h.Errorf("local: Twalk: failed to call stat on %v for %s", path, m.Fid())
				break
			}

			fil.Name = path

			h.Tracef("local: Twalk: %s :: %v %v", m.Fid(), fil.Name, info.Mode())
			if err == nil {
				q := session.PutQid(fil.Name, ModeFromOS(info.Mode()).QidType(), versionFromFileInfo(info))
				walkedQids = append(walkedQids, q)
			} else {
				q := session.PutQid(fil.Name, 0, versionFromFileInfo(info))
				walkedQids = append(walkedQids, q)
			}

			if fil.H != nil {
				fil.H.Close()
			}
			fil = serverFile{
				Name: path,
				Mode: ModeFromOS(info.Mode()),
			}
		}
		// From 9p docs:
		//   "If the full sequence of nwname elements is walked successfully,
		//   newfid will represent the file that results. If not, newfid (and
		//   fid) will be unaffected"
		if len(walkedQids) == int(m.NumWname()) {
			session.PutFid(m.NewFid(), fil)
		}
		h.Tracef("local: Twalk: %s=>%s: %v %v -> %v", m.Fid(), m.NewFid(), fil.Name, m.NumWname(), len(walkedQids))
		if len(walkedQids) > 0 {
			h.Tracef("local: Twalk: %s=>%s: qid=%v", m.Fid(), m.NewFid(), walkedQids[len(walkedQids)-1])
		}
		w.Rwalk(walkedQids)

	case Tstat:
		fil, ok := session.FileForFid(m.Fid())
		if !ok {
			h.Errorf("local: Tstat: unknown %s", m.Fid())
			w.Rerrorf("unknown fid %d", m.Fid())
			return
		}
		fullPath := cleanPath(fil.Name)
		info, err := h.Fs.Stat(fullPath)
		if err != nil {
			h.Errorf("local: Tstat: failed to call stat on %v: %s", fullPath, err)
			w.Rerror(err)
			return
		}
		qid := session.PutQid(fil.Name, ModeFromOS(info.Mode()).QidType(), versionFromFileInfo(info))
		stat := fileInfoToStat(qid, info)
		fil.User = stat.Uid()
		fil.Mode = ModeFromOS(info.Mode())
		session.PutFid(m.Fid(), fil)
		h.Tracef("local: Tstat %s %v -> %s %v", m.Fid(), fil.Name, stat, info.Mode())
		w.Rstat(stat)

	case Tread:
		fil, ok := session.FileForFid(m.Fid())
		if !ok {
			h.Errorf("local: Tread: unknown %s", m.Fid())
			w.Rerrorf("unknown fid %d", m.Fid())
			return
		}

		if fil.Flag&3 != OREAD && fil.Flag&3 != ORDWR {
			h.Errorf("local: Tread: file opened with wrong flags: %s", m.Fid())
			w.Rerrorf("%s wasn't opened with read flag set", m.Fid())
			return
		}

		if fil.H == nil {
			h.Errorf("local: Tread: file not opened: %s", m.Fid())
			w.Rerrorf("%s wasn't opened", m.Fid())
			return
		}
		data := w.RreadBuffer()
		// TODO: handle overflow of converting uint64 -> int64
		// TODO: handle retriable errors
		n, err := fil.H.ReadAt(data, int64(m.Offset()))
		if n == 0 {
			if err == io.EOF {
				w.Rread(nil)
				return
			}
			h.Errorf("local: Tread: error: fid %d couldn't read: %s", m.Fid(), err)
			w.Rerror(err)
			return
		} else if err != nil && err != io.EOF {
			h.Tracef("local: Tread: warn: %s couldn't read full buffer (only %d bytes): %s", m.Fid(), n, err)
		}
		h.Tracef("local: Tread: %s (offset=%d, bytes=%d)", m.Fid(), m.Offset(), n)
		w.Rread(data[:n])

	case Tclunk:
		fil, ok := session.FileForFid(m.Fid())
		if ok {
			if fil.H != nil {
				// if fil.DecRef() {
				fil.H.Close()
				if q, ok := session.Qid(fil.Name); ok {
					session.DeleteFileHandle(q)
				}
				fil.H = nil
				// }
			}
			if fil.Flag&ORCLOSE != 0 {
				h.Tracef("local: deleting %s %#v", m.Fid(), fil.Name)
				// delete the file
				h.Fs.Delete(fil.Name)
				session.DeleteQid(fil.Name)
			}
			session.DeleteFid(m.Fid())
			h.Tracef("local: Tclunk: %s %v", m.Fid(), fil.Name)
			w.Rclunk()
		} else {
			w.Rerrorf("local: Tclunk: unknown %s", m.Fid())
		}

	case Tremove:
		fil, ok := session.FileForFid(m.Fid())
		if ok {
			h.Fs.Delete(fil.Name)
			session.DeleteFid(m.Fid())
			session.DeleteQid(fil.Name)
			h.Tracef("local: Tremove: %v %v", m.Fid(), fil.Name)
			w.Rremove()
		} else {
			w.Rerrorf("local: Tremove: unknown fid %d", m.Fid())
		}

	case Tcreate:
		fil, ok := session.FileForFid(m.Fid())
		if !ok {
			h.Errorf("local: Tcreate: unknown fid: %v", m.Fid())
			w.Rerrorf("unknown fid %d", m.Fid())
			return
		}
		name := m.Name()
		if name == "." || name == ".." {
			h.Errorf("local: Tcreate: cannot make file: %v", name)
			w.Rerrorf("invalid name: %#v", name)
			return
		}
		fullPath := filepath.Join(fil.Name, cleanPath(m.Name()))
		info, err := h.Fs.Stat(fullPath)
		if os.IsExist(err) {
			h.Errorf("local: Tcreate: file exists %v: %s", fullPath, err)
			w.Rerror(err)
			return
		}
		isDir := m.Perm()&M_DIR != 0
		fil.Name = fullPath
		q := session.PutQid(fil.Name, m.Perm().QidType(), versionFromFileInfo(info))
		if isDir {
			// From Topen docs:
			//   "It is illegal to write a directory, truncate it, or attempt to remove it on close"
			if m.Mode()&ORCLOSE != 0 {
				h.Tracef("local: Tcreate: client error: requested dir with ORCLOSE")
				w.Rerrorf("dir cannot have ORCLOSE set")
				return
			} else if m.Mode()&OTRUNC != 0 {
				h.Tracef("local: Tcreate: client error: requested dir with OTRUNC")
				w.Rerrorf("dir cannot have OTRUNC set")
				return
			}

			if err := h.Fs.MakeDir(fullPath, m.Perm()); err != nil {
				h.Errorf("local: Tcreate: failed to create dir: %s", err)
				w.Rerror(err)
				return
			}
			fil.Flag = m.Mode()
			fil.Mode = m.Perm()
			fil.H = &directoryHandle{
				fs:      h.Fs,
				session: session,
				path:    fullPath,
			}
			// fil.IncRef()
		} else {
			f, err := h.Fs.CreateFile(fullPath, m.Mode(), m.Perm())
			if err != nil || f == nil {
				h.Tracef("local: Tcreate: error creating file %v %v: %s", fullPath, m.Perm().ToOsMode(), err)
				w.Rerror(err)
				return
			}
			fil.Flag = m.Mode()
			fil.Mode = m.Perm()
			fil.H = f
			// fil.IncRef()
			session.PutFileHandle(q, f)
		}
		if info != nil {
			fil.Mode = ModeFromOS(info.Mode())
		}
		h.Tracef("local: Tcreate: %v -> %v (isDir=%v, mode=%d)", m.Fid(), fil.Name, isDir, m.Mode())
		session.PutFid(m.Fid(), fil)
		w.Rcreate(q, 0) // TODO: would be nice to support iounit

	case Twrite:
		fil, ok := session.FileForFid(m.Fid())
		if !ok {
			h.Errorf("local: Twrite: unknown fid: %v", m.Fid())
			w.Rerrorf("unknown fid %d", m.Fid())
			return
		}

		if fil.Flag&3 != OWRITE && fil.Flag&3 != ORDWR {
			h.Errorf("local: Twrite: file opened with wrong flags: fid %v", m.Fid())
			w.Rerrorf("fid %d wasn't opened with write flag set", m.Fid())
			return
		}

		if fil.H == nil {
			h.Errorf("local: Twrite: file not opened: %v", m.Fid())
			w.Rerrorf("fid %d wasn't opened", m.Fid())
			return
		}
		data := m.Data()
		h.Tracef("local: Twrite: want fid %d (offset=%d, data=%d)", m.Fid(), m.Offset(), len(data))
		// TODO: handle overflow of converting uint64 -> int64
		// TODO: handle retriable errors
		n, err := fil.H.WriteAt(data, int64(m.Offset()))
		if n == 0 && err != nil {
			h.Errorf("local: Twrite: error: fid %d couldn't write: %s", m.Fid(), err)
			w.Rerrorf("failed to write to fid %d", m.Fid())
			return
		}
		session.TouchQid(fil.Name, fil.Mode.QidType())
		if err != nil {
			h.Tracef("local: Twrite: warn: fid %d couldn't write full buffer (only %d/%d bytes): %s", m.Fid(), n, len(data), err)
		}
		h.Tracef("local: Twrite: fid %d (offset=%d, bytes=%d)", m.Fid(), m.Offset(), n)
		w.Rwrite(uint32(n))

	case Twstat:
		fil, ok := session.FileForFid(m.Fid())
		if !ok {
			h.Errorf("local: Twstat: unknown fid: %v", m.Fid())
			w.Rerrorf("unknown fid %d", m.Fid())
			return
		}
		qid := session.TouchQid(fil.Name, fil.Mode.QidType())
		fullPath := cleanPath(fil.Name)
		stat := m.Stat()

		// From docs:
		// "As a special case, if all the elements of the directory entry in a
		// Twstat message are ``don't touch'' values, the server may interpret it
		// as a request to guarantee that the contents of the associated file are
		// committed to stable storage before the Rwstat message is returned.
		// (Consider the message to mean, ``make the state of the file exactly what
		// it claims to be.'')"
		if stat.IsZero() {
			if fil.H != nil {
				if err := fil.H.Sync(); err != nil {
					h.Errorf("local: Twstat: failed to fsync %v: %s", fullPath, err)
					w.Rerror(err)
					return
				}
			}
		} else {

			// From docs:
			//   "A wstat request can avoid modifying some properties of the file
			//   by providing explicit “don’t touch” values in the stat data that
			//   is sent: zero-length strings for text values and the maximum
			//   unsigned value of appropriate size for integral values. As a
			//   special case, if all the elements of the directory entry in a
			//   Twstat message are “don’t touch” values, the server may
			//   interpret it as a request to guarantee that the contents of the
			//   associated file are committed to stable storage before the
			//   Rwstat message is returned. (Consider the message to mean, “make
			//   the state of the file exactly what it claims to be.”)"

			if !stat.MuidNoTouch() {
				h.Errorf("local: Twstat: client failed: deny attempt to change Muid")
				w.Rerrorf("wstat: attempted to change Muid")
				return
			}

			if !stat.DevNoTouch() {
				h.Errorf("local: Twstat: client failed: deny attempt to change dev (%d != %d)", stat.Dev(), NoTouchU32)
				w.Rerrorf("wstat: attempted to change dev")
				return
			}

			if !stat.TypeNoTouch() {
				h.Errorf("local: Twstat: client failed: deny attempt to change type (%d)", stat.Type())
				w.Rerrorf("wstat: attempted to change type")
				return
			}

			if !stat.Qid().IsNoTouch() {
				h.Errorf("local: Twstat: client failed: deny attempt to change qid")
				w.Rerrorf("wstat: attempted to change qid")
				return
			}

			if !stat.ModeNoTouch() && int(stat.Mode()&M_DIR>>24) != int(qid.Type()&QT_DIR) {
				h.Errorf("local: Twstat: client failed: deny attempt to change M_DIR bit on Mode")
				w.Rerrorf("wstat: attempted to change Mode to M_DIR bit")
				return
			}

			err := h.Fs.WriteStat(fullPath, stat)
			if err != nil {
				h.Errorf("local: Twstat: failed for %v: %s", fullPath, err)
				w.Rerror(err)
				return
			}
		}
		// session.PutFid(m.Fid(), File{
		// 	Name: fil.Name,
		// 	User: fil.User,
		// 	Flag: fil.Flag,
		// 	Mode: ModeFromOS(m.Stat()),
		// 	H:    fil.H,
		// })
		// qid := session.PutQid(fil.Name, ModeFromOS(m.Mode()).QidType())
		h.Tracef("local: Twstat ->Rwstat %v %v -> %s", m.Fid(), fil.Name, stat)
		w.Rwstat()

	default:
		break // let default behavior run
	}
}
