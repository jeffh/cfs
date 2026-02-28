package ninep

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"iter"
	"log/slog"
	"math"
	"os"
	"path/filepath"
	"strings"
)

type directoryHandle struct {
	offset int64
	index  int
	next   func() (fs.FileInfo, error, bool)
	stop   func()
	buffer []Stat
	rem    []Stat
	eof    bool

	// required for construction
	fs      FileSystem
	session *Session
	path    string
	ctx     context.Context
}

func (h *directoryHandle) ReadAt(p []byte, offset int64) (int, error) {
	// TODO: we can return more than one directory entry if we know the maxMsgSize < stat sizes
	if offset == 0 {
		// reset
		h.offset = 0
		h.index = 0
		h.eof = false
		h.rem = nil
		if h.stop != nil {
			h.stop()
			it := h.fs.ListDir(h.ctx, h.path)
			h.next, h.stop = iter.Pull2(it)
		}
	}
	if h.offset != offset {
		return 0, ErrSeekNotAllowed
	}
	if h.next == nil {
		it := h.fs.ListDir(h.ctx, h.path)
		if it == nil {
			panic("nil iterator")
		}
		h.next, h.stop = iter.Pull2(it)
		h.buffer = make([]Stat, 32)
		h.rem = h.buffer[:0]
	}
	if len(h.rem) == 0 {
		if h.eof {
			return 0, io.EOF
		}
		// TODO: is this a good idea to clear qids to avoid a memory leak?
		// Pre-compute path prefix to avoid repeated filepath.Join allocations
		pathPrefix := h.path + string(filepath.Separator)
		for _, info := range h.buffer {
			if info != nil {
				h.session.MayDeleteQid(pathPrefix + info.Name())
			}
		}

		h.rem = h.buffer[:0]
		for i, c := 0, cap(h.buffer); i < c; i++ {
			info, err, ok := h.next()
			if ok {
				if info != nil {
					subpath := pathPrefix + info.Name()
					q := h.session.PutQidInfo(subpath, info)
					st := fileInfoToStat(q, info)
					h.rem = append(h.rem, st)
				}
				if err != nil {
					return 0, err
				}
			} else {
				h.eof = true
				if len(h.rem) == 0 {
					return 0, io.EOF
				}
				break
			}
		}
	}
	next := h.rem[0]
	h.rem = h.rem[1:]
	size := next.Nbytes()
	if len(p) < size {
		// fmt.Printf("%d < %d\n", len(p), size)
		return 0, io.ErrShortBuffer
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
	// TODO: is this a good idea to clear qids to avoid a memory leak?
	if len(h.buffer) > 0 {
		pathPrefix := h.path + string(filepath.Separator)
		for _, info := range h.buffer {
			if info != nil {
				h.session.MayDeleteQid(pathPrefix + info.Name())
			}
		}
	}
	// reset
	if h.stop != nil {
		h.stop()
	}
	h.offset = 0
	h.index = 0
	h.next = nil
	h.stop = nil
	h.rem = nil
	h.buffer = nil
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
	if !ok {
		return size, name, "", "", ""
	}
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

// Default implementation to manage the server's part of the 9p protocol.
//
// Manages sessions from multiple clients.
//
// Delegates the actual file manage to FileSystem interface.
// Delegates auth to the Authorizer
//
// If the FileSystem implements io.Closer, then the defaultHandler will also
// invoke Close() when the server is shutting down.
type defaultHandler struct {
	Fs     FileSystem
	Auth   Authorizer
	st     SessionTracker
	Logger *slog.Logger
}

// Invoked by the server when the server is shutting down.
func (h *defaultHandler) Shutdown() {
	if closer, ok := h.Fs.(io.Closer); ok {
		_ = closer.Close()
	}
}

// Invoked by the server when a client connection is accepted and the basic 9p
// version protocol is negotiated
func (h *defaultHandler) Connected(addr string) {
	h.st.Add(addr)
}

// Invoked by the server when a client has been disconnected.
func (h *defaultHandler) Disconnected(addr string) {
	if h.Logger != nil {
		h.Logger.DebugContext(context.Background(), "disconnect", slog.String("addr", addr))
	}
	h.st.Remove(addr)
}

// Invoked by the server to handle a 9p protocol message
func (h *defaultHandler) Handle9P(connCtx, ctx context.Context, m Message, w Replier) {
	session := h.st.Lookup(w.RemoteAddr())
	if session == nil {
		if h.Logger != nil {
			h.Logger.ErrorContext(ctx, "no previous session for address", slog.String("addr", w.RemoteAddr()))
		}
		return
	}
	connCtx = context.WithValue(connCtx, SessionKey, session)
	connCtx = context.WithValue(connCtx, RawMessageKey, m)
	ctx = context.WithValue(ctx, SessionKey, session)
	ctx = context.WithValue(ctx, RawMessageKey, m)
	switch m := m.(type) {
	case Tauth:
		if h.Auth == nil {
			if h.Logger != nil {
				h.Logger.ErrorContext(ctx, "Tattach: unsupported", slog.String("addr", w.RemoteAddr()))
			}
			w.Rerror(ErrUnsupported)
		} else {
			handle, err := h.Auth.Auth(ctx, w.RemoteAddr(), m.Uname(), m.Aname())
			if err != nil {
				if h.Logger != nil {
					h.Logger.WarnContext(ctx, "Tattach: reject auth request: authorizer error", slog.String("addr", w.RemoteAddr()), slog.String("error", err.Error()))
				}
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

			if h.Logger != nil {
				h.Logger.DebugContext(ctx, "Tattach: offer auth file", slog.String("file", fil.Name))
			}
			w.Rauth(qid)
		}
		return

	case Tattach:
		if h.Auth != nil {
			if m.Afid() != NO_FID {
				fil, found := session.FileForFid(m.Afid())
				if !found {
					if h.Logger != nil {
						h.Logger.ErrorContext(ctx, "Tattach: reject auth request", slog.String("addr", w.RemoteAddr()))
					}
					w.Rerrorf("no authentication")
					return
				}

				afid, ok := fil.H.(AuthFileHandle)
				if !ok {
					if h.Logger != nil {
						h.Logger.ErrorContext(ctx, "Tattach: invalid afid (not auth file)", slog.String("addr", w.RemoteAddr()))
					}
					w.Rerrorf("unauthorized afid")
					return
				}

				if !afid.Authorized(m.Uname(), m.Aname()) {
					if h.Logger != nil {
						h.Logger.ErrorContext(ctx, "Tattach: invalid afid (not authorized yet)", slog.String("addr", w.RemoteAddr()))
					}
					w.Rerrorf("unauthorized afid")
					return
				}

				if h.Logger != nil {
					h.Logger.DebugContext(ctx, "Tattach: authorized afid", slog.Uint64("afid", uint64(m.Afid())))
				}
			} else {
				if h.Logger != nil {
					h.Logger.ErrorContext(ctx, "Tattach: reject auth request", slog.String("addr", w.RemoteAddr()))
				}
				w.Rerrorf("authentication required")
				return
			}
		} else {
			if m.Afid() != NO_FID {
				if h.Logger != nil {
					h.Logger.ErrorContext(ctx, "Tattach: reject auth request", slog.String("addr", w.RemoteAddr()))
				}
				w.Rerrorf("no authentication")
				return
			}
		}

		// associate fid to root
		if h.Logger != nil {
			h.Logger.DebugContext(ctx, "Tattach: associate fid to root",
				slog.String("addr", w.RemoteAddr()),
				slog.Uint64("fid", uint64(m.Fid())))
		}
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
			if h.Logger != nil {
				h.Logger.ErrorContext(ctx, "Topen: unknown fid",
					slog.String("addr", w.RemoteAddr()),
					slog.Uint64("fid", uint64(m.Fid())))
			}
			w.Rerrorf("unknown fid %d", m.Fid())
			return
		}
		fullPath := cleanPath(fil.Name)
		// fmt.Printf("==> Topen: START\n")
		info, err := h.Fs.Stat(ctx, fullPath)
		if err != nil {
			if h.Logger != nil {
				h.Logger.ErrorContext(ctx, "Topen: failed to call stat",
					slog.String("addr", w.RemoteAddr()),
					slog.String("path", fullPath),
					slog.String("error", err.Error()))
			}
			w.Rerror(err)
			return
		}
		q := session.PutQidInfo(fil.Name, info)
		// fmt.Printf("==> Topen: %s\n", strFileInfo(info))
		if info.IsDir() {
			// From Topen docs:
			//   "It is illegal to write a directory, truncate it, or attempt to remove it on close"
			if m.Mode()&ORCLOSE != 0 {
				if h.Logger != nil {
					h.Logger.WarnContext(ctx, "Topen: client error: requested dir with ORCLOSE", slog.String("addr", w.RemoteAddr()), slog.String("path", fullPath))
				}
				w.Rerrorf("dir cannot have ORCLOSE set")
				return
			} else if m.Mode()&OTRUNC != 0 {
				if h.Logger != nil {
					h.Logger.WarnContext(ctx, "Topen: client error: requested dir with OTRUNC", slog.String("addr", w.RemoteAddr()), slog.String("path", fullPath))
				}
				w.Rerrorf("dir cannot have OTRUNC set")
				return
			}
			fil.Mode = ModeFromFileInfo(info)
			fil.H = &directoryHandle{
				fs:      h.Fs,
				session: session,
				path:    fullPath,
				ctx:     connCtx,
			}
			// fil.IncRef()
		} else {
			f, err := h.Fs.OpenFile(connCtx, fullPath, m.Mode())
			if err != nil || f == nil {
				if h.Logger != nil {
					h.Logger.Error("Topen: error opening file", slog.String("addr", w.RemoteAddr()), slog.String("path", fullPath), slog.Any("error", err))
				}
				w.Rerrorf("cannot open: %s", err)
				return
			}
			fil.Mode = ModeFromFileInfo(info)
			fil.Flag = m.Mode()
			fil.H = f
			// fil.IncRef()
			session.PutFileHandle(q, f)
		}
		if h.Logger != nil {
			h.Logger.DebugContext(ctx, "Topen",
				slog.String("addr", w.RemoteAddr()),
				slog.String("path", fullPath),
				slog.String("qid", q.String()))
		}
		session.PutFid(m.Fid(), fil)
		w.Ropen(q, 0) // TODO: would be nice to support iounit

	case Twalk:
		fil, ok := session.FileForFid(m.Fid())
		if !ok {
			if h.Logger != nil {
				h.Logger.ErrorContext(ctx, "unknown fid",
					slog.String("addr", w.RemoteAddr()),
					slog.Uint64("fid", uint64(m.Fid())))
			}
			w.Rerrorf("unknown fid %d", m.Fid())
			return
		}
		if m.Fid() != m.NewFid() {
			if _, found := session.FileForFid(m.NewFid()); found {
				if h.Logger != nil {
					h.Logger.ErrorContext(ctx, "newfid already taken",
						slog.Uint64("fid", uint64(m.Fid())),
						slog.Uint64("newfid", uint64(m.NewFid())))
				}
				w.Rerrorf("conflict with newfid (fid=%d, newfid=%d)", m.Fid(), m.NewFid())
				return
			}
		}
		if fil.Mode&M_DIR == 0 && m.NumWname() > 0 {
			if h.Logger != nil {
				h.Logger.ErrorContext(ctx, "walk in non-directory",
					slog.Uint64("fid", uint64(m.Fid())),
					slog.String("name", fil.Name))
			}
			w.Rerrorf("walk in non-directory")
			return
		}

		walkedQids := make([]Qid, 0, int(m.NumWname()))
		path := cleanPath(fil.Name)
		var info os.FileInfo
		if wfs, ok := h.Fs.(WalkableFileSystem); ok {
			size := int(m.NumWname())
			parts := make([]string, 0, size)
			if h.Logger != nil {
				h.Logger.DebugContext(ctx, "using WalkableFileSystem", slog.Int("size", size))
			}
			for _, uncleanName := range m.Wnames() {
				if h.Logger != nil {
					h.Logger.DebugContext(ctx, "walk path", slog.String("path", uncleanName))
				}
				name := cleanPath(uncleanName)
				if strings.Contains(name, string(os.PathSeparator)) {
					if h.Logger != nil {
						h.Logger.ErrorContext(ctx, "invalid walk element",
							slog.Uint64("fid", uint64(m.Fid())),
							slog.String("name", name))
					}
					w.Rerrorf("invalid walk element: %v", name)
					return
				}
				parts = append(parts, name)
			}
			infos, err := wfs.Walk(ctx, parts)
			if err != nil {
				if h.Logger != nil {
					h.Logger.ErrorContext(ctx,
						"invalid file system walk",
						slog.Uint64("fid", uint64(m.Fid())),
						slog.String("path", filepath.Join(parts...)),
						slog.Any("error", err))
				}
				w.Rerror(err)
				return
			}
			if len(parts) > 1 && parts[len(parts)-1] == "." && len(infos) == len(parts)-1 {
				infos = append(infos, infos[len(infos)-1])
			}
			size = len(infos)
			for i, name := range parts {
				if i >= size {
					break
				}

				if name == "/" {
					path = "/"
				} else {
					path = filepath.Join(path, name)
				}

				info := infos[i]

				if info != nil {
					if h.Logger != nil {
						h.Logger.DebugContext(ctx, "walk info",
							slog.Uint64("fid", uint64(m.Fid())),
							slog.String("path", path),
							slog.Int("mode", int(info.Mode()&M_DIR>>24)))
					}
				} else {
					if h.Logger != nil {
						h.Logger.ErrorContext(ctx, "implementation returned a nil info without an error; returning not found")
					}
					w.Rerror(os.ErrNotExist)
					return
				}

				q := session.PutQidInfo(path, info)
				walkedQids = append(walkedQids, q)

				if fil.H != nil {
					_ = fil.H.Close()
				}
				fil = serverFile{
					Name: path,
					Mode: ModeFromFS(info.Mode()),
				}
			}
		} else {
			if h.Logger != nil {
				h.Logger.DebugContext(ctx, "not using WalkableFileSystem")
			}
			for _, uncleanName := range m.Wnames() {
				name := cleanPath(uncleanName)
				if name == "/" {
					path = "/"
				} else if strings.Contains(name, string(os.PathSeparator)) {
					if h.Logger != nil {
						h.Logger.ErrorContext(ctx, "invalid walk element",
							slog.Uint64("fid", uint64(m.Fid())),
							slog.String("name", name))
					}
					w.Rerrorf("invalid walk element: %v", name)
					return
				} else {
					path = filepath.Join(path, name)
				}
				var err error
				info, err = h.Fs.Stat(ctx, path)
				if err != nil {
					if h.Logger != nil {
						h.Logger.ErrorContext(ctx, "failed to call stat", "path", path, "fid", m.Fid(), "err", err)
					}
					break
				}

				if info != nil {
					if h.Logger != nil {
						h.Logger.DebugContext(ctx, "walk info",
							slog.Uint64("fid", uint64(m.Fid())),
							slog.String("path", path),
							slog.Int("mode", int(info.Mode()&M_DIR>>24)))
					}
				} else {
					if h.Logger != nil {
						h.Logger.ErrorContext(ctx, "implementation returned a nil info without an error; returning not found")
					}
					w.Rerror(os.ErrNotExist)
					return
				}

				q := session.PutQidInfo(path, info)
				walkedQids = append(walkedQids, q)

				if fil.H != nil {
					_ = fil.H.Close()
				}
				fil = serverFile{
					Name: path,
					Mode: ModeFromFS(info.Mode()),
				}
			}
		}
		// From 9p docs:
		//   "If the full sequence of nwname elements is walked successfully,
		//   newfid will represent the file that results. If not, newfid (and
		//   fid) will be unaffected"
		if len(walkedQids) == int(m.NumWname()) {
			session.PutFid(m.NewFid(), fil)
		}
		if h.Logger != nil {
			h.Logger.DebugContext(ctx, "walk result",
				slog.Uint64("fid", uint64(m.Fid())),
				slog.Uint64("newfid", uint64(m.NewFid())),
				slog.String("file", fil.Name),
				slog.Int("num_wname", int(m.NumWname())),
				slog.Int("walked_qids", len(walkedQids)))
		}
		if len(walkedQids) > 0 {
			if h.Logger != nil {
				h.Logger.DebugContext(ctx, "walk result",
					slog.Uint64("fid", uint64(m.Fid())),
					slog.Uint64("newfid", uint64(m.NewFid())),
					slog.String("qid", walkedQids[len(walkedQids)-1].String()))
			}
		}
		w.Rwalk(walkedQids)

	case Tstat:
		fil, ok := session.FileForFid(m.Fid())
		if !ok {
			if h.Logger != nil {
				h.Logger.ErrorContext(ctx, "unknown fid",
					slog.String("addr", w.RemoteAddr()),
					slog.Uint64("fid", uint64(m.Fid())))
			}
			w.Rerrorf("unknown fid %d", m.Fid())
			return
		}
		fullPath := cleanPath(fil.Name)
		info, err := h.Fs.Stat(ctx, fullPath)
		if err != nil {
			if h.Logger != nil {
				h.Logger.Error("failed to call stat", "fullPath", fullPath, "err", err)
			}
			w.Rerror(err)
			return
		}
		qid := session.PutQidInfo(fil.Name, info)
		stat := fileInfoToStat(qid, info)
		fil.User = stat.Uid()
		fil.Mode = ModeFromFS(info.Mode())
		session.PutFid(m.Fid(), fil)
		if h.Logger != nil {
			h.Logger.DebugContext(ctx, "stat result",
				slog.Uint64("fid", uint64(m.Fid())),
				slog.String("file", fil.Name),
				slog.String("stat", stat.String()))
		}
		w.Rstat(stat)

	case Tread:
		fil, ok := session.FileForFid(m.Fid())
		if !ok {
			if h.Logger != nil {
				h.Logger.ErrorContext(ctx, "unknown fid",
					slog.String("addr", w.RemoteAddr()),
					slog.Uint64("fid", uint64(m.Fid())))
			}
			w.Rerrorf("unknown fid %d", m.Fid())
			return
		}

		if fil.Flag&3 != OREAD && fil.Flag&3 != ORDWR {
			if h.Logger != nil {
				h.Logger.ErrorContext(ctx, "file opened with wrong flags",
					slog.Uint64("fid", uint64(m.Fid())))
			}
			w.Rerrorf("%s wasn't opened with read flag set", m.Fid())
			return
		}

		if fil.H == nil {
			if h.Logger != nil {
				h.Logger.ErrorContext(ctx, "file not opened",
					slog.Uint64("fid", uint64(m.Fid())))
			}
			w.Rerrorf("%s wasn't opened", m.Fid())
			return
		}
		data := w.RreadBuffer()
		{
			readSize := m.Count()
			dataSize := len(data)
			if int(readSize) > dataSize {
				readSize = uint32(dataSize)
			}
			data = data[:readSize]
		}
		// TODO: handle overflow of converting uint64 -> int64
		// TODO: handle retriable errors
		n, err := fil.H.ReadAt(data, int64(m.Offset()))
		if n == 0 {
			if err == io.EOF || err == nil {
				w.Rread(0)
				if h.Logger != nil {
					h.Logger.DebugContext(ctx, "read EOF", slog.Uint64("fid", uint64(m.Fid())))
				}
				return
			}
			if h.Logger != nil {
				h.Logger.ErrorContext(ctx, "error",
					slog.Uint64("fid", uint64(m.Fid())),
					slog.String("error", err.Error()))
			}
			w.Rerror(err)
			return
		} else if err != nil && err != io.EOF {
			if h.Logger != nil {
				h.Logger.ErrorContext(ctx, "warn",
					slog.Uint64("fid", uint64(m.Fid())),
					slog.String("error", err.Error()))
			}
		}
		if h.Logger != nil {
			h.Logger.DebugContext(ctx, "read result",
				slog.Uint64("fid", uint64(m.Fid())),
				slog.Uint64("offset", m.Offset()),
				slog.Int("bytes", n))
		}
		if uint64(len(data)) > math.MaxUint32 {
			panic(fmt.Errorf("data is larger than allowed: %d", len(data)))
		}
		if uint64(n) > math.MaxUint32 {
			panic(fmt.Errorf("read data is larger than allowed: %d", n))
		}
		w.Rread(uint32(n))

	case Tclunk:
		fil, ok := session.FileForFid(m.Fid())
		if ok {
			if fil.H != nil {
				// if fil.DecRef() {
				_ = fil.H.Close()
				if q, ok := session.Qid(fil.Name); ok {
					session.DeleteFileHandle(q)
				}
				fil.H = nil
				// }
			}
			if fil.Flag&ORCLOSE != 0 {
				if h.Logger != nil {
					h.Logger.DebugContext(ctx, "deleting",
						slog.Uint64("fid", uint64(m.Fid())),
						slog.String("file", fil.Name))
				}
				if fsd, ok := h.Fs.(DeleteWithModeFileSystem); ok {
					_ = fsd.DeleteWithMode(ctx, fil.Name, fil.Mode)
				} else {
					// delete the file
					_ = h.Fs.Delete(ctx, fil.Name)
				}
			}
			session.MayDeleteQid(fil.Name) // not ideal, since there may be other active references! but this is better then leaking memory?
			session.DeleteFid(m.Fid())
			if h.Logger != nil {
				h.Logger.DebugContext(ctx, "Tclunk",
					slog.Uint64("fid", uint64(m.Fid())),
					slog.String("file", fil.Name))
			}
			w.Rclunk()
		} else {
			w.Rerrorf("srv: Tclunk: unknown %s", m.Fid())
		}

	case Tremove:
		fil, ok := session.FileForFid(m.Fid())
		if ok {
			if h.Logger != nil {
				h.Logger.DebugContext(ctx, "Tremove",
					slog.Uint64("fid", uint64(m.Fid())),
					slog.String("file", fil.Name))
			}
			err := Delete(ctx, h.Fs, fil.Name, fil.Mode)
			session.DeleteFid(m.Fid())
			session.DeleteQid(fil.Name)
			if err != nil {
				if h.Logger != nil {
					h.Logger.Error("error", slog.Uint64("fid", uint64(m.Fid())), slog.String("file", fil.Name), slog.String("error", err.Error()))
				}
				w.Rerror(err)
			} else {
				w.Rremove()
			}
		} else {
			w.Rerrorf("srv: Tremove: unknown fid %d", m.Fid())
		}

	case Tcreate:
		fil, ok := session.FileForFid(m.Fid())
		if !ok {
			if h.Logger != nil {
				h.Logger.Error("unknown fid", slog.Uint64("fid", uint64(m.Fid())))
			}
			w.Rerrorf("unknown fid %d", m.Fid())
			return
		}
		name := m.Name()
		if name == "." || name == ".." {
			if h.Logger != nil {
				h.Logger.Error("cannot create file", slog.String("file", name))
			}
			w.Rerrorf("invalid name: %#v", name)
			return
		}
		fullPath := filepath.Join(fil.Name, cleanPath(m.Name()))
		info, err := h.Fs.Stat(ctx, fullPath)
		if errors.Is(err, os.ErrExist) {
			if h.Logger != nil {
				h.Logger.Error("file exists", slog.String("file", fullPath), slog.String("error", err.Error()))
			}
			w.Rerror(err)
			return
		}
		isDir := m.Perm()&M_DIR != 0
		fil.Name = fullPath
		var q Qid
		if in, ok := info.(FileInfoPath); ok {
			q = session.PutQidDirect(fil.Name, in.Path(), m.Perm().QidType(), versionFromFileInfo(info))
		} else {
			q = session.PutQid(fil.Name, m.Perm().QidType(), versionFromFileInfo(info))
		}
		if isDir {
			// From Topen docs:
			//   "It is illegal to write a directory, truncate it, or attempt to remove it on close"
			if m.Mode()&ORCLOSE != 0 {
				if h.Logger != nil {
					h.Logger.Error("client error: requested dir with ORCLOSE")
				}
				w.Rerrorf("dir cannot have ORCLOSE set")
				return
			} else if m.Mode()&OTRUNC != 0 {
				if h.Logger != nil {
					h.Logger.Error("client error: requested dir with OTRUNC")
				}
				w.Rerrorf("dir cannot have OTRUNC set")
				return
			}

			if err := h.Fs.MakeDir(ctx, fullPath, m.Perm()); err != nil {
				if h.Logger != nil {
					h.Logger.Error("failed to create dir", slog.String("file", fullPath), slog.String("error", err.Error()))
				}
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
			session.PutFileHandle(q, fil.H)
		} else {
			f, err := h.Fs.CreateFile(connCtx, fullPath, m.Mode(), m.Perm())
			if err != nil || f == nil {
				if h.Logger != nil {
					h.Logger.Error("error creating file", slog.String("file", fullPath), slog.String("mode", m.Perm().String()), slog.String("error", err.Error()))
				}
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
			fil.Mode = ModeFromFS(info.Mode())
		}
		if h.Logger != nil {
			h.Logger.Info("Tcreate", slog.Uint64("fid", uint64(m.Fid())), slog.String("file", fil.Name), slog.Bool("isDir", isDir), slog.String("mode", m.Mode().String()))
		}
		session.PutFid(m.Fid(), fil)
		w.Rcreate(q, 0) // TODO: would be nice to support iounit

	case Twrite:
		fil, ok := session.FileForFid(m.Fid())
		if !ok {
			if h.Logger != nil {
				h.Logger.Error("unknown fid", slog.Uint64("fid", uint64(m.Fid())))
			}
			w.Rerrorf("unknown fid %d", m.Fid())
			return
		}

		if fil.Flag&3 != OWRITE && fil.Flag&3 != ORDWR {
			if h.Logger != nil {
				h.Logger.Error("file opened with wrong flags", slog.Uint64("fid", uint64(m.Fid())))
			}
			w.Rerrorf("fid %d wasn't opened with write flag set", m.Fid())
			return
		}

		if fil.H == nil {
			if h.Logger != nil {
				h.Logger.Error("file not opened",
					slog.Uint64("fid", uint64(m.Fid())))
			}
			w.Rerrorf("fid %d wasn't opened", m.Fid())
			return
		}

		data := m.Data()
		if h.Logger != nil {
			h.Logger.DebugContext(ctx, "write request",
				slog.Uint64("fid", uint64(m.Fid())),
				slog.Uint64("offset", m.Offset()),
				slog.Int("data_len", len(data)))
		}
		// TODO: handle overflow of converting uint64 -> int64
		// TODO: handle retriable errors
		n, err := fil.H.WriteAt(data, int64(m.Offset()))
		if n == 0 && err != nil {
			if h.Logger != nil {
				h.Logger.ErrorContext(ctx, "error",
					slog.Uint64("fid", uint64(m.Fid())),
					slog.String("error", err.Error()))
			}
			w.Rerror(err)
			return
		}
		session.TouchQid(fil.Name, fil.Mode.QidType())
		if h.Logger != nil {
			if err != nil {
				h.Logger.ErrorContext(ctx, "warn",
					slog.Uint64("fid", uint64(m.Fid())),
					slog.String("error", err.Error()))
			} else {
				h.Logger.DebugContext(ctx, "write result",
					slog.Uint64("fid", uint64(m.Fid())),
					slog.Uint64("offset", m.Offset()),
					slog.Int("bytes", n))
			}
		}
		w.Rwrite(uint32(n))

	case Twstat:
		fil, ok := session.FileForFid(m.Fid())
		if !ok {
			if h.Logger != nil {
				h.Logger.ErrorContext(ctx, "unknown fid",
					slog.String("addr", w.RemoteAddr()),
					slog.Uint64("fid", uint64(m.Fid())))
			}
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
		if stat.IsNoTouch() {
			if fil.H != nil {
				if err := fil.H.Sync(); err != nil {
					if h.Logger != nil {
						h.Logger.Error("failed to fsync %v: %s", fullPath, err)
					}
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
				if h.Logger != nil {
					h.Logger.ErrorContext(ctx, "client failed: deny attempt to change Muid")
				}
				w.Rerrorf("wstat: attempted to change Muid")
				return
			}

			if !stat.DevNoTouch() {
				if h.Logger != nil {
					h.Logger.ErrorContext(ctx, "client failed: deny attempt to change dev",
						slog.Uint64("dev", uint64(stat.Dev())),
						slog.Uint64("expected", uint64(NoTouchU32)))
				}
				w.Rerrorf("wstat: attempted to change dev")
				return
			}

			if !stat.TypeNoTouch() {
				if h.Logger != nil {
					h.Logger.ErrorContext(ctx, "client failed: deny attempt to change type",
						slog.Uint64("type", uint64(stat.Type())))
				}
				w.Rerrorf("wstat: attempted to change type")
				return
			}

			if !stat.Qid().IsNoTouch() {
				if h.Logger != nil {
					h.Logger.ErrorContext(ctx, "client failed: deny attempt to change qid")
				}
				w.Rerrorf("wstat: attempted to change qid")
				return
			}

			if !stat.LengthNoTouch() && qid.Type()&QT_DIR != 0 {
				if h.Logger != nil {
					h.Logger.ErrorContext(ctx, "client failed: deny attempt to change length of dir")
				}
				w.Rerrorf("wstat: attempted to change length of dir")
				return
			}

			if !stat.ModeNoTouch() && int((stat.Mode()&M_DIR)>>24) != int(qid.Type()&QT_DIR) {
				if h.Logger != nil {
					h.Logger.ErrorContext(ctx, "client failed: deny attempt to change M_DIR bit on Mode")
				}
				w.Rerrorf("wstat: attempted to change Mode to M_DIR bit")
				return
			}

			// other states that are not allowed, but are not covered for now:
			// - modifying uid (aka the owner)
			// - gid should only be modified by owner (if the owner is also part of the new group)

			err := h.Fs.WriteStat(ctx, fullPath, stat)
			if err != nil {
				if h.Logger != nil {
					h.Logger.ErrorContext(ctx, "failed for %v: %s", fullPath, err)
				}
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
		if h.Logger != nil {
			h.Logger.DebugContext(ctx, "Twstat ->Rwstat",
				slog.Uint64("fid", uint64(m.Fid())),
				slog.String("file", fil.Name))
		}
		w.Rwstat()

	default:
		break // let default behavior run
	}
}
