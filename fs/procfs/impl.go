package procfs

import (
	"context"
	"fmt"
	"io/fs"
	"iter"
	"os/user"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/jeffh/cfs/ninep"
)

type controlFile struct {
	Info   *ninep.SimpleFileInfo
	Handle func(f *fsys, pid Pid) (ninep.FileHandle, error)
}

var controls = map[string]controlFile{
	"pid":        {Handle: pidFile},
	"status":     {Handle: statusFile},
	"gid":        {Handle: gidFile},
	"uid":        {Handle: uidFile},
	"real_uid":   {Handle: realUidFile},
	"real_gid":   {Handle: realGidFile},
	"parent_pid": {Handle: parentPidFile},
	"pid_group":  {Handle: pidGroupFile},
	"cmdline":    {Handle: cmdlineFile},
	"env":        {Handle: envFile},
	"fds":        {Handle: fdsFile},
}

var sortedKeys []string

func init() {
	sortedKeys = make([]string, 0, len(controls))
	for k := range controls {
		sortedKeys = append(sortedKeys, k)
	}
	sort.Strings(sortedKeys)
}

var mx = ninep.NewMux().
	Define().Path("/").As("root").
	Define().Path("/{pid}").As("proc").
	Define().Path("/{pid}/{op}").As("procOp")

type fsys struct {
	m        sync.Mutex
	uidCache map[int]string
	gidCache map[int]string
}

var _ ninep.FileSystem = (*fsys)(nil)

func (f *fsys) MakeDir(ctx context.Context, path string, mode ninep.Mode) error {
	return ninep.ErrWriteNotAllowed
}

func (f *fsys) CreateFile(ctx context.Context, path string, flag ninep.OpenMode, mode ninep.Mode) (ninep.FileHandle, error) {
	return nil, ninep.ErrWriteNotAllowed
}

func (f *fsys) OpenFile(ctx context.Context, path string, flag ninep.OpenMode) (ninep.FileHandle, error) {
	var res ninep.Match
	ok := mx.Match(path, &res)
	if !ok {
		return nil, fs.ErrNotExist
	}
	switch res.Id {
	case "procOp":
		pid := res.Vars[0]
		op := res.Vars[1]
		parsedPid, err := strconv.Atoi(pid)
		if err != nil {
			return nil, fs.ErrNotExist
		}
		control, ok := controls[op]
		if !ok {
			return nil, fs.ErrNotExist
		}
		return control.Handle(f, Pid(parsedPid))
	default:
		return nil, fs.ErrNotExist
	}
}

func (f *fsys) ListDir(ctx context.Context, path string) iter.Seq2[fs.FileInfo, error] {
	var res ninep.Match
	ok := mx.Match(path, &res)
	if !ok {
		return ninep.FileInfoErrorIterator(fs.ErrNotExist)
	}

	switch res.Id {
	case "root":
		return func(yield func(fs.FileInfo, error) bool) {
			now := time.Now()
			pids, err := pidsList(QUERY_ALL)
			if err != nil {
				yield(nil, err)
				return
			}
			sort.Sort(SortablePids(pids))
			for _, pid := range pids {
				info, err := f.procDir(pid, now)
				if !yield(info, err) {
					return
				}
			}
		}
	case "proc":
		pid := res.Vars[0]
		parsedPid, err := strconv.Atoi(pid)
		if err != nil {
			return ninep.FileInfoErrorIterator(fs.ErrNotExist)
		}
		return func(yield func(fs.FileInfo, error) bool) {
			pi, err := pidInfo(Pid(parsedPid))
			if err != nil {
				yield(nil, err)
				return
			}
			user, group := f.userGroup(int(pi.Uid), int(pi.Gid))
			for _, key := range sortedKeys {
				meta := controls[key]
				var info fs.FileInfo = meta.Info
				if meta.Info == nil {
					info = &ninep.SimpleFileInfo{
						FIName: key,
						FIMode: 0444,
					}
				}
				info = ninep.FileInfoWithUsers(info, user, group, "")
				if !yield(info, nil) {
					return
				}
			}
		}
	case "procOp":
		return ninep.FileInfoErrorIterator(fs.ErrNotExist)
	default:
		panic("unreachable")
	}
}

func (f *fsys) Stat(ctx context.Context, path string) (fs.FileInfo, error) {
	var res ninep.Match
	ok := mx.Match(path, &res)
	if !ok {
		return nil, fs.ErrNotExist
	}
	now := time.Now()
	switch res.Id {
	case "root":
		return &ninep.SimpleFileInfo{
			FIName:    "/",
			FIModTime: now,
			FIMode:    fs.ModeDir | 0555,
		}, nil
	case "proc":
		pid := res.Vars[0]
		parsedPid, err := strconv.Atoi(pid)
		if err != nil {
			return nil, fs.ErrNotExist
		}
		return f.procDir(Pid(parsedPid), now)
	case "procOp":
		pid := res.Vars[0]
		op := res.Vars[1]
		parsedPid, err := strconv.Atoi(pid)
		if err != nil {
			return nil, fs.ErrNotExist
		}
		control, ok := controls[op]
		if !ok {
			return nil, fs.ErrNotExist
		}
		pi, err := pidInfo(Pid(parsedPid))
		if err != nil {
			return nil, err
		}
		user, group := f.userGroup(int(pi.Uid), int(pi.Gid))
		var info fs.FileInfo = control.Info
		if control.Info == nil {
			info = &ninep.SimpleFileInfo{
				FIName:    op,
				FIModTime: now,
				FIMode:    0444,
			}
		}
		info = ninep.FileInfoWithUsers(info, user, group, "")
		return info, nil
	default:
		panic("unreachable")
	}
}

func (f *fsys) WriteStat(ctx context.Context, path string, stat ninep.Stat) error {
	return nil
}

func (f *fsys) Delete(ctx context.Context, path string) error {
	return nil
}

//////////////////////////////////////////////////////////

func (f *fsys) procControls(pid Pid) iter.Seq2[fs.FileInfo, error] {
	return func(yield func(fs.FileInfo, error) bool) {
		now := time.Now()
		pi, err := pidInfo(pid)
		if err != nil {
			yield(nil, err)
			return
		}
		user, group := f.userGroup(int(pi.Uid), int(pi.Gid))
		for _, key := range sortedKeys {
			meta := controls[key]
			var info fs.FileInfo = meta.Info
			if meta.Info == nil {
				info = &ninep.SimpleFileInfo{
					FIName:    key,
					FIModTime: now,
					FIMode:    0444,
				}
			}
			info = ninep.FileInfoWithUsers(info, user, group, "")
			if !yield(info, nil) {
				return
			}
		}
	}
}

func (f *fsys) procDir(pid Pid, now time.Time) (fs.FileInfo, error) {
	pi, err := pidInfo(pid)
	if err != nil {
		return nil, err
	}
	user, group := f.userGroup(int(pi.Uid), int(pi.Gid))
	var info fs.FileInfo = &ninep.SimpleFileInfo{
		FIName:    strconv.Itoa(int(pid)),
		FIModTime: now,
		FIMode:    fs.ModeDir | 0777,
	}
	info = ninep.FileInfoWithUsers(info, user, group, "")
	return info, nil
}

func (f *fsys) userGroup(uid, gid int) (string, string) {
	f.m.Lock()
	defer f.m.Unlock()
	return f.user(uid), f.group(gid)
}

func (f *fsys) user(uid int) string {
	if f.uidCache == nil {
		f.uidCache = make(map[int]string)
	}
	if name, ok := f.uidCache[uid]; ok {
		return name
	}
	usr, err := user.LookupId(strconv.Itoa(uid))
	if err != nil {
		return ""
	}
	f.uidCache[uid] = usr.Username
	return usr.Username
}

func (f *fsys) group(gid int) string {
	if f.gidCache == nil {
		f.gidCache = make(map[int]string)
	}
	if name, ok := f.gidCache[gid]; ok {
		return name
	}
	grp, err := user.LookupGroupId(strconv.Itoa(gid))
	if err != nil {
		return ""
	}
	f.gidCache[gid] = grp.Name
	return grp.Name
}

func pidFile(f *fsys, pid Pid) (ninep.FileHandle, error) {
	b := []byte(strconv.Itoa(int(pid)))
	return &ninep.ReadOnlyMemoryFileHandle{Contents: b}, nil
}

func statusFile(f *fsys, pid Pid) (ninep.FileHandle, error) {
	pi, err := pidInfo(pid)
	if err != nil {
		return nil, err
	}
	b := []byte(pi.Status.String())
	return &ninep.ReadOnlyMemoryFileHandle{Contents: b}, nil
}

func gidFile(f *fsys, pid Pid) (ninep.FileHandle, error) {
	pi, err := pidInfo(pid)
	if err != nil {
		return nil, err
	}
	b := []byte(strconv.Itoa(int(pi.Gid)) + "\n" + f.group(int(pi.Gid)) + "\n")
	return &ninep.ReadOnlyMemoryFileHandle{Contents: b}, nil
}

func uidFile(f *fsys, pid Pid) (ninep.FileHandle, error) {
	pi, err := pidInfo(pid)
	if err != nil {
		return nil, err
	}
	b := []byte(strconv.Itoa(int(pi.Uid)) + "\n" + f.user(int(pi.Uid)) + "\n")
	return &ninep.ReadOnlyMemoryFileHandle{Contents: b}, nil
}

func realUidFile(f *fsys, pid Pid) (ninep.FileHandle, error) {
	pi, err := pidInfo(pid)
	if err != nil {
		return nil, err
	}
	b := []byte(strconv.Itoa(int(pi.RealUid)))
	return &ninep.ReadOnlyMemoryFileHandle{Contents: b}, nil
}

func realGidFile(f *fsys, pid Pid) (ninep.FileHandle, error) {
	pi, err := pidInfo(pid)
	if err != nil {
		return nil, err
	}
	b := []byte(strconv.Itoa(int(pi.RealGid)))
	return &ninep.ReadOnlyMemoryFileHandle{Contents: b}, nil
}

func parentPidFile(f *fsys, pid Pid) (ninep.FileHandle, error) {
	pi, err := pidInfo(pid)
	if err != nil {
		return nil, err
	}
	b := []byte(strconv.Itoa(int(pi.ParentPid)))
	return &ninep.ReadOnlyMemoryFileHandle{Contents: b}, nil
}

func pidGroupFile(f *fsys, pid Pid) (ninep.FileHandle, error) {
	pi, err := pidInfo(pid)
	if err != nil {
		return nil, err
	}
	b := []byte(strconv.Itoa(pi.PidGroup))
	return &ninep.ReadOnlyMemoryFileHandle{Contents: b}, nil
}

func cmdlineFile(f *fsys, pid Pid) (ninep.FileHandle, error) {
	argv, err := pidArgs(pid)
	if err == nil {
		for i, arg := range argv[1:] {
			if strings.Index(arg, " ") != -1 {
				argv[i+1] = strconv.Quote(arg)
			}
		}
	}
	b := []byte(strings.Join(argv, " "))
	return &ninep.ReadOnlyMemoryFileHandle{Contents: b}, nil
}

func envFile(f *fsys, pid Pid) (ninep.FileHandle, error) {
	env, err := pidEnv(pid)
	if err != nil {
		return nil, err
	}
	b := []byte(strings.Join(env, "\n"))
	return &ninep.ReadOnlyMemoryFileHandle{Contents: b}, nil
}

func fdsFile(f *fsys, pid Pid) (ninep.FileHandle, error) {
	fds, err := pidFds(pid)
	if err != nil {
		return nil, err
	}
	str := make([]string, len(fds))
	for i, fd := range fds {
		str[i] = fmt.Sprintf("num=%d type=%d name=%q socket_type=%q socket_source_addr=%q socket_remote_addr=%q", fd.Num, fd.Type, fd.Name, fd.SocketType, fd.SocketSourceAddr, fd.SocketRemoteAddr)
	}
	b := []byte(strings.Join(str, "\n"))
	return &ninep.ReadOnlyMemoryFileHandle{Contents: b}, nil
}
