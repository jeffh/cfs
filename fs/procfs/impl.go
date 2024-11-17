package procfs

import (
	"context"
	"io/fs"
	"iter"
	"os/user"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/jeffh/cfs/ninep"
	"github.com/jeffh/cfs/ninep/kvp"
)

type controlFile struct {
	Text func(f *fsys, pid Pid, pi *ProcInfo) ([]byte, error)
}

var controls = map[string]controlFile{
	"pid":        {Text: pidFile},
	"status":     {Text: statusFile},
	"gid":        {Text: gidFile},
	"uid":        {Text: uidFile},
	"real_uid":   {Text: realUidFile},
	"real_gid":   {Text: realGidFile},
	"parent_pid": {Text: parentPidFile},
	"pid_group":  {Text: pidGroupFile},
	"cmdline":    {Text: cmdlineFile},
	"env":        {Text: envFile},
	"fds":        {Text: fdsFile},
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
	Define().Path("/").TrailSlash().As("root").
	Define().Path("/{pid}").TrailSlash().As("proc").
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
		text, err := control.Text(f, Pid(parsedPid), nil)
		if err != nil {
			return nil, err
		}
		return &ninep.ReadOnlyMemoryFileHandle{Contents: []byte(text)}, nil
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
			now := time.Now()
			pi, err := pidInfo(Pid(parsedPid))
			if err != nil {
				yield(nil, err)
				return
			}
			user, group := f.userGroup(int(pi.Uid), int(pi.Gid))
			for _, key := range sortedKeys {
				control, ok := controls[key]
				if !ok {
					continue
				}
				text, _ := control.Text(f, Pid(parsedPid), &pi)
				size := int64(len(text))
				var info fs.FileInfo = &ninep.SimpleFileInfo{
					FIName:    key,
					FIMode:    ninep.Readable,
					FISize:    size,
					FIModTime: now,
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
			FIName:    ".",
			FIModTime: now,
			FIMode:    fs.ModeDir | ninep.Readable | ninep.Executable,
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
		text, _ := control.Text(f, Pid(parsedPid), &pi)
		size := int64(len(text))
		user, group := f.userGroup(int(pi.Uid), int(pi.Gid))
		var info fs.FileInfo = &ninep.SimpleFileInfo{
			FIName:    op,
			FIModTime: now,
			FIMode:    ninep.Readable,
			FISize:    size,
		}
		info = ninep.FileInfoWithUsers(info, user, group, "")
		return info, nil
	default:
		panic("unreachable")
	}
}

func (f *fsys) WriteStat(ctx context.Context, path string, stat ninep.Stat) error {
	return fs.ErrPermission
}

func (f *fsys) Delete(ctx context.Context, path string) error {
	return fs.ErrPermission
}

//////////////////////////////////////////////////////////

func (f *fsys) procDir(pid Pid, now time.Time) (fs.FileInfo, error) {
	pi, err := pidInfo(pid)
	if err != nil {
		return nil, err
	}
	user, group := f.userGroup(int(pi.Uid), int(pi.Gid))
	var info fs.FileInfo = &ninep.SimpleFileInfo{
		FIName:    strconv.Itoa(int(pid)),
		FIModTime: now,
		FIMode:    fs.ModeDir | ninep.Readable | ninep.Executable,
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

func pidFile(f *fsys, pid Pid, pi *ProcInfo) ([]byte, error) {
	return []byte(strconv.Itoa(int(pid))), nil
}

func statusFile(f *fsys, pid Pid, pi *ProcInfo) ([]byte, error) {
	if pi == nil {
		pinfo, err := pidInfo(pid)
		if err != nil {
			return nil, err
		}
		pi = &pinfo
	}
	return []byte(pi.Status.String()), nil
}

func gidFile(f *fsys, pid Pid, pi *ProcInfo) ([]byte, error) {
	if pi == nil {
		pinfo, err := pidInfo(pid)
		if err != nil {
			return nil, err
		}
		pi = &pinfo
	}
	return []byte(strconv.Itoa(int(pi.Gid)) + "\n" + f.group(int(pi.Gid)) + "\n"), nil
}

func uidFile(f *fsys, pid Pid, pi *ProcInfo) ([]byte, error) {
	if pi == nil {
		pinfo, err := pidInfo(pid)
		if err != nil {
			return nil, err
		}
		pi = &pinfo
	}
	return []byte(strconv.Itoa(int(pi.Uid)) + "\n" + f.user(int(pi.Uid)) + "\n"), nil
}

func realUidFile(f *fsys, pid Pid, pi *ProcInfo) ([]byte, error) {
	if pi == nil {
		pinfo, err := pidInfo(pid)
		if err != nil {
			return nil, err
		}
		pi = &pinfo
	}
	return []byte(strconv.Itoa(int(pi.RealUid))), nil
}

func realGidFile(f *fsys, pid Pid, pi *ProcInfo) ([]byte, error) {
	if pi == nil {
		pinfo, err := pidInfo(pid)
		if err != nil {
			return nil, err
		}
		pi = &pinfo
	}
	return []byte(strconv.Itoa(int(pi.RealGid))), nil
}

func parentPidFile(f *fsys, pid Pid, pi *ProcInfo) ([]byte, error) {
	if pi == nil {
		pinfo, err := pidInfo(pid)
		if err != nil {
			return nil, err
		}
		pi = &pinfo
	}
	return []byte(strconv.Itoa(int(pi.ParentPid))), nil
}

func pidGroupFile(f *fsys, pid Pid, pi *ProcInfo) ([]byte, error) {
	if pi == nil {
		pinfo, err := pidInfo(pid)
		if err != nil {
			return nil, err
		}
		pi = &pinfo
	}
	return []byte(strconv.Itoa(pi.PidGroup)), nil
}

func cmdlineFile(f *fsys, pid Pid, pi *ProcInfo) ([]byte, error) {
	argv, err := pidArgs(pid)
	if err == nil {
		for i, arg := range argv[1:] {
			if strings.Contains(arg, " ") {
				argv[i+1] = strconv.Quote(arg)
			}
		}
	}
	return []byte(strings.Join(argv, " ")), nil
}

func envFile(f *fsys, pid Pid, pi *ProcInfo) ([]byte, error) {
	env, err := pidEnv(pid)
	if err != nil {
		return nil, err
	}
	return []byte(strings.Join(env, "\n")), nil
}

func fdsFile(f *fsys, pid Pid, pio *ProcInfo) ([]byte, error) {
	fds, err := pidFds(pid)
	if err != nil {
		return nil, err
	}
	str := make([]string, len(fds))
	for i, fd := range fds {
		pairs := [][2]string{
			{"num", strconv.Itoa(fd.Num)},
			{"type", string(fd.Type)},
			{"name", fd.Name},
			{"socket_type", fd.SocketType},
			{"socket_source_addr", fd.SocketSourceAddr},
			{"socket_remote_addr", fd.SocketRemoteAddr},
		}
		str[i] = kvp.NonEmptyKeyPairs(pairs)
	}
	return []byte(strings.Join(str, "\n")), nil
}
