// Implements a 9p file system that talks to a given server via SFTP
package sftpfs

import (
	"context"
	"fmt"
	"os"
	"os/user"
	"path/filepath"
	"time"

	"github.com/jeffh/cfs/ninep"
	"github.com/pkg/sftp"
	"golang.org/x/crypto/ssh"
)

type sftpFs struct {
	client *ssh.Client
	conn   *sftp.Client
	prefix string
}

var _ ninep.FileSystem = (*sftpFs)(nil)

func DefaultSSHConfig(sshUser, sshKeyPath string) (*ssh.ClientConfig, error) {
	username := sshUser
	if username == "" {
		user, err := user.Current()
		if err != nil {
			return nil, fmt.Errorf("Failed to determine current user: %w", err)
		}
		username = user.Username
	}

	sshConfig := &ssh.ClientConfig{
		User: username,
		Auth: removeNils([]ssh.AuthMethod{
			SSHAgent(),
			publicKeyFile(sshKeyPath),
			// publicKeyFile("~/.ssh/id_ed25519"),
			// publicKeyFile("~/.ssh/id_rsa"),
			// publicKeyFile("~/.ssh/id_dsa"),
		}),
		HostKeyCallback: ssh.InsecureIgnoreHostKey(), // TODO: read local hosts file
	}
	return sshConfig, nil
}

func New(conn *ssh.Client, prefix string) (ninep.FileSystem, error) {
	sftpConn, err := sftp.NewClient(conn)
	if err != nil {
		return nil, err
	}

	return &sftpFs{conn, sftpConn, prefix}, nil
}

func (fs *sftpFs) MakeDir(ctx context.Context, path string, mode ninep.Mode) error {
	fullPath := filepath.Join(fs.prefix, path)
	err := fs.conn.MkdirAll(fullPath)
	if err == nil {
		err = fs.conn.Chmod(fullPath, mode.ToOsMode())
	}
	return err
}

func (fs *sftpFs) CreateFile(ctx context.Context, path string, flag ninep.OpenMode, mode ninep.Mode) (ninep.FileHandle, error) {
	fullPath := filepath.Join(fs.prefix, path)
	h, err := fs.conn.OpenFile(fullPath, flag.ToOsFlag()|os.O_CREATE)
	if err != nil {
		return nil, err
	}
	err = fs.conn.Chmod(fullPath, mode.ToOsMode())
	if err != nil {
		return nil, err
	}
	return &File{h: h}, nil
}

func (fs *sftpFs) OpenFile(ctx context.Context, path string, flag ninep.OpenMode) (ninep.FileHandle, error) {
	fullPath := filepath.Join(fs.prefix, path)
	h, err := fs.conn.OpenFile(fullPath, flag.ToOsFlag())
	if err != nil {
		return nil, err
	}
	return &File{h: h}, nil
}

func (fs *sftpFs) ListDir(ctx context.Context, path string) (ninep.FileInfoIterator, error) {
	fullPath := filepath.Join(fs.prefix, path)
	infos, err := fs.conn.ReadDir(fullPath)
	if err != nil {
		return nil, err
	}
	return ninep.FileInfoSliceIterator(infos), nil
}

func (fs *sftpFs) Stat(ctx context.Context, path string) (os.FileInfo, error) {
	fullPath := filepath.Join(fs.prefix, path)
	return fs.conn.Stat(fullPath)
}

func (fs *sftpFs) WriteStat(ctx context.Context, path string, s ninep.Stat) error {
	changeGID := !s.GidNoTouch()
	changeUID := !s.UidNoTouch()
	// NOTE(jeff): technically, the spec disallows changing uids
	if changeUID {
		return ninep.ErrChangeUidNotAllowed
	}
	if changeGID {
		return ninep.ErrChangeGidNotAllowed
	}

	fullPath := filepath.Join(fs.prefix, path)
	// for restoring:
	// "Either all the changes in wstat request happen, or none of them does:
	// if the request succeeds, all changes were made; if it fails, none were."
	info, err := fs.conn.Stat(fullPath)
	if err != nil {
		return err
	}

	statT, ok := info.Sys().(*sftp.FileStat)
	if !ok {
		return ninep.ErrUnsupported
	}

	if !s.NameNoTouch() && path != s.Name() {
		newPath := filepath.Join(fs.prefix, s.Name())
		err = fs.conn.Rename(fullPath, newPath)
		if err != nil {
			return err
		}

		defer func() {
			if err != nil {
				fs.conn.Rename(newPath, fullPath)
			}
		}()

		fullPath = newPath
	}

	if !s.ModeNoTouch() {
		old := info.Mode()
		err = fs.conn.Chmod(fullPath, s.Mode().ToOsMode())
		if err != nil {
			return err
		}
		defer func() {
			if err != nil {
				fs.conn.Chmod(fullPath, old)
			}
		}()
	}

	changeMtime := !s.MtimeNoTouch()
	changeAtime := !s.AtimeNoTouch()
	if changeAtime || changeMtime {
		var oldAtime, oldMtime time.Time

		var ok bool
		oldAtime = time.Unix(int64(statT.Atime), 0)
		if !ok {
			oldAtime = info.ModTime()
		}
		oldMtime = info.ModTime()

		if changeMtime && changeAtime {
			err = fs.conn.Chtimes(fullPath, time.Unix(int64(s.Atime()), 0), time.Unix(int64(s.Mtime()), 0))
		} else if changeMtime {
			err = fs.conn.Chtimes(fullPath, oldAtime, time.Unix(int64(s.Mtime()), 0))
		} else if changeAtime {
			err = fs.conn.Chtimes(fullPath, time.Unix(int64(s.Atime()), 0), oldMtime)
		}
		if err != nil {
			return err
		}
		defer func() {
			if err != nil {
				fs.conn.Chtimes(fullPath, oldAtime, oldMtime)
			}
		}()
	}

	// this should be last since it's really hard to undo this
	if !s.LengthNoTouch() {
		err = fs.conn.Truncate(fullPath, int64(s.Length()))
		if err != nil {
			return err
		}
	}
	return err
}

func (fs *sftpFs) Delete(ctx context.Context, path string) error {
	fullPath := filepath.Join(fs.prefix, path)
	return fs.conn.Remove(fullPath)
}

func (fs *sftpFs) Close() error {
	return fs.conn.Close()
}
