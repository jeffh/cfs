package encryptfs

import (
	"context"
	"crypto/rsa"
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/jeffh/cfs/ninep"
	"github.com/secure-io/sio-go"
)

// A file handle to an encrypted file. Due to the nature of encryption,
// only supports writing linearly (ignores offset value)
type chachaFileHandle struct {
	R      *sio.DecReader
	W      *sio.EncWriter
	Backed ninep.FileHandle
}

func (h *chachaFileHandle) Read(p []byte) (n int, err error) {
	if h.R == nil {
		return 0, io.EOF
	}
	return h.R.Read(p)
}
func (h *chachaFileHandle) Write(p []byte) (n int, err error) {
	if h.W == nil {
		return 0, io.EOF
	}
	return h.W.Write(p)
}

func (h *chachaFileHandle) Sync() error { return h.Backed.Sync() }

func (h *chachaFileHandle) Close() error {
	if h.W != nil {
		if err := h.W.Close(); err != nil {
			h.Backed.Close()
			return err
		}
	}
	return h.Backed.Close()
}

type commitHandle struct {
	tmp   ninep.FileHandle
	dirty bool

	stream    *sio.Stream
	writeFs   ninep.FileSystem
	writePath string

	key     []byte
	keyFs   ninep.FileSystem
	keyPath string
	pubKey  *rsa.PublicKey

	tmpFs   ninep.FileSystem
	tmpPath string
}

var _ ninep.FileHandle = (*commitHandle)(nil)

func OpenEncryptedFile(
	ctx context.Context,
	tmpFs ninep.FileSystem,
	tmpFile ninep.FileHandle,
	tmpPath string,
	writeFs ninep.FileSystem,
	dataPath string,
	stream *sio.Stream,
	nonce, key []byte,
	keyFs ninep.FileSystem,
	keyPath string,
	pubKey *rsa.PublicKey,
) (*commitHandle, error) {
	var isNewFile bool
	{
		info, err := writeFs.Stat(ctx, dataPath)
		isNewFile = errors.Is(err, os.ErrNotExist) || info.Size() == 0
	}

	eh, err := writeFs.OpenFile(ctx, dataPath, ninep.OREAD)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	err = nil
	if eh != nil && nonce != nil {
		defer eh.Close()
		h := stream.DecryptReader(ninep.Reader(eh), nonce, nil)
		if !isNewFile {
			var buf [4096]byte
			_, err = io.CopyBuffer(ninep.Writer(tmpFile), h, buf[:])
			if err != nil {
				eh.Close()
				return nil, err
			}
		}
	}

	commitHandle := &commitHandle{
		tmp: tmpFile,

		stream:    stream,
		writeFs:   writeFs,
		writePath: dataPath,

		key:     key,
		keyFs:   keyFs,
		keyPath: keyPath,
		pubKey:  pubKey,

		tmpFs:   tmpFs,
		tmpPath: tmpPath,
	}
	return commitHandle, nil
}

func (h *commitHandle) commit(ctx context.Context) error {
	nonce, err := generateChachaNonce(h.stream.NonceSize())
	if err != nil {
		return fmt.Errorf("Failed to generate nonce: %w", err)
	}

	keyFile, err := h.keyFs.CreateFile(ctx, h.keyPath, ninep.OWRITE|ninep.OTRUNC, 0600)
	if err != nil {
		return err
	}

	{
		buf := []byte("1")          // version
		buf = append(buf, h.key...) // chacha key
		buf = append(buf, nonce...) // chacha nonce

		cipher, err := PublicKeyEncrypt(h.pubKey, buf)
		if err != nil {
			keyFile.Close()
			h.keyFs.Delete(ctx, h.keyPath)
			return err
		}

		_, err = keyFile.WriteAt([]byte(cipher), 0)
		if err != nil {
			keyFile.Close()
			h.keyFs.Delete(ctx, h.keyPath)
			return err
		}
	}
	defer keyFile.Close()

	handle, err := h.writeFs.OpenFile(ctx, h.writePath, ninep.OWRITE|ninep.OTRUNC)
	if err != nil {
		return err
	}
	defer handle.Close()

	w := h.stream.EncryptWriter(ninep.Writer(handle), nonce, nil)
	defer w.Close()

	var buf [4096]byte
	_, err = io.CopyBuffer(w, ninep.Reader(h.tmp), buf[:])
	return err
}

func (h *commitHandle) deleteTemp() error {
	h.tmp.Close()
	return h.tmpFs.Delete(context.Background(), h.tmpPath)
}

func (h *commitHandle) ReadAt(p []byte, off int64) (n int, err error) { return h.tmp.ReadAt(p, off) }
func (h *commitHandle) WriteAt(p []byte, off int64) (n int, err error) {
	h.dirty = true
	return h.tmp.WriteAt(p, off)
}
func (h *commitHandle) Sync() error {
	if h.dirty {
		if err := h.tmp.Sync(); err != nil {
			return err
		}
		err := h.commit(context.Background())
		if err != nil {
			h.dirty = false
		}
		return err
	}
	return nil
}
func (h *commitHandle) Close() error {
	if err := h.Sync(); err != nil {
		h.tmp.Close()
		return err
	}
	return h.deleteTemp()
}
