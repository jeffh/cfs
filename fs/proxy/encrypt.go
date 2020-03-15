package proxy

import (
	"context"
	"crypto/cipher"
	"crypto/ed25519"
	"crypto/rand"
	"fmt"
	"os"
	"path/filepath"

	"github.com/jeffh/cfs/ninep"
	"golang.org/x/crypto/chacha20poly1305"
)

// Basic encrypted file system overlay:
// - DataMount is where all the encrypted data is stored. File names are NOT
//   encrypted.
// - KeysMount is where all the encrypted keys are stored. File names are NOT
//   encrypted. Used to decrypted DataMount.
// - PrivKey is the private key used to decrypt the KeysMount.
//
// PrivKey must be securely stored, but KeysMount and DataMount can be in
// untrusted locations.
type EncryptedFileSystem struct {
	DataMount FileSystemMount // where to write all data (encrypted)
	KeysMount FileSystemMount // where to write secret keys for DataMount files (encrypted)
	PrivKey   ed25519.PrivateKey
}

var _ ninep.FileSystem = (*EncryptedFileSystem)(nil)

func (fs *EncryptedFileSystem) createSharedSecret() (k []byte, aead cipher.AEAD, noune []byte, err error) {
	key := make([]byte, chacha20poly1305.KeySize)
	_, err = rand.Read(key)
	if err != nil {
		return nil, nil, nil, err
	}
	aead, err = chacha20poly1305.NewX(key)
	if err != nil {
		return nil, nil, nil, err
	}

	nounce := make([]byte, chacha20poly1305.NonceSizeX)
	_, err = rand.Read(nounce)
	if err != nil {
		return nil, nil, nil, err
	}
	return key, aead, nounce, err
}

func (fs *EncryptedFileSystem) loadSharedSecret(k []byte) (cipher.AEAD, error) {
	return chacha20poly1305.NewX(k)
}

func (fs *EncryptedFileSystem) MakeDir(ctx context.Context, path string, mode ninep.Mode) error {
	err := fs.KeysMount.FS.MakeDir(ctx, filepath.Join(fs.KeysMount.Prefix, path), 755)
	if err != nil {
		return err
	}
	return fs.DataMount.FS.MakeDir(ctx, filepath.Join(fs.DataMount.Prefix, path), mode)
}

func (fs *EncryptedFileSystem) CreateFile(ctx context.Context, path string, flag ninep.OpenMode, mode ninep.Mode) (ninep.FileHandle, error) {
	_, err := fs.KeysMount.FS.CreateFile(ctx, filepath.Join(fs.KeysMount.Prefix, path), flag, mode)
	if err != nil {
		return nil, err
	}
	return nil, fs.DataMount.FS.MakeDir(ctx, filepath.Join(fs.DataMount.Prefix, path), mode)
}

func (fs *EncryptedFileSystem) OpenFile(ctx context.Context, path string, flag ninep.OpenMode) (ninep.FileHandle, error) {
	return nil, fmt.Errorf("Not implemented")
}

func (fs *EncryptedFileSystem) ListDir(ctx context.Context, path string) (ninep.FileInfoIterator, error) {
	return nil, fmt.Errorf("Not implemented")
}

func (fs *EncryptedFileSystem) Stat(ctx context.Context, path string) (os.FileInfo, error) {
	return nil, fmt.Errorf("Not implemented")
}

func (fs *EncryptedFileSystem) WriteStat(ctx context.Context, path string, s ninep.Stat) error {
	return fmt.Errorf("Not implemented")
}

func (fs *EncryptedFileSystem) Delete(ctx context.Context, path string) error {
	return fmt.Errorf("Not implemented")
}
