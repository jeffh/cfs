package encryptfs

import (
	"context"
	"crypto/cipher"
	"crypto/rsa"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"

	"github.com/jeffh/cfs/fs/proxy"
	"github.com/jeffh/cfs/ninep"
	"github.com/secure-io/sio-go"
	"golang.org/x/crypto/chacha20poly1305"
)

// Basic encrypted file system overlay:
// - DataMount is where all the encrypted data is stored. File names are NOT
//   encrypted.
// - KeysMount is where all the encrypted keys are stored. File names are NOT
//   encrypted. Used to decrypted DataMount.
// - DecryptMount is where all the "in-memory" opened files reside. Should be
//   primarily something in memory or a secure location as opened files
//   are decrypted to this location to support read-at locations
// - PrivKey is the private key used to decrypt the KeysMount.
//
// PrivKey must be securely stored, but KeysMount and DataMount can be in
// untrusted locations.
type EncryptedFileSystem struct {
	DataMount    proxy.FileSystemMount // where to write all data (encrypted)
	KeysMount    proxy.FileSystemMount // where to write secret keys for DataMount files (encrypted)
	DecryptMount proxy.FileSystemMount // where files are temporarily unencrypted for reads and writes
	PrivKey      *rsa.PrivateKey       // required
}

func New(privKey *rsa.PrivateKey, keysMount, dataMount, decryptMount proxy.FileSystemMount) *EncryptedFileSystem {
	return &EncryptedFileSystem{
		dataMount,
		keysMount,
		decryptMount,
		privKey,
	}
}

func (f *EncryptedFileSystem) Init(ctx context.Context) error {
	if err := f.ensureRoots(ctx); err != nil {
		return err
	}
	return nil
}

var _ ninep.FileSystem = (*EncryptedFileSystem)(nil)

func (f *EncryptedFileSystem) loadSharedSecret(k []byte) (cipher.AEAD, error) {
	return chacha20poly1305.NewX(k)
}

func (f *EncryptedFileSystem) ensureRoots(ctx context.Context) error {
	if err := f.DecryptMount.FS.MakeDir(ctx, f.DecryptMount.Prefix, 0700); err != nil {
		if !errors.Is(err, os.ErrExist) {
			return fmt.Errorf("Failed to initialize mem root: %w", err)
		}
	}

	if err := f.KeysMount.FS.MakeDir(ctx, f.KeysMount.Prefix, 0700); err != nil {
		if !errors.Is(err, os.ErrExist) {
			return fmt.Errorf("Failed to initialize keys root: %w", err)
		}
	}

	if err := f.KeysMount.FS.MakeDir(ctx, f.DataMount.Prefix, 0755); err != nil {
		if !errors.Is(err, os.ErrExist) {
			return fmt.Errorf("Failed to initialize data root: %w", err)
		}
	}
	return nil
}

func (f *EncryptedFileSystem) MakeDir(ctx context.Context, path string, mode ninep.Mode) error {
	err := f.KeysMount.FS.MakeDir(ctx, filepath.Join(f.KeysMount.Prefix, path), 755)
	if err != nil {
		return err
	}
	return f.DataMount.FS.MakeDir(ctx, filepath.Join(f.DataMount.Prefix, path), mode)
}

func (f *EncryptedFileSystem) CreateFile(ctx context.Context, path string, flag ninep.OpenMode, mode ninep.Mode) (ninep.FileHandle, error) {
	key, err := generateChachaKey()
	if err != nil {
		return nil, fmt.Errorf("Failed to generate key: %w", err)
	}
	stream, err := sio.XChaCha20Poly1305.Stream(key)
	if err != nil {
		return nil, err
	}

	keyPath := filepath.Join(f.KeysMount.Prefix, path)

	// TODO: handle multiple creates of this file
	tmpPath := filepath.Join(f.DecryptMount.Prefix, path)
	tmpFile, err := f.DecryptMount.FS.CreateFile(ctx, tmpPath, ninep.ORDWR|ninep.OTRUNC, 0600)
	if err != nil {
		return nil, err
	}
	dataPath := filepath.Join(f.DataMount.Prefix, path)

	// just to set the mode
	dataFile, err := f.DataMount.FS.CreateFile(ctx, dataPath, flag, mode)
	if err != nil {
		tmpFile.Close()
		f.DecryptMount.FS.Delete(ctx, tmpPath)
		return nil, err
	}
	dataFile.Close()

	commitHandle, err := OpenEncryptedFile(
		ctx,
		f.DecryptMount.FS,
		tmpFile,
		tmpPath,
		f.DataMount.FS,
		dataPath,
		stream,
		nil, // nonce - nil means don't read encrypted file
		key,
		f.KeysMount.FS,
		keyPath,
		&f.PrivKey.PublicKey,
	)
	if err != nil {
		tmpFile.Close()
		f.DecryptMount.FS.Delete(ctx, tmpPath)
		return nil, err
	}
	return &ninep.ProtectedFileHandle{commitHandle, flag}, nil
}

func (f *EncryptedFileSystem) OpenFile(ctx context.Context, path string, flag ninep.OpenMode) (ninep.FileHandle, error) {
	keyPath := filepath.Join(f.KeysMount.Prefix, path)
	keyFile, err := f.KeysMount.FS.OpenFile(ctx, keyPath, ninep.ORDWR)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			keyFile, err = f.KeysMount.FS.CreateFile(ctx, keyPath, ninep.ORDWR|ninep.OTRUNC, 0600)
		} else {
			return nil, err
		}
		if err != nil {
			return nil, err
		}
	}
	defer keyFile.Close()

	info, err := f.KeysMount.FS.Stat(ctx, keyPath)
	if err != nil {
		return nil, err
	}

	var (
		key    []byte
		nonce  []byte
		stream *sio.Stream
	)
	// we have an empty file
	if info.Size() == 0 {
		key, err = generateChachaKey()
		if err != nil {
			return nil, fmt.Errorf("Failed to generate key: %w", err)
		}
		stream, err = sio.XChaCha20Poly1305.Stream(key)
		if err != nil {
			return nil, err
		}
	} else {
		cipher, err := ioutil.ReadAll(ninep.Reader(keyFile))
		if err != nil {
			return nil, err
		}

		plaintext, err := PrivateKeyDecrypt(f.PrivKey, cipher)
		if err != nil {
			return nil, err
		}

		// sio reserved 4 bits of the nonce for its use
		expectedSize := 1 + chacha20poly1305.KeySize + chacha20poly1305.NonceSizeX - 4
		if len(plaintext) < expectedSize {
			return nil, fmt.Errorf("Unexpected key file size: %v < %v (actual vs expected)", len(plaintext), expectedSize)
		}

		if plaintext[0] != '1' {
			return nil, fmt.Errorf("Unexpected key file version: %v != 1", plaintext[0])
		}

		key = plaintext[1 : 1+chacha20poly1305.KeySize]
		nonce = plaintext[1+chacha20poly1305.KeySize:]
		stream, err = sio.XChaCha20Poly1305.Stream(key)
		if err != nil {
			return nil, err
		}
	}

	// TODO: handle multiple creates of this file
	tmpPath := filepath.Join(f.DecryptMount.Prefix, path)
	tmpFile, err := f.DecryptMount.FS.CreateFile(ctx, tmpPath, ninep.ORDWR|ninep.OTRUNC, 0600)
	if err != nil {
		return nil, err
	}
	dataPath := filepath.Join(f.DataMount.Prefix, path)

	commitHandle, err := OpenEncryptedFile(
		ctx,
		f.DecryptMount.FS,
		tmpFile,
		tmpPath,
		f.DataMount.FS,
		dataPath,
		stream,
		nonce,
		key,
		f.KeysMount.FS,
		keyPath,
		&f.PrivKey.PublicKey,
	)
	if err != nil {
		tmpFile.Close()
		f.DecryptMount.FS.Delete(ctx, tmpPath)
		return nil, err
	}
	return &ninep.ProtectedFileHandle{commitHandle, flag}, nil
}

func (f *EncryptedFileSystem) ListDir(ctx context.Context, path string) (ninep.FileInfoIterator, error) {
	return f.DataMount.FS.ListDir(ctx, filepath.Join(f.DataMount.Prefix, path))
}

func (f *EncryptedFileSystem) Stat(ctx context.Context, path string) (os.FileInfo, error) {
	return f.DataMount.FS.Stat(ctx, filepath.Join(f.DataMount.Prefix, path))
}

func (f *EncryptedFileSystem) WriteStat(ctx context.Context, path string, s ninep.Stat) error {
	statErr := f.DataMount.FS.WriteStat(ctx, filepath.Join(f.DataMount.Prefix, path), s)
	if statErr == nil {
		if !s.NameNoTouch() && path != s.Name() {
			keyStat := ninep.SyncStatWithName(filepath.Join(f.KeysMount.Prefix, s.Name()))
			err := f.KeysMount.FS.WriteStat(ctx, filepath.Join(f.KeysMount.Prefix, path), keyStat)
			// TODO: if there's a key already there... overwrite it
			if err != nil {
				return err
			}
		}
	}
	return statErr
}

func (f *EncryptedFileSystem) Delete(ctx context.Context, path string) error {
	err := f.DataMount.FS.Delete(ctx, filepath.Join(f.DataMount.Prefix, path))
	if err != nil {
		return err
	}
	return f.KeysMount.FS.Delete(ctx, filepath.Join(f.KeysMount.Prefix, path))
}
