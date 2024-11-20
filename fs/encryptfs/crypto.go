package encryptfs

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha512"
	"crypto/x509"
	"io"
	"os"

	"github.com/jeffh/cfs/ninep"
	"github.com/secure-io/sio-go"
	"golang.org/x/crypto/chacha20poly1305"
)

const PrivateKeyBits = 4096

func GeneratePrivateKey(path string, bits int) (*rsa.PrivateKey, error) {
	privKey, err := rsa.GenerateKey(rand.Reader, bits)
	if err != nil {
		return nil, err
	}
	buf, err := x509.MarshalPKCS8PrivateKey(privKey)
	if err != nil {
		return nil, err
	}
	err = os.WriteFile(path, buf, 0600)
	if err != nil {
		return nil, err
	}
	return privKey, err
}

func LoadPrivateKey(path string) (*rsa.PrivateKey, error) {
	buf, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	k, err := x509.ParsePKCS8PrivateKey(buf)
	if err != nil {
		return nil, err
	}

	ed, ok := k.(*rsa.PrivateKey)
	if !ok {
		return nil, ErrInvalidKey
	}
	return ed, nil
}

func PublicKeyEncrypt(pub *rsa.PublicKey, msg []byte) ([]byte, error) {
	return rsa.EncryptOAEP(sha512.New(), rand.Reader, pub, msg, nil)
}

func PrivateKeyDecrypt(priv *rsa.PrivateKey, cipher []byte) ([]byte, error) {
	return rsa.DecryptOAEP(sha512.New(), rand.Reader, priv, cipher, nil)
}

func generateChachaKey() ([]byte, error) {
	key := make([]byte, chacha20poly1305.KeySize)
	_, err := rand.Read(key)
	if err != nil {
		return nil, err
	}
	return key, err
}

func generateChachaNonce(size int) ([]byte, error) {
	key := make([]byte, size)
	_, err := rand.Read(key)
	if err != nil {
		return nil, err
	}
	return key, err
}

// Returns a writer that encrypts bytes written to it before writing it to w
func encryptedWriter(chachaKey, chachaNonce []byte, w io.Writer) (*sio.EncWriter, error) {
	s, err := sio.XChaCha20Poly1305.Stream(chachaKey)
	if err != nil {
		return nil, err
	}
	return s.EncryptWriter(w, chachaNonce, nil), nil
}

// Returns a reader that decrypts bytes read from r before returning it
func decryptedReader(chachaKey, chachaNonce []byte, r io.Reader) (*sio.DecReader, error) {
	s, err := sio.XChaCha20Poly1305.Stream(chachaKey)
	if err != nil {
		return nil, err
	}
	return s.DecryptReader(r, chachaNonce, nil), nil
}

func encryptedHandle(f ninep.FileHandle, stream *sio.Stream, chachaNonce []byte) (*chachaFileHandle, error) {
	var (
		r *sio.DecReader
		w *sio.EncWriter
	)
	r = stream.DecryptReader(ninep.Reader(f), chachaNonce, nil)
	w = stream.EncryptWriter(ninep.Writer(f), chachaNonce, nil)
	h := &chachaFileHandle{
		R:      r,
		W:      w,
		Backed: f,
	}
	return h, nil
}
