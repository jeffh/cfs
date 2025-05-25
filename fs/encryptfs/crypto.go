package encryptfs

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha512"
	"crypto/x509"
	"os"

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

