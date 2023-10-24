package crypto

import (
	"context"

	"github.com/jt0/gomer/gomerr"
)

var NoOpCipher = Cipher{
	Encrypter: noOpEncrypter{},
	Decrypter: noOpDecrypter{},
}

type noOpEncrypter struct{}

func (n noOpEncrypter) Encrypt(plaintext []byte, _ map[string]*string) ([]byte, gomerr.Gomerr) {
	return plaintext, nil
}

func (n noOpEncrypter) EncryptWithContext(_ context.Context, plaintext []byte, _ map[string]*string) ([]byte, gomerr.Gomerr) {
	return plaintext, nil
}

type noOpDecrypter struct{}

func (n noOpDecrypter) Decrypt(encoded []byte, _ map[string]*string) ([]byte, gomerr.Gomerr) {
	return encoded, nil
}

func (n noOpDecrypter) DecryptWithContext(_ context.Context, encoded []byte, _ map[string]*string) ([]byte, gomerr.Gomerr) {
	return encoded, nil
}
