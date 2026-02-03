package crypto

import (
	"context"

	"github.com/jt0/gomer/gomerr"
)

type Encrypter interface {
	Encrypt(ctx context.Context, plaintext []byte, encryptionContext map[string]string) ([]byte, gomerr.Gomerr)
}

type Decrypter interface {
	Decrypt(ctx context.Context, encoded []byte, encryptionContext map[string]string) ([]byte, gomerr.Gomerr)
}

type Cipher struct {
	Encrypter
	Decrypter
}
