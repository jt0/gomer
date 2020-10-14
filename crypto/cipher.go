package crypto

import (
	"github.com/jt0/gomer/gomerr"
)

type Encrypter interface {
	Encrypt(plaintext []byte, encryptionContext map[string]*string) ([]byte, gomerr.Gomerr)
}

type Decrypter interface {
	Decrypt(encoded []byte, encryptionContext map[string]*string) ([]byte, gomerr.Gomerr)
}

type Cipher struct {
	Encrypter
	Decrypter
}
