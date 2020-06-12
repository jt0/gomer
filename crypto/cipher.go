package crypto

import (
	"github.com/jt0/gomer/gomerr"
)

type Cipher interface {
	Encrypt(plaintext []byte, encryptionContext map[string]*string) (*string, gomerr.Gomerr)
	Decrypt(encoded *string, encryptionContext map[string]*string) ([]byte, gomerr.Gomerr)
}

type CipherError struct {
	gomerr.Gomerr
}

func CipherFailure(cause error) gomerr.Gomerr {
	return gomerr.BuildWithCause(cause, new(CipherError))
}
