package crypto

import (
	"github.com/jt0/gomer/gomerr"
)

type Cipher interface {
	Encrypt(plaintext []byte, encryptionContext map[string]*string) (*string, *gomerr.ApplicationError)
	Decrypt(encoded *string, encryptionContext map[string]*string) ([]byte, *gomerr.ApplicationError)
}
