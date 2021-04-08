package crypto

import (
	"bytes"
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/binary"
	"io"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/kms"
	"github.com/aws/aws-sdk-go/service/kms/kmsiface"

	"github.com/jt0/gomer/gomerr"
)

const (
	encryptedEncodingFormatVersion     = 1
	encryptedEncodingFormatVersionByte = byte(encryptedEncodingFormatVersion)
)

type kmsDataKeyEncrypter struct {
	kms   kmsiface.KMSAPI
	keyId string
}

// TODO: add support for asymmetric keys
func KmsDataKeyEncrypter(kmsClient kmsiface.KMSAPI, masterKeyId string) Encrypter {
	return kmsDataKeyEncrypter{
		kms:   kmsClient,
		keyId: masterKeyId,
	}
}

func (k kmsDataKeyEncrypter) Encrypt(plaintext []byte, encryptionContext map[string]*string) ([]byte, gomerr.Gomerr) {
	return k.EncryptWithContext(context.Background(), plaintext, encryptionContext)
}

// Decrypt returns the decrypted form of the encrypted content given the optional encryptionContext. If
//
//  gomerr.NotFoundError:
//      The Encrypter keyId isn't found within KMS
//  gomerr.BadValueError:
//      The KMS key is in an invalid state
//  gomerr.InternalError:
//      A problem with the underlying crypto libraries
//  gomerr.DependencyError:
//      An unexpected error occurred calling KMS
// TODO: add support for grant tokens?
func (k kmsDataKeyEncrypter) EncryptWithContext(context context.Context, plaintext []byte, encryptionContext map[string]*string) ([]byte, gomerr.Gomerr) {
	input := &kms.GenerateDataKeyInput{
		KeyId:             &k.keyId,
		EncryptionContext: encryptionContext,
		KeySpec:           aws.String(kms.DataKeySpecAes256),
	}

	dataKey, err := k.kms.GenerateDataKeyWithContext(context, input)
	if err != nil {
		if awsErr, ok := err.(awserr.Error); ok {
			switch awsErr.Code() {
			case kms.ErrCodeNotFoundException:
				return nil, gomerr.NotFound("kms.KeyId", *input.KeyId).Wrap(err)
			case kms.ErrCodeDisabledException:
				return nil, gomerr.InvalidValue("KmsKey."+*input.KeyId+".KeyState", kms.KeyStateDisabled, kms.KeyStateEnabled).Wrap(err)
			case kms.ErrCodeInvalidStateException:
				return nil, gomerr.InvalidValue("KmsKey."+*input.KeyId+".KeyState", "<unavailable>", kms.KeyStateEnabled).Wrap(err)
			case kms.ErrCodeInvalidKeyUsageException:
				return nil, gomerr.InvalidValue("KmsKey."+*input.KeyId+".KeyUsage", "<unavailable>", kms.KeyUsageTypeEncryptDecrypt).Wrap(err)
			}
		}

		return nil, gomerr.Dependency("KMS", input).Wrap(err)
	}

	encrypted, nonce, ge := encrypt(dataKey.Plaintext, plaintext)
	if ge != nil {
		return nil, ge
	}

	return encode(encrypted, nonce, dataKey.CiphertextBlob), nil
}

func encrypt(key, plaintext []byte) (encrypted []byte, nonce []byte, ge gomerr.Gomerr) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, nil, gomerr.Internal("aes.NewCipher").Wrap(err)
	}

	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, nil, gomerr.Internal("cipher.NewGCM").Wrap(err)
	}

	nonce = make([]byte, aead.NonceSize())
	_, _ = io.ReadFull(rand.Reader, nonce)

	return aead.Seal(plaintext[:0], nonce, plaintext, nil), nonce, nil
}

func encode(ciphertext, nonce, ciphertextBlob []byte) []byte {
	writer := new(bytes.Buffer)

	writer.WriteByte(encryptedEncodingFormatVersionByte)

	_ = binary.Write(writer, binary.LittleEndian, uint16(len(ciphertext)))
	writer.Write(ciphertext)

	_ = binary.Write(writer, binary.LittleEndian, uint16(len(ciphertextBlob)))
	writer.Write(ciphertextBlob)

	_ = binary.Write(writer, binary.LittleEndian, uint16(len(nonce)))
	writer.Write(nonce)

	return writer.Bytes()
}

type kmsDataKeyDecrypter struct {
	kms kmsiface.KMSAPI
}

func KmsDataKeyDecrypter(kmsClient kmsiface.KMSAPI) Decrypter {
	return kmsDataKeyDecrypter{
		kms: kmsClient,
	}
}

// Decrypt returns the same data (and errors) as DecryptWithContext using just the Background context.
func (k kmsDataKeyDecrypter) Decrypt(encrypted []byte, encryptionContext map[string]*string) ([]byte, gomerr.Gomerr) {
	return k.DecryptWithContext(context.Background(), encrypted, encryptionContext)
}

// TODO: add support for grant tokens?
// DecryptWithContext returns the decrypted form of the encrypted content given the optional encryptionContext.
//
//  gomerr.UnmarshalError:
//      There is a problem reading the the encoded data
//  gomerr.BadValueError (type = Invalid):
//      The encryption context did not match the encrypted data, or the encrypted data is corrupted, or the KMS
//      key is in an invalid state
//  gomerr.InternalError:
//      A problem with the underlying crypto libraries
//  gomerr.DependencyError:
//      An unexpected error occurred calling KMS
func (k kmsDataKeyDecrypter) DecryptWithContext(context context.Context, encrypted []byte, encryptionContext map[string]*string) ([]byte, gomerr.Gomerr) {
	ciphertext, ciphertextBlob, nonce, ge := k.decode(encrypted)
	if ge != nil {
		return nil, ge
	}

	input := &kms.DecryptInput{
		CiphertextBlob:    ciphertextBlob,
		EncryptionContext: encryptionContext,
	}

	dataKey, err := k.kms.DecryptWithContext(context, input)
	if err != nil {
		if awsErr, ok := err.(awserr.Error); ok {
			switch awsErr.Code() {
			case kms.ErrCodeInvalidCiphertextException:
				return nil, gomerr.InvalidValue("ciphertext", input, nil).Wrap(err)
			case kms.ErrCodeDisabledException:
				return nil, gomerr.InvalidValue("KmsKey.KeyState", kms.KeyStateDisabled, kms.KeyStateEnabled).Wrap(err)
			case kms.ErrCodeInvalidStateException:
				return nil, gomerr.InvalidValue("KmsKey.KeyState", "<unavailable>", kms.KeyStateEnabled).Wrap(err)
			case kms.ErrCodeInvalidKeyUsageException:
				return nil, gomerr.InvalidValue("KmsKey.KeyUsage", "<unavailable>", kms.KeyUsageTypeEncryptDecrypt).Wrap(err)
			}
		}

		return nil, gomerr.Dependency("Kms", input).Wrap(err)
	}

	return k.decrypt(dataKey.Plaintext, ciphertext, nonce)
}

// decode extracts the previously encoded values for ciphertext, ciphertextBlob, and nonce. Possible errors:
//
//  gomerr.Unmarshal:
//      There is a problem reading the the encoded data
func (k kmsDataKeyDecrypter) decode(encoded []byte) (ciphertext []byte, ciphertextBlob []byte, nonce []byte, ge gomerr.Gomerr) {
	reader := bytes.NewBuffer(encoded)

	// Only one version to check so far...
	if version, _ := reader.ReadByte(); version != encryptedEncodingFormatVersion {
		return nil, nil, nil, gomerr.Unmarshal("encoded", reader, version).Wrap(ge)
	}

	var length uint16
	_ = binary.Read(reader, binary.LittleEndian, &length)
	ciphertext = make([]byte, length)
	if n, err := reader.Read(ciphertext); err != nil || n != int(length) {
		return nil, nil, nil, gomerr.Unmarshal("ciphertext", reader, ciphertext).Wrap(err)
	}

	_ = binary.Read(reader, binary.LittleEndian, &length)
	ciphertextBlob = make([]byte, length)
	if n, err := reader.Read(ciphertextBlob); err != nil || n != int(length) {
		return nil, nil, nil, gomerr.Unmarshal("ciphertextBlob", reader, ciphertextBlob).Wrap(err)
	}

	_ = binary.Read(reader, binary.LittleEndian, &length)
	nonce = make([]byte, length)
	if n, err := reader.Read(nonce); err != nil || n != int(length) {
		return nil, nil, nil, gomerr.Unmarshal("nonce", reader, nonce).Wrap(err)
	}

	return
}

// decrypt performs the ciphertext decryption using the provided key and nonce. The response contains the decrypted
// value or:
//
//  gomerr.Internal:
//      An error wrapping the underlying Go crypto error.
func (k kmsDataKeyDecrypter) decrypt(key []byte, ciphertext []byte, nonce []byte) ([]byte, gomerr.Gomerr) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, gomerr.Internal("aes.NewCipher").Wrap(err)
	}

	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, gomerr.Internal("cipher.NewGCM").Wrap(err)
	}

	plaintext, err := aead.Open(ciphertext[:0], nonce, ciphertext, nil)
	if err != nil {
		return nil, gomerr.Internal("aead.Open").Wrap(err)
	}

	return plaintext, nil
}
