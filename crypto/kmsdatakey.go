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
	"github.com/jt0/gomer/gomerr/constraint"
)

const (
	encryptedEncodingFormatVersion     = 1
	encryptedEncodingFormatVersionByte = byte(encryptedEncodingFormatVersion)
)

type kmsDataKeyEncrypter struct {
	kms         kmsiface.KMSAPI
	masterKeyId string
}

var keyStateConstraint = constraint.Equals("Enabled")
var keyUsageConstraint = constraint.Equals("ENCRYPT_DECRYPT")

// TODO: add support for asymmetric keys
func KmsDataKeyEncrypter(kmsClient kmsiface.KMSAPI, masterKeyId string) Encrypter {
	return kmsDataKeyEncrypter{
		kms:         kmsClient,
		masterKeyId: masterKeyId,
	}
}

func (k kmsDataKeyEncrypter) Encrypt(plaintext []byte, encryptionContext map[string]*string) ([]byte, gomerr.Gomerr) {
	return k.EncryptWithContext(context.Background(), plaintext, encryptionContext)
}

// TODO: add support for grant tokens?
func (k kmsDataKeyEncrypter) EncryptWithContext(context context.Context, plaintext []byte, encryptionContext map[string]*string) ([]byte, gomerr.Gomerr) {
	input := &kms.GenerateDataKeyInput{
		KeyId:             &k.masterKeyId,
		EncryptionContext: encryptionContext,
		KeySpec:           aws.String(kms.DataKeySpecAes256),
	}

	dataKey, err := k.kms.GenerateDataKeyWithContext(context, input)
	if err != nil {
		if awsErr, ok := err.(awserr.Error); ok {
			switch awsErr.Code() {
			case kms.ErrCodeNotFoundException:
				return nil, gomerr.NotFound("kms.CustomerMasterKey", k.masterKeyId).Wrap(err)
			case kms.ErrCodeDisabledException, kms.ErrCodeInvalidStateException:
				return nil, constraint.NotSatisfied(keyStateConstraint, "kms.CustomerMasterKey.KeyState", input.KeyId).Wrap(err)
			case kms.ErrCodeInvalidKeyUsageException:
				return nil, constraint.NotSatisfied(keyUsageConstraint, "kms.CustomerMasterKey.KeyUsage", input.KeyId).Wrap(err)
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

// Decrypt returns the decrypted form of the encrypted content given the optional encryptionContext. If
//
//  gomerr.Unprocessable:
//      The encoding format is not recognized
//  gomerr.Unmarshal:
//      There is a problem reading the the encoded data
//  gomerr.BadValue:
//      Either the encryption context did not match the encrypted data, or the encrypted data is corrupted
//  gomerr.NotSatisfied:
//      Either the required key's state or usage has an unexpected value
//  gomerr.Dependency:
//      An unexpected error occurred calling KMS

func (k kmsDataKeyDecrypter) Decrypt(encrypted []byte, encryptionContext map[string]*string) ([]byte, gomerr.Gomerr) {
	return k.DecryptWithContext(context.Background(), encrypted, encryptionContext)
}

// TODO: add support for grant tokens?
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
				return nil, constraint.NotSatisfied(constraint.Invalid, "ciphertext", input).Wrap(err)
			case kms.ErrCodeDisabledException, kms.ErrCodeInvalidStateException:
				return nil, constraint.NotSatisfied(keyStateConstraint, "kms.CustomerMasterKey.KeyState", "<embedded>").Wrap(err)
			case kms.ErrCodeInvalidKeyUsageException:
				return nil, constraint.NotSatisfied(keyUsageConstraint, "kms.CustomerMasterKey.KeyUsage", "<embedded>").Wrap(err)
			}
		}

		return nil, gomerr.Dependency("KMS", input).Wrap(err)
	}

	return k.decrypt(dataKey.Plaintext, ciphertext, nonce)
}

var validEncodingFormats = constraint.OneOf(encryptedEncodingFormatVersion)

// decode extracts the previously encoded values for ciphertext, ciphertextBlob, and nonce. Possible errors:
//
//  gomerr.Unprocessable:
//      The encoding format is not recognized
//  gomerr.Unmarshal:
//      There is a problem reading the the encoded data
func (k kmsDataKeyDecrypter) decode(encoded []byte) (ciphertext []byte, ciphertextBlob []byte, nonce []byte, ge gomerr.Gomerr) {
	reader := bytes.NewBuffer(encoded)

	version, _ := reader.ReadByte()
	if version != encryptedEncodingFormatVersionByte {
		return nil, nil, nil, gomerr.Unprocessable("encryptedEncodingFormatVersion", version, validEncodingFormats)
		// return gomerr.Unexpected
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
