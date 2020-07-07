package crypto

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/binary"
	"io"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/kms"
	"github.com/aws/aws-sdk-go/service/kms/kmsiface"

	"github.com/jt0/gomer/constraint"
	"github.com/jt0/gomer/gomerr"
)

const (
	encryptedEncodingFormatVersion     = 1
	encryptedEncodingFormatVersionByte = byte(encryptedEncodingFormatVersion)
)

type kmsEncrypter struct {
	kms         kmsiface.KMSAPI
	masterKeyId string
}

func KmsEncrypter(kmsClient kmsiface.KMSAPI, masterKeyId string) Encrypter {
	return kmsEncrypter{
		kms:         kmsClient,
		masterKeyId: masterKeyId,
	}
}

func (k kmsEncrypter) Encrypt(plaintext []byte, encryptionContext map[string]*string) ([]byte, gomerr.Gomerr) {
	input := &kms.GenerateDataKeyInput{
		KeyId:             &k.masterKeyId,
		EncryptionContext: encryptionContext,
		KeySpec:           aws.String(kms.DataKeySpecAes256),
	}

	dataKey, err := k.kms.GenerateDataKey(input)
	if err != nil {
		if awsErr, ok := err.(awserr.Error); ok {
			switch awsErr.Code() {
			case kms.ErrCodeNotFoundException:
				return nil, gomerr.NotFound("kms.CustomerMasterKey", k.masterKeyId).WithCause(awsErr)
			case kms.ErrCodeDisabledException:
				fallthrough
			case kms.ErrCodeInvalidStateException:
				return nil, gomerr.Dependency(err, input).AddNotes("kms.GenerateDataKey()")
			case kms.ErrCodeInvalidGrantTokenException:
				fallthrough
			case kms.ErrCodeInvalidKeyUsageException:
				return nil, gomerr.Dependency(err, input)
			}
		}

		return nil, gomerr.Dependency(err, input)
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
		return nil, nil, CipherFailure(err)
	}

	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, nil, CipherFailure(err)
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

type kmsDecrypter struct {
	kms kmsiface.KMSAPI
}

func KmsDecrypter(kmsClient kmsiface.KMSAPI) Decrypter {
	return kmsDecrypter{
		kms: kmsClient,
	}
}

func (k kmsDecrypter) Decrypt(encrypted []byte, encryptionContext map[string]*string) ([]byte, gomerr.Gomerr) {
	ciphertext, ciphertextBlob, nonce, ge := decode(encrypted)
	if ge != nil {
		return nil, ge
	}

	input := &kms.DecryptInput{
		CiphertextBlob:    ciphertextBlob,
		EncryptionContext: encryptionContext,
	}

	dataKey, err := k.kms.Decrypt(input)
	if err != nil {
		if awsErr, ok := err.(awserr.Error); ok {
			switch awsErr.Code() {
			case kms.ErrCodeDisabledException:
				fallthrough
			case kms.ErrCodeInvalidStateException:
				return nil, gomerr.Dependency(err, input).AddNotes("kms.Decrypt()")
			case kms.ErrCodeInvalidGrantTokenException:
				fallthrough
			case kms.ErrCodeInvalidCiphertextException:
				fallthrough
			case kms.ErrCodeInvalidKeyUsageException:
				return nil, gomerr.Dependency(err, input)
			}
		}

		return nil, gomerr.Dependency(err, input)
	}

	return decrypt(dataKey.Plaintext, ciphertext, nonce)
}

func decode(encoded []byte) (ciphertext []byte, ciphertextBlob []byte, nonce []byte, ge gomerr.Gomerr) {
	reader := bytes.NewBuffer(encoded)

	version, _ := reader.ReadByte()
	if version != encryptedEncodingFormatVersionByte {
		return nil, nil, nil, gomerr.BadValue("encryptedEncodingFormatVersion", version, constraint.Values(encryptedEncodingFormatVersionByte))
	}

	var length uint16

	_ = binary.Read(reader, binary.LittleEndian, &length)
	ciphertext = make([]byte, length)
	if n, err := reader.Read(ciphertext); err != nil || n != int(length) {
		return nil, nil, nil, gomerr.Unmarshal(err, reader, "ciphertext")
	}

	_ = binary.Read(reader, binary.LittleEndian, &length)
	ciphertextBlob = make([]byte, length)
	if n, err := reader.Read(ciphertextBlob); err != nil || n != int(length) {
		return nil, nil, nil, gomerr.Unmarshal(err, reader, "ciphertextBlob")
	}

	_ = binary.Read(reader, binary.LittleEndian, &length)
	nonce = make([]byte, length)
	if n, err := reader.Read(nonce); err != nil || n != int(length) {
		return nil, nil, nil, gomerr.Unmarshal(err, reader, "nonce")
	}

	return
}

func decrypt(key []byte, ciphertext []byte, nonce []byte) ([]byte, gomerr.Gomerr) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, CipherFailure(err)
	}

	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, CipherFailure(err)
	}

	plaintext, err := aead.Open(ciphertext[:0], nonce, ciphertext, nil)
	if err != nil {
		return nil, CipherFailure(err)
	}

	return plaintext, nil
}
