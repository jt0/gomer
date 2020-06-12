package crypto

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"encoding/binary"
	"io"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/kms"
	"github.com/aws/aws-sdk-go/service/kms/kmsiface"

	"github.com/jt0/gomer/constraint"
	"github.com/jt0/gomer/gomerr"
)

type KmsDataKey struct {
	kms         kmsiface.KMSAPI
	masterKeyId *string
	nonceLength uint8
}

func NewKmsDataKey(kmsClient kmsiface.KMSAPI, masterKeyId string, nonceLength uint8) (*KmsDataKey, gomerr.Gomerr) {
	input := &kms.DescribeKeyInput{
		KeyId: aws.String(masterKeyId),
	}

	output, err := kmsClient.DescribeKey(input)
	if err != nil {
		if awsErr, ok := err.(awserr.Error); ok {
			switch awsErr.Code() {
			case kms.ErrCodeNotFoundException:
				fallthrough
			case kms.ErrCodeInvalidArnException:
				return nil, gomerr.NotFound("kms.CustomerMasterKey", masterKeyId).WithCause(awsErr).AddCulprit(gomerr.Configuration)
			}
		}

		return nil, gomerr.Dependency(err, input)
	}

	if !*output.KeyMetadata.Enabled {
		return nil, gomerr.BadValue("kms.CustomerMasterKey", *output.KeyMetadata.KeyState, constraint.Values("ENABLED")).AddNotes("masterKeyId: " + masterKeyId).AddCulprit(gomerr.Configuration)
	}

	return &KmsDataKey{
		kms:         kmsClient,
		masterKeyId: output.KeyMetadata.KeyId,
		nonceLength: nonceLength,
	}, nil
}

func (k *KmsDataKey) Encrypt(plaintext []byte, encryptionContext map[string]*string) (*string, gomerr.Gomerr) {
	input := &kms.GenerateDataKeyInput{
		KeyId:             k.masterKeyId,
		EncryptionContext: encryptionContext,
		KeySpec:           aws.String(kms.DataKeySpecAes256),
	}

	// TODO: build (optional) support for caching
	dataKey, err := k.kms.GenerateDataKey(input)
	if err != nil {
		if awsErr, ok := err.(awserr.Error); ok {
			switch awsErr.Code() {
			case kms.ErrCodeNotFoundException:
				return nil, gomerr.NotFound("kms.CustomerMasterKey", *k.masterKeyId).WithCause(awsErr).AddCulprit(gomerr.Configuration)
			case kms.ErrCodeDisabledException:
				fallthrough
			case kms.ErrCodeInvalidStateException:
				return nil, gomerr.Dependency(err, input).AddNotes("kms.GenerateDataKey()").AddCulprit(gomerr.Configuration)
			case kms.ErrCodeInvalidGrantTokenException:
				fallthrough
			case kms.ErrCodeInvalidKeyUsageException:
				return nil, gomerr.Dependency(err, input).AddCulprit(gomerr.Configuration)
			}
		}

		return nil, gomerr.Dependency(err, input)
	}

	return k.encrypt(plaintext, dataKey)
}

func (k *KmsDataKey) encrypt(plaintext []byte, dataKey *kms.GenerateDataKeyOutput) (*string, gomerr.Gomerr) {
	block, err := aes.NewCipher(dataKey.Plaintext)
	if err != nil {
		return nil, CipherFailure(err)
	}

	nonce := make([]byte, k.nonceLength)
	_, _ = io.ReadFull(rand.Reader, nonce)

	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, CipherFailure(err)
	}

	return encode(aead.Seal(plaintext[:0], nonce, plaintext, nil), dataKey.CiphertextBlob, nonce), nil
}

const encryptedEncodingFormatVersion = 1
const encryptedEncodingFormatVersionByte = byte(encryptedEncodingFormatVersion)

func encode(ciphertext []byte, ciphertextBlob []byte, nonce []byte) *string {
	writer := new(bytes.Buffer)

	writer.WriteByte(encryptedEncodingFormatVersionByte)

	_ = binary.Write(writer, binary.LittleEndian, uint16(len(ciphertext)))
	writer.Write(ciphertext)

	_ = binary.Write(writer, binary.LittleEndian, uint16(len(ciphertextBlob)))
	writer.Write(ciphertextBlob)

	_ = binary.Write(writer, binary.LittleEndian, uint16(len(nonce)))
	writer.Write(nonce)

	encoded := base64.RawURLEncoding.EncodeToString(writer.Bytes())

	return &encoded
}

func (k *KmsDataKey) Decrypt(encoded *string, encryptionContext map[string]*string) ([]byte, gomerr.Gomerr) {
	ciphertext, ciphertextBlob, nonce, ge := decode(encoded)
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
			case kms.ErrCodeNotFoundException:
				return nil, gomerr.NotFound("kms.CustomerMasterKey", *k.masterKeyId).WithCause(awsErr).AddCulprit(gomerr.Configuration)
			case kms.ErrCodeDisabledException:
				fallthrough
			case kms.ErrCodeInvalidStateException:
				return nil, gomerr.Dependency(err, input).AddNotes("kms.Decrypt()").AddCulprit(gomerr.Configuration)
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

	return k.decrypt(ciphertext, nonce, dataKey)
}

func decode(encoded *string) (ciphertext []byte, ciphertextBlob []byte, nonce []byte, ge gomerr.Gomerr) {
	encodedBytes, err := base64.RawURLEncoding.DecodeString(*encoded)
	if err != nil {
		return nil, nil, nil, gomerr.BadValue("encoded", *encoded, constraint.Base64()).WithCause(err).AddCulprit(gomerr.Client)
	}

	reader := bytes.NewBuffer(encodedBytes)

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

func (k *KmsDataKey) decrypt(ciphertext []byte, nonce []byte, dataKey *kms.DecryptOutput) ([]byte, gomerr.Gomerr) {
	block, err := aes.NewCipher(dataKey.Plaintext)
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
