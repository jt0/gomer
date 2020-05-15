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
	"github.com/aws/aws-sdk-go/service/kms"
	"github.com/aws/aws-sdk-go/service/kms/kmsiface"

	"github.com/jt0/gomer/gomerr"
)

type KmsDataKey struct {
	kms         kmsiface.KMSAPI
	masterKeyId *string
	nonceLength uint8
}

func NewKmsDataKey(kmsClient kmsiface.KMSAPI, masterKeyId string, nonceLength uint8) *KmsDataKey {
	input := &kms.DescribeKeyInput{
		KeyId: aws.String(masterKeyId),
	}

	output, err := kmsClient.DescribeKey(input)
	if err != nil {
		panic("Failed to describe key: " + err.Error())
	}

	if !*output.KeyMetadata.Enabled {
		panic("Key is not in ENABLED state: " + *output.KeyMetadata.KeyState)
	}

	return &KmsDataKey{
		kms:         kmsClient,
		masterKeyId: output.KeyMetadata.KeyId,
		nonceLength: nonceLength,
	}
}

func (k *KmsDataKey) Encrypt(plaintext []byte, encryptionContext map[string]*string) (*string, *gomerr.ApplicationError) {
	input := &kms.GenerateDataKeyInput{
		KeyId:             k.masterKeyId,
		EncryptionContext: encryptionContext,
		KeySpec:           aws.String(kms.DataKeySpecAes256),
	}

	dataKey, err := k.kms.GenerateDataKey(input)
	if err != nil {
		// TODO: log
		return nil, gomerr.InternalServerError("Unable to generate data key")
	}

	return k.encrypt(plaintext, dataKey)
}

func (k *KmsDataKey) Decrypt(encoded *string, encryptionContext map[string]*string) ([]byte, *gomerr.ApplicationError) {
	ciphertext, ciphertextBlob, nonce, ae := decode(encoded)
	if ae != nil {
		return nil, ae
	}

	input := &kms.DecryptInput{
		CiphertextBlob:    ciphertextBlob,
		EncryptionContext: encryptionContext,
	}

	dataKey, err := k.kms.Decrypt(input)
	if err != nil {
		// TODO: log
		return nil, gomerr.InternalServerError("Unable to decrypt data key")
	}

	return k.decrypt(ciphertext, nonce, dataKey)
}

func (k *KmsDataKey) encrypt(plaintext []byte, dataKey *kms.GenerateDataKeyOutput) (*string, *gomerr.ApplicationError) {
	block, err := aes.NewCipher(dataKey.Plaintext)
	if err != nil {
		return nil, gomerr.InternalServerError("Error getting cipher")
	}

	nonce := make([]byte, k.nonceLength)
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		panic(err.Error())
	}

	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, gomerr.InternalServerError("Error wrapping cipher")
	}

	return encode(aead.Seal(nil, nonce, plaintext, nil), dataKey.CiphertextBlob, nonce), nil
}

func (k *KmsDataKey) decrypt(ciphertext []byte, nonce []byte, dataKey *kms.DecryptOutput) ([]byte, *gomerr.ApplicationError) {
	block, err := aes.NewCipher(dataKey.Plaintext)
	if err != nil {
		return nil, gomerr.InternalServerError("Error getting cipher")
	}

	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, gomerr.InternalServerError("Error wrapping cipher")
	}

	plaintext, err := aead.Open(nil, nonce, ciphertext, nil)
	if err != nil {
		return nil, gomerr.InternalServerError("Unable to decrypt data")
	}

	return plaintext, nil
}

const encryptedDataEncodingVersion = byte(1)

func encode(ciphertext []byte, ciphertextBlob []byte, nonce []byte) *string {
	writer := new(bytes.Buffer)

	writer.WriteByte(encryptedDataEncodingVersion)

	binary.Write(writer, binary.LittleEndian, uint16(len(ciphertext)))
	writer.Write(ciphertext)

	binary.Write(writer, binary.LittleEndian, uint16(len(ciphertextBlob)))
	writer.Write(ciphertextBlob)

	binary.Write(writer, binary.LittleEndian, uint16(len(nonce)))
	writer.Write(nonce)

	encoded := base64.RawURLEncoding.EncodeToString(writer.Bytes())

	return &encoded
}

func decode(encoded *string) (ciphertext []byte, ciphertextBlob []byte, nonce []byte, ae *gomerr.ApplicationError) {
	encodedBytes, err := base64.RawURLEncoding.DecodeString(*encoded)
	if err != nil {
		return nil, nil, nil, gomerr.BadRequest("Corrupted data - unable to decode")
	}

	reader := bytes.NewBuffer(encodedBytes)

	version, _ := reader.ReadByte()
	if version != encryptedDataEncodingVersion {
		return nil, nil, nil, gomerr.BadRequest("Unrecognized encoding version")
	}

	var length uint16

	binary.Read(reader, binary.LittleEndian, &length)
	ciphertext = make([]byte, length)
	if n, err := reader.Read(ciphertext); err != nil || n != int(length) {
		return nil, nil, nil, gomerr.BadRequest("Corrupted data - unable to read")
	}

	binary.Read(reader, binary.LittleEndian, &length)
	ciphertextBlob = make([]byte, length)
	if n, err := reader.Read(ciphertextBlob); err != nil || n != int(length) {
		return nil, nil, nil, gomerr.BadRequest("Corrupted data - unable to read")
	}

	binary.Read(reader, binary.LittleEndian, &length)
	nonce = make([]byte, length)
	if n, err := reader.Read(nonce); err != nil || n != int(length) {
		return nil, nil, nil, gomerr.BadRequest("Corrupted data - unable to read")
	}

	ae = nil

	return
}
