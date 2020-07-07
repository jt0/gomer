package dynamodb

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"

	"github.com/jt0/gomer/constraint"
	"github.com/jt0/gomer/crypto"
	"github.com/jt0/gomer/data"
	"github.com/jt0/gomer/gomerr"
)

type nextTokenizer struct {
	cipher crypto.Cipher
}

type nextToken struct {
	Version          string             `json:"v"`
	Filter           map[string]*string `json:"fd"`
	LastEvaluatedKey map[string]string  `json:"lek"`
	Expiration       time.Time          `json:"exp"`
}

const (
	nextTokenFormatVersion = "1"
	stringPrefix           = "S:"
	numberPrefix           = "N:"
)

func (t *nextTokenizer) tokenize(q data.Queryable, lastEvaluatedKey map[string]*dynamodb.AttributeValue) (*string, gomerr.Gomerr) {
	if lastEvaluatedKey == nil {
		return nil, nil
	}

	nextToken := &nextToken{
		Version:          nextTokenFormatVersion,
		Filter:           nil, // TODO
		LastEvaluatedKey: encodeLastEvaluatedKey(lastEvaluatedKey),
		Expiration:       expirationTime(),
	}

	toEncrypt, err := json.Marshal(nextToken)
	if err != nil {
		return nil, gomerr.Marshal(err, nextToken).AddCulprit(gomerr.Internal)
	}

	encrypted, ge := t.cipher.Encrypt(toEncrypt, nil)
	if ge != nil {
		return nil, ge
	}

	encoded := base64.RawURLEncoding.EncodeToString(encrypted)
	return &encoded, nil
}

func (t *nextTokenizer) untokenize(q data.Queryable) (map[string]*dynamodb.AttributeValue, gomerr.Gomerr) {
	if q.NextPageToken() == nil {
		return nil, nil
	}

	encrypted, err := base64.RawURLEncoding.DecodeString(*q.NextPageToken())
	if err != nil {
		return nil, gomerr.Unmarshal(err, *q.NextPageToken(), encrypted).AddCulprit(gomerr.Client)
	}

	toUnmarshal, ge := t.cipher.Decrypt(encrypted, nil)
	if ge != nil {
		return nil, ge
	}

	nt := &nextToken{}
	if err := json.Unmarshal(toUnmarshal, nt); err != nil {
		return nil, gomerr.Unmarshal(err, toUnmarshal, nt).AddCulprit(gomerr.Internal)
	}

	if nt.Version != nextTokenFormatVersion {
		return nil, gomerr.BadValue("nextTokenFormatVersion", nt.Version, constraint.Values(nextTokenFormatVersion)).AddCulprit(gomerr.Internal)
	}

	if nt.expired() {
		return nil, gomerr.TokenExpired(nt.Expiration).AddCulprit(gomerr.Client)
	}

	// TODO: validate filter

	return decodeLastEvaluatedKey(nt.LastEvaluatedKey), nil
}

func expirationTime() time.Time {
	return time.Now().UTC().Add(time.Hour * 24)
}

func (nt *nextToken) expired() bool {
	return time.Now().UTC().After(nt.Expiration)
}

func encodeLastEvaluatedKey(lastEvaluatedKey map[string]*dynamodb.AttributeValue) map[string]string {
	lek := make(map[string]string, len(lastEvaluatedKey))

	for key, value := range lastEvaluatedKey {
		if value.S != nil {
			lek[key] = fmt.Sprintf("%s%s", stringPrefix, *value.S)
		} else if value.N != nil {
			lek[key] = fmt.Sprintf("%s%s", numberPrefix, *value.N)
		}
	}

	return lek
}

func decodeLastEvaluatedKey(lek map[string]string) map[string]*dynamodb.AttributeValue {
	var exclusiveStartKey = make(map[string]*dynamodb.AttributeValue)

	for key, value := range lek {
		if strings.HasPrefix(value, numberPrefix) {
			exclusiveStartKey[key] = &dynamodb.AttributeValue{N: aws.String(strings.TrimPrefix(value, numberPrefix))}
		} else {
			exclusiveStartKey[key] = &dynamodb.AttributeValue{S: aws.String(strings.TrimPrefix(value, stringPrefix))}
		}
	}

	return exclusiveStartKey
}
