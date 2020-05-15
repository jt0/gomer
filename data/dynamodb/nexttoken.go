package dynamodb

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"

	"github.com/jt0/gomer/crypto"
	"github.com/jt0/gomer/data"
	"github.com/jt0/gomer/gomerr"
)

type nextTokenizer struct {
	cipher crypto.Cipher
}

type nextToken struct {
	Version          int                `json:"v"`
	Filter           map[string]*string `json:"fd"`
	LastEvaluatedKey map[string]string  `json:"lek"`
	Expiration       time.Time          `json:"exp"`
}

const (
	v1           = 1
	stringPrefix = "S:"
	numberPrefix = "N:"
)

func (t *nextTokenizer) tokenize(q data.Queryable, lastEvaluatedKey map[string]*dynamodb.AttributeValue) (*string, *gomerr.ApplicationError) {
	if lastEvaluatedKey == nil {
		return nil, nil
	}

	nextToken := &nextToken{
		Version:          v1,
		Filter:           nil, // TODO
		LastEvaluatedKey: encodeLastEvaluatedKey(lastEvaluatedKey),
		Expiration:       expirationTime(),
	}

	toEncrypt, _ := json.Marshal(nextToken)

	return t.cipher.Encrypt(toEncrypt, nil)
}

func (t *nextTokenizer) untokenize(q data.Queryable) (map[string]*dynamodb.AttributeValue, *gomerr.ApplicationError) {
	if q.NextPageToken() == nil {
		return nil, nil
	}

	toUnmarshal, ae := t.cipher.Decrypt(q.NextPageToken(), nil)
	if ae != nil {
		return nil, ae
	}

	nt := &nextToken{}
	if err := json.Unmarshal(toUnmarshal, nt); err != nil {
		return nil, gomerr.InternalServerError("Unable to next token data")
	}

	if nt.Version != v1 {
		return nil, gomerr.InternalServerError("Unexpected next token version ")
	}

	if expired(nt.Expiration) {
		return nil, gomerr.PaginationTokenExpired()
	}

	// TODO: validate filter

	return decodeLastEvaluatedKey(nt.LastEvaluatedKey), nil
}

func expirationTime() time.Time {
	return time.Now().UTC().Add(time.Hour * 24)
}

func expired(expiration time.Time) bool {
	return time.Now().UTC().After(expiration)
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
