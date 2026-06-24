package dynamodb

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/jt0/gomer/crypto"
	"github.com/jt0/gomer/data"
	"github.com/jt0/gomer/gomerr"
)

type NextTokenizer interface {
	Tokenize(ctx context.Context, q data.Queryable, lastEvaluatedKey map[string]types.AttributeValue) (*string, gomerr.Gomerr)
	Untokenize(ctx context.Context, q data.Queryable) (map[string]types.AttributeValue, gomerr.Gomerr)
}

type nextTokenizer struct {
	cipher crypto.Cipher
}

// TODO: add queryable details into token
type nextToken struct {
	Filter           map[string]*string `json:"fd"`
	LastEvaluatedKey map[string]string  `json:"lek"`
	Expiration       time.Time          `json:"exp"`
}

func (nt *nextToken) ExpiresAt() time.Time {
	return nt.Expiration
}

const NextPageToken = "NextPageToken"

func (t *nextTokenizer) Tokenize(ctx context.Context, q data.Queryable, lastEvaluatedKey map[string]types.AttributeValue) (*string, gomerr.Gomerr) {
	if lastEvaluatedKey == nil {
		return nil, nil
	}

	if t.cipher.Encrypter == nil {
		return nil, gomerr.Configuration("NextTokenCipher.Encrypter not configured")
	}

	nt := &nextToken{
		Filter:           nil, // TODO
		LastEvaluatedKey: encodeLastEvaluatedKey(lastEvaluatedKey),
		Expiration:       time.Now().UTC().Add(time.Hour * 24),
	}

	toEncrypt, err := json.Marshal(nt)
	if err != nil {
		return nil, gomerr.Marshal(NextPageToken, nt).Wrap(err)
	}

	// TODO: provide an encryption context - probably w/ q data
	encrypted, ge := t.cipher.Encrypt(ctx, toEncrypt, nil)
	if ge != nil {
		return nil, ge
	}

	encoded := base64.RawURLEncoding.EncodeToString(encrypted)
	return &encoded, nil
}

// Untokenize will pull the NextPageToken from the queryable and (if there is one) decode the value. Possible errors:
//
//	gomerr.BadValueError's Type:
//	    Expired:
//	    	If the token was generated more than 24 hours ago (a hard-coded duration)
//	    Malformed:
//	        If the token is not Base64-encoded
//	        If the token fails decryption
//
// See the crypto.kmsDataKeyDecrypter Decrypt operation for additional errors types.
func (t *nextTokenizer) Untokenize(ctx context.Context, q data.Queryable) (map[string]types.AttributeValue, gomerr.Gomerr) {
	if q.NextPageToken() == nil {
		return nil, nil
	}

	if t.cipher.Decrypter == nil {
		return nil, gomerr.Configuration("NextTokenCipher.Decrypter not configured")
	}

	encrypted, err := base64.RawURLEncoding.DecodeString(*q.NextPageToken())
	if err != nil {
		return nil, gomerr.MalformedValue(NextPageToken, nil).Wrap(err)
	}

	toUnmarshal, ge := t.cipher.Decrypt(ctx, encrypted, nil)
	if ge != nil {
		return nil, gomerr.MalformedValue(NextPageToken, nil).Wrap(ge)
	}

	nt := &nextToken{}
	if err = json.Unmarshal(toUnmarshal, nt); err != nil {
		return nil, gomerr.MalformedValue(NextPageToken, nil).Wrap(err)
	}

	if time.Now().UTC().After(nt.Expiration) {
		return nil, gomerr.ValueExpired(NextPageToken, nt.Expiration)
	}

	// TODO: validate filter

	return decodeLastEvaluatedKey(nt.LastEvaluatedKey), nil
}

const (
	stringPrefix = "S:"
	numberPrefix = "N:"
)

func encodeLastEvaluatedKey(lastEvaluatedKey map[string]types.AttributeValue) map[string]string {
	lek := make(map[string]string, len(lastEvaluatedKey))

	for key, value := range lastEvaluatedKey {
		switch v := value.(type) {
		case *types.AttributeValueMemberS:
			lek[key] = stringPrefix + v.Value
		case *types.AttributeValueMemberN:
			lek[key] = numberPrefix + v.Value
		}
	}

	return lek
}

func decodeLastEvaluatedKey(lek map[string]string) map[string]types.AttributeValue {
	var exclusiveStartKey = make(map[string]types.AttributeValue)

	for key, value := range lek {
		if strings.HasPrefix(value, numberPrefix) {
			exclusiveStartKey[key] = &types.AttributeValueMemberN{
				Value: strings.TrimPrefix(value, numberPrefix),
			}
		} else {
			exclusiveStartKey[key] = &types.AttributeValueMemberS{
				Value: strings.TrimPrefix(value, stringPrefix),
			}
		}
	}

	return exclusiveStartKey
}
