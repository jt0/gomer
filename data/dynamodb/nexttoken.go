package dynamodb

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/jt0/gomer/crypto"
	"github.com/jt0/gomer/data"
	"github.com/jt0/gomer/gomerr"
)

type nextTokenizer struct {
	cipher crypto.Cipher
}

type nextToken struct {
	Version          uint               `json:"v"`
	Filter           map[string]*string `json:"fd"`
	LastEvaluatedKey map[string]string  `json:"lek"`
	Expiration       time.Time          `json:"exp"`
}

func (nt nextToken) ExpiresAt() time.Time {
	return nt.Expiration
}

const (
	stringPrefix = "S:"
	numberPrefix = "N:"

	NextPageToken = "NextPageToken"
)

var formatVersionExpirations = []time.Time{
	time.Date(1971, 11, 30, 3, 56, 0, 0, time.UTC), // Version "0" expired a while ago
}

var formatVersion = uint(len(formatVersionExpirations))

// TODO: add queryable details into token
func (t *nextTokenizer) tokenize(ctx context.Context, q data.Queryable, lastEvaluatedKey map[string]types.AttributeValue) (*string, gomerr.Gomerr) {
	if lastEvaluatedKey == nil {
		return nil, nil
	}

	nt := &nextToken{
		Version:          formatVersion,
		Filter:           nil, // TODO
		LastEvaluatedKey: encodeLastEvaluatedKey(lastEvaluatedKey),
		Expiration:       expirationTime(),
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

// untokenize will pull the NextPageToken from the queryable and (if there is one) decode the value. Possible errors:
//
//	gomerr.BadValueError's Type:
//	    Expired:
//	    	If the token was generated more than 24 hours ago (a hard-coded duration)
//	        If the token is using an old format version
//	    Malformed:
//	        If the token is not Base64-encoded
//	        If the token fails decryption
//
// See the crypto.kmsDataKeyDecrypter Decrypt operation for additional errors types.
func (t *nextTokenizer) untokenize(ctx context.Context, q data.Queryable) (map[string]types.AttributeValue, gomerr.Gomerr) {
	if q.NextPageToken() == nil {
		return nil, nil
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

	// only one version to check so far...
	if nt.Version != formatVersion {
		return nil, gomerr.ValueExpired(NextPageToken, formatVersionExpirations[nt.Version]).Wrap(ge)
	}

	if nt.tokenExpired() {
		return nil, gomerr.ValueExpired(NextPageToken, nt.Expiration)
	}

	// TODO: validate filter

	return decodeLastEvaluatedKey(nt.LastEvaluatedKey), nil
}

func expirationTime() time.Time {
	return time.Now().UTC().Add(time.Hour * 24)
}

func (nt *nextToken) tokenExpired() bool {
	return time.Now().UTC().After(nt.Expiration)
}

func (nt *nextToken) formatVersionExpired() bool {
	if nt.Version == formatVersion {
		return false
	}
	return time.Now().UTC().After(formatVersionExpirations[nt.Version])
}

func encodeLastEvaluatedKey(lastEvaluatedKey map[string]types.AttributeValue) map[string]string {
	lek := make(map[string]string, len(lastEvaluatedKey))

	for key, value := range lastEvaluatedKey {
		switch v := value.(type) {
		case *types.AttributeValueMemberS:
			lek[key] = fmt.Sprintf("%s%s", stringPrefix, v.Value)
		case *types.AttributeValueMemberN:
			lek[key] = fmt.Sprintf("%s%s", numberPrefix, v.Value)
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
