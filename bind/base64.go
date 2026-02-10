package bind

import (
	b64 "encoding/base64"
	"reflect"

	"github.com/jt0/gomer/gomerr"
	"github.com/jt0/gomer/structs"
)

func init() {
	if ge := structs.RegisterToolFunctions(map[string]structs.ToolFunction{
		"$_b64Decode":       b64DecodeFunction(b64.StdEncoding),
		"$_b64RawDecode":    b64DecodeFunction(b64.RawStdEncoding),
		"$_b64UrlDecode":    b64DecodeFunction(b64.URLEncoding),
		"$_b64RawUrlDecode": b64DecodeFunction(b64.RawURLEncoding),
		"$_b64Encode":       b64EncodeFunction(b64.StdEncoding),
		"$_b64RawEncode":    b64EncodeFunction(b64.RawStdEncoding),
		"$_b64UrlEncode":    b64EncodeFunction(b64.URLEncoding),
		"$_b64RawUrlEncode": b64EncodeFunction(b64.RawURLEncoding),
	}); ge != nil {
		panic(ge.String())
	}
}

func b64DecodeFunction(encoding *b64.Encoding) structs.ToolFunction {
	return func(_ reflect.Value, fv reflect.Value, _ structs.ToolContext) (any, gomerr.Gomerr) {
		if !fv.IsValid() || fv.IsZero() {
			return nil, nil
		}

		bytes, ok := fv.Interface().([]byte)
		if !ok {
			return nil, gomerr.Configuration("b64Decode requires input value to already be an []byte, not " + fv.Type().String())
		}

		decoded := make([]byte, encoding.DecodedLen(len(bytes)))
		n, err := encoding.Decode(decoded, bytes)
		if err != nil {
			return nil, gomerr.Unprocessable("Unable to base64 decode the given data", bytes).Wrap(err)
		}

		return decoded[:n], nil
	}
}

func b64EncodeFunction(encoding *b64.Encoding) structs.ToolFunction {
	return func(_ reflect.Value, fv reflect.Value, _ structs.ToolContext) (any, gomerr.Gomerr) {
		if !fv.IsValid() || fv.IsZero() {
			return nil, nil
		}

		bytes, ok := fv.Interface().([]byte)
		if !ok {
			return nil, gomerr.Unprocessable("Field type must be '[]byte'", fv.Type().String())
		}

		return encoding.EncodeToString(bytes), nil
	}
}
