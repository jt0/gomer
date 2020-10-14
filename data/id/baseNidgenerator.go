package id

import (
	"math"
	"math/rand"
)

//goland:noinspection SpellCheckingInspection
var AsciiEncodingCharacters = []byte("0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

type CharsOrBits func(int, uint64) (int, int)

func Chars(count int, base uint64) (bits int, chars int) {
	return int(math.Floor(float64(count) * math.Log2(float64(base)))), count
}

func Bits(count int, base uint64) (bits int, chars int) {
	return count, int(math.Ceil(float64(count) / math.Log2(float64(base))))
}

// TODO: add support for either count(chars) or count(bits)
func NewBase36IdGenerator(count int, charsOrBits CharsOrBits) Generator {
	return NewBaseNIdGenerator(count, charsOrBits, AsciiEncodingCharacters[0:36])
}

func NewBase62IdGenerator(count int, charsOrBits CharsOrBits) Generator {
	return NewBaseNIdGenerator(count, charsOrBits, AsciiEncodingCharacters)
}

func NewBase16IdGenerator(count int, charsOrBits CharsOrBits) Generator {
	return NewBaseNIdGenerator(count, charsOrBits, AsciiEncodingCharacters[0:16])
}

func NewBase10IdGenerator(count int, charsOrBits CharsOrBits) Generator {
	return NewBaseNIdGenerator(count, charsOrBits, AsciiEncodingCharacters[0:10])
}

func NewBaseNIdGenerator(count int, charsOrBits CharsOrBits, encodingCharacters []byte) Generator {
	if count < 1 {
		panic("count must be >= 1")
	}

	base := uint64(len(encodingCharacters))
	if base < 2 {
		panic("must provide at least two encoding encodingCharacters")
	}

	bitsPerId, charsPerId := charsOrBits(count, base)

	return baseNIdGenerator{
		base:           base,
		bitsPerId:      bitsPerId,
		charsPerId:     charsPerId,
		charsPerUint64: int(math.Floor(float64(bitsPerUint64) / math.Log2(float64(base)))),
		chars:          encodingCharacters,
	}
}

type baseNIdGenerator struct {
	base           uint64
	bitsPerId      int
	charsPerId     int
	charsPerUint64 int
	chars          []byte
}

const (
	bitsPerUint64 = 64
)

func (b baseNIdGenerator) Generate() string {
	id := make([]byte, 0, b.charsPerId)
	for len(id) < b.charsPerId {
		random := rand.Uint64()

		for i := 0; i < b.charsPerUint64; i++ {
			id = append(id, b.chars[random%b.base])
			random /= b.base
		}
	}

	return string(id[0:b.charsPerId])
}

//// generates per configured generator (e.g. no extra bits)
//// uses low-order bits on a partial byte
//func (b baseNIdGenerator) GenerateWithSuffix(bytes []byte, numberOfBits int) string {
//
//}
//
//// encodes per chars, but
//func (b baseNIdGenerator) Encode(bytes []byte) string {
//
//}
