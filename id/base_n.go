package id

import (
	"math"
	"math/rand"
	"sync"
	"time"
)

const (
	Digits     = "0123456789"
	HexDigits  = Digits + "ABCDEF"
	AlphaLower = "abcdefghijklmnopqrstuvwxyz"
	AlphaUpper = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
)

type LengthUnit = uint8

const (
	Bits LengthUnit = iota
	Chars
)

func NewBase36IdGenerator(idLength int, lengthUnit LengthUnit) Generator {
	return NewBaseNIdGenerator(idLength, lengthUnit, []byte(Digits+AlphaLower), time.Now().UnixNano())
}

func NewBase62IdGenerator(idLength int, lengthUnit LengthUnit) Generator {
	return NewBaseNIdGenerator(idLength, lengthUnit, []byte(Digits+AlphaLower+AlphaUpper), time.Now().UnixNano())
}

func NewBase16IdGenerator(idLength int, lengthUnit LengthUnit) Generator {
	return NewBaseNIdGenerator(idLength, lengthUnit, []byte(HexDigits), time.Now().UnixNano())
}

func NewBase10IdGenerator(idLength int, lengthUnit LengthUnit) Generator {
	return NewBaseNIdGenerator(idLength, lengthUnit, []byte(Digits), time.Now().UnixNano())
}

var charsPerId = map[LengthUnit]func(int, uint16) uint16{
	Bits: func(count int, base uint16) uint16 {
		return uint16(math.Ceil(float64(count) / math.Log2(float64(base))))
	},
	Chars: func(count int, _ uint16) uint16 {
		return uint16(count)
	},
}

// NewBaseNIdGenerator returns an implementation of Generator that generates id values according to the provided input.
// idLength and lengthUnit together specify the number of bits or chars that the generated id will contain, and
// encodingCharacters provides the character set used to represent the generated identifier. Note the base of the
// identifier is determined by the number of available encoding characters - if one provides 16 characters, then the
// returned value will be in base-16. The seed is the value given to the underlying random number generator. For
// convenience, the following helpers are available:
//
//   NewBase36IdGenerator - generates ids with digits and lower-case ascii letters
//   NewBase62IdGenerator - generates ids with digits and both lower- and upper-case ascii letters
//   NewBase16IdGenerator - generates ids with the hexadecimal values (0-9A-F)
//   NewBase10IdGenerator - generates ids with just digits
//
// BaseNIdGenerators are all safe for concurrent use by multiple goroutines.
func NewBaseNIdGenerator(idLength int, lengthUnit LengthUnit, encodingCharacters []byte, seed int64) Generator {
	if idLength < 1 {
		panic("idLength must be >= 1")
	}

	if lengthUnit > LengthUnit(len(charsPerId)-1) {
		panic("invalid value for lengthUnit - please use one of the LengthUnit constants")
	}

	base := uint64(len(encodingCharacters))
	if base < 2 {
		panic("must provide at least two encodingCharacters")
	}

	for _, c := range encodingCharacters {
		if c < '!' || c > '~' { // '!' is the first ascii printable character, '~' is the last one
			panic("encoding characters must only contain ascii-printable characters")
		}
	}

	numChars := charsPerId[lengthUnit](idLength, uint16(base))
	if numChars > math.MaxUint8+1 {
		panic("maximum number of characters in an id is 256")
	}

	const _64 = 64 // number of bits in a uint64

	return &baseNIdGenerator{
		base:           base,
		charsPerId:     uint8(numChars),
		charsPerUint64: uint8(math.Floor(float64(_64) / math.Log2(float64(base)))), // max is 64 when base is 2, min is 8 when base is 256
		random:         rand.New(rand.NewSource(seed)),
		encoding:       encodingCharacters,
	}
}

type baseNIdGenerator struct {
	base           uint64
	charsPerId     uint8
	charsPerUint64 uint8
	mutex          sync.Mutex
	random         *rand.Rand
	encoding       []byte
}

func (b *baseNIdGenerator) Generate() string {
	return string(b.generateChars())
}

func (b *baseNIdGenerator) generateChars() []byte {
	id := make([]byte, b.charsPerId)
	for n := b.charsPerId; n > 0; {
		b.mutex.Lock()
		random := b.random.Uint64()
		b.mutex.Unlock()

		numToEncode := min(n, b.charsPerUint64)
		for i := uint8(0); i < numToEncode; i++ {
			id[n-1] = b.encoding[random%b.base]
			random /= b.base
			n--
		}
	}

	return id[0:b.charsPerId]
}

func min(charsLeft, charsPerUint64 uint8) uint8 {
	if charsLeft < charsPerUint64 {
		return charsLeft
	} else {
		return charsPerUint64
	}
}

// // generates per configured generator (e.g. no extra bits)
// // uses low-order bits on a partial byte
// func (b baseNIdGenerator) GenerateWithSuffix(bytes []byte, numberOfBits int) string {
//
// }
//
// // encodes per chars, but
// func (b baseNIdGenerator) Encode(bytes []byte) string {
//
// }
