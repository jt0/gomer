package id

import (
	"time"
)

// UUID specification is found in RFC 4122 (https://tools.ietf.org/html/rfc4122). A variant 4 UUID is one that includes
// 122 random bits, and 6 fixed bits (2 for the UUID variant, 4 for the UUID version).
//
// Section 4.1.2 of the RFC (https://tools.ietf.org/html/rfc4122#section-4.1.2) has a similar diagram of the bit layout
// for a UUID. This has been modified in two ways. First, the bit ordering has been reversed to be more natural to the
// binary representation of a byte (e.g. a byte with only the least significant bit set is 0b00000001). The second
// difference is that this diagram shows what each byte contains so as to be explicit about where the UUID variant
// (indicated by VAR) and version (indicated by VERSION) values go. The bits for variant and to indicate v4 are found
// below.
//
//   Octet → 0               1               2               3
// Chars     7 6 5 4 3 2 1 0 7 6 5 4 3 2 1 0 7 6 5 4 3 2 1 0 7 6 5 4 3 2 1 0
//   ↓      +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
//  0-7     | time_low (3)  : time_low (2)  : time_low (1)  : time_low (0)  |    most
//          +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
//  8-15    | time_mid (1)  : time_mid (0)  :VERSION| t_h+4 :  time_hi (0)  |
//          +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
// 16-23    |VAR| clk_seq_hi|  clk_seq_low  :   node (1)    :   node (0)    |
//          +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
// 24-31    |   node (5)    :   node (4)    :   node (3)    :   node (2)    |    least
//          +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+      ↑
//                most                                            least      ← Significance

var UuidV4 = NewUuidV4IdGenerator(time.Now().UnixNano(), true)

// NewUuidV4IdGenerator returns a generator that generate versions 4 (i.e. randomly generated) UUIDs. The 'seed'
// parameter specifies what value to use to start the production of random values, and 'withHyphens' determines if the
// generated values have the common hyphen separators between the different UUID fields. For convenience, a pre-seeded
// UuidV4 generator (with hyphens) has been predefined.
//
// UuidV4IdGenerators are safe for concurrent use by multiple goroutines.
func NewUuidV4IdGenerator(seed int64, withHyphens bool) Generator {
	return uuidV4{
		base16:      NewBaseNIdGenerator(32, Chars, []byte(HexDigits), seed).(*baseNIdGenerator),
		withHyphens: withHyphens,
	}
}

type uuidV4 struct {
	base16      *baseNIdGenerator
	withHyphens bool
}

const (
	versionByte = 12 // per diagram above (upper left is byte '0')
	variantByte = 16 // per diagram above (upper left is byte '0')

	v4VersionBits = 0b0100
	variantBits   = 0b10 << 2
)

func (u uuidV4) Generate() string {
	chars := u.base16.generateChars()

	chars[versionByte] = u.base16.encoding[v4VersionBits]                                 // Should be encoded as '4'
	chars[variantByte] = u.base16.encoding[variantBits|hexToBits(chars[variantByte])&0x3] // variant bits ORed with the current char's bottom 2 bits

	if u.withHyphens {
		s := make([]byte, 36)
		copy(s, chars[0:8])
		s[8] = '-'
		copy(s[9:], chars[8:12])
		s[13] = '-'
		copy(s[14:], chars[12:16])
		s[18] = '-'
		copy(s[19:], chars[16:20])
		s[23] = '-'
		copy(s[24:], chars[20:32])

		return string(s)
	} else {
		return string(chars)
	}
}

func hexToBits(b byte) byte {
	if b >= '0' || b <= '9' {
		return b - 48 // '0'
	} else if b >= 'A' || b <= 'F' {
		return b - 75 // 'A' + 10
	} else {
		panic("provided value is not encoded in hexadecimal: " + string(b))
	}
}
