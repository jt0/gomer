package id

import (
	"testing"
)

func Test_baseNIdGenerator_Characters(t *testing.T) {
	//goland:noinspection SpellCheckingInspection
	tests := []struct {
		name         string
		generator    func(count int, charsOrBits CharsOrBits) Generator
		want         string
		bitsPerId    int
		charsPerId   int
		bitsPerChars int // can be larger that bitsPerId as there can be "extra room" w/in the last char for more bits
	}{
		{"Base10", NewBase10IdGenerator, "0123456789", 128, 39, 129},
		{"Base16", NewBase16IdGenerator, "0123456789abcdef", 128, 32, 128},
		{"Base36", NewBase36IdGenerator, "0123456789abcdefghijklmnopqrstuvwxyz", 128, 25, 129},
		{"Base62", NewBase62IdGenerator, "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ", 128, 22, 130},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			g := tt.generator(tt.bitsPerId, Bits).(baseNIdGenerator)
			if got := g.chars; string(got) != tt.want {
				t.Errorf("chars = %v, want %v", string(got), tt.want)
			}
			if _, charsPerId := Bits(tt.bitsPerId, g.base); charsPerId != tt.charsPerId {
				t.Errorf("charsPerId = %d, want %d", charsPerId, tt.charsPerId)
			}
			if bitsPerChars, _ := Chars(tt.charsPerId, g.base); bitsPerChars != tt.bitsPerChars {
				t.Errorf("bitsPerId = %d, want %d", bitsPerChars, tt.bitsPerChars)
			}
		})
	}
}

//func Test_baseNIdGenerator_Generate(t *testing.T) {
//	type fields struct {
//		bitsPerId      int
//		base           uint64
//		charsPerId     int
//		charsPerUint64 int
//		chars          []byte
//	}
//	tests := []struct {
//		name   string
//		fields fields
//		want   string
//	}{
//		// TODO: Add test cases.
//	}
//	for _, tt := range tests {
//		t.Run(tt.name, func(t *testing.T) {
//			g := baseNIdGenerator{
//				bitsPerId:      tt.fields.bitsPerId,
//				base:           tt.fields.base,
//				charsPerId:     tt.fields.charsPerId,
//				charsPerUint64: tt.fields.charsPerUint64,
//				chars:          tt.fields.chars,
//			}
//			if got := g.Generate(); got != tt.want {
//				t.Errorf("Generate() = %v, want %v", got, tt.want)
//			}
//		})
//	}
//}
