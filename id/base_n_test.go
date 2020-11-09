package id

import (
	"testing"
)

func TestBaseNIdGenerator_CharsPerId(t *testing.T) {
	//goland:noinspection SpellCheckingInspection
	tests := []struct {
		name       string
		generator  func(idLength int, lengthUnit LengthUnit) Generator
		bitsPerId  int
		charsPerId uint16
	}{
		{"Base10", NewBase10IdGenerator, 128, 39},
		{"Base16", NewBase16IdGenerator, 128, 32},
		{"Base36", NewBase36IdGenerator, 128, 25},
		{"Base62", NewBase62IdGenerator, 128, 22},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			g := tt.generator(tt.bitsPerId, Bits).(*baseNIdGenerator)
			if charsPerId := charsPerId[Bits](tt.bitsPerId, uint16(g.base)); charsPerId != tt.charsPerId {
				t.Errorf("charsPerId = %d, want %d", charsPerId, tt.charsPerId)
			}
		})
	}
}

// func Test_baseNIdGenerator_Generate(t *testing.T) {
// 	type fields struct {
// 		bitsPerId      int
// 		base           uint64
// 		charsPerId     int
// 		charsPerUint64 int
// 		chars          []byte
// 	}
// 	tests := []struct {
// 		name   string
// 		fields fields
// 		want   string
// 	}{
// 	}
// 	for _, tt := range tests {
// 		t.Run(tt.name, func(t *testing.T) {
// 			g := baseNIdGenerator{
// 				bitsPerId:      tt.fields.bitsPerId,
// 				base:           tt.fields.base,
// 				charsPerId:     tt.fields.charsPerId,
// 				charsPerUint64: tt.fields.charsPerUint64,
// 				chars:          tt.fields.chars,
// 			}
// 			if got := g.Generate(); got != tt.want {
// 				t.Errorf("Generate() = %v, want %v", got, tt.want)
// 			}
// 		})
// 	}
// }
