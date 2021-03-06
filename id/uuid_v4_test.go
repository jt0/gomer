package id

import (
	"bytes"
	"testing"
	"time"
)

func TestUuidV4IdGenerator_Generate(t *testing.T) {
	tests := []struct {
		name        string
		seed        int64
		withHyphens bool
		expected    []byte
	}{
		{"Seed_1_WithHyphen", 1, true, []byte("78629A0F-5F3F-464F-8D65-822107FCFD52")},
		{"Seed_2_WithHyphen", 2, true, []byte("21ED4CAA-C044-416F-9569-F9E2CB82822F")},
		{"Seed_3_WithHyphen", 3, true, []byte("D38967F9-31A5-4490-9C28-64602BE7FB85")},
		{"Seed_4_WithHyphen", 4, true, []byte("8D11FED4-81CA-40AF-9F26-CE1D9C7D80E2")},
		{"Seed_1_WithoutHyphen", 1, false, []byte("78629A0F5F3F464F8D65822107FCFD52")},
		{"Seed_2_WithoutHyphen", 2, false, []byte("21ED4CAAC044416F9569F9E2CB82822F")},
		{"Seed_3_WithoutHyphen", 3, false, []byte("D38967F931A544909C2864602BE7FB85")},
		{"Seed_4_WithoutHyphen", 4, false, []byte("8D11FED481CA40AF9F26CE1D9C7D80E2")},
		{"Seed_Unique_Check_Bits_1", time.Now().UnixNano(), true, nil},
		{"Seed_Unique_Check_Bits_2", time.Now().UnixNano(), true, nil},
		{"Seed_Unique_Check_Bits_3", time.Now().UnixNano(), true, nil},
		{"Seed_Unique_Check_Bits_4", time.Now().UnixNano(), true, nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			g := NewUuidV4IdGenerator(tt.seed, tt.withHyphens)
			uuid := []byte(g.Generate())
			if tt.expected != nil {
				if !bytes.Equal(uuid, tt.expected) {
					t.Errorf("Got %s, but expected  %s", uuid, tt.expected)
				}
			} else {
				// Version is a 4-bit (aka one character) value, hence comparison to '4'
				if version := uuid[14]; version != '4' {
					t.Errorf("Got version %b, but expected 0b0100", version)
				}
				// Variant is a 2-bit value, hence the conversion to bits and comparison to 0b10. By way of chars, the
				// value should be one of 8, 9, A, B (aka 0b10xx).
				if variant := hexToBits(uuid[19]) >> 2; variant != 0b10 {
					t.Errorf("Got variant %b, but expected 0b10", variant)
				}
			}
		})
	}
}
