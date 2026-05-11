package constraint

import (
	"testing"

	"github.com/jt0/gomer/structs"
)

type accessMode string

type fsSpec struct {
	AccessMode accessMode `validate:"oneof(read_only,read_write)"`
	CreateMode *string    `validate:"or(field($.AccessMode,neq,read_write),required)"`
}

type rangeSpec struct {
	Min int `validate:""`
	Max int `validate:"field($.Min,lte,$.Max)"`
}

type boolSpec struct {
	Enabled  bool   `validate:""`
	Required string `validate:"or(field($.Enabled,eq,false),required)"`
}

func TestFieldTest(t *testing.T) {
	vt := NewValidationTool(structs.StructTagDirectiveProvider{"validate"})

	tests := []struct {
		name    string
		value   any
		wantErr bool
	}{
		{
			"access_read_only_nil_createmode_passes",
			&fsSpec{AccessMode: "read_only", CreateMode: nil},
			false,
		},
		{
			"access_read_write_nil_createmode_fails",
			&fsSpec{AccessMode: "read_write", CreateMode: nil},
			true,
		},
		{
			"access_read_write_with_createmode_passes",
			&fsSpec{AccessMode: "read_write", CreateMode: strPtr("private")},
			false,
		},
		{
			"range_min_lte_max_passes",
			&rangeSpec{Min: 1, Max: 10},
			false,
		},
		{
			"range_min_eq_max_passes",
			&rangeSpec{Min: 5, Max: 5},
			false,
		},
		{
			"range_min_gt_max_fails",
			&rangeSpec{Min: 10, Max: 1},
			true,
		},
		{
			"bool_enabled_false_empty_required_passes",
			&boolSpec{Enabled: false, Required: ""},
			false,
		},
		{
			"bool_enabled_true_empty_required_fails",
			&boolSpec{Enabled: true, Required: ""},
			true,
		},
		{
			"bool_enabled_true_with_required_passes",
			&boolSpec{Enabled: true, Required: "hello"},
			false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := Validate(tt.value, vt)
			if (err != nil) != tt.wantErr {
				t.Errorf("Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func strPtr(s string) *string { return &s }
