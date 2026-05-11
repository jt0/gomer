package constraint

import (
	"reflect"
	"testing"
)

func Test_constraintFor(t *testing.T) {
	type test struct {
		name       string
		constraint string
		expected   string
	}
	tests := []test{
		{"len", "len(1,2)", "lengthBetween(1, 2)"},
		{"required", "required", "isRequired"},
		{"or", "or(required,len(1,2))", "or(isRequired, lengthBetween(1, 2))"},
		{"map_1", "map(notZero,intBetween(1,100000))", "map(isNotZero, int(gte(1), lte(100000)))"},
		{"map_2", "map(len(3),struct)", "map(lengthEquals(3), struct)"},
		{"eq", "eq(1)", "equals(1)"},
		{"map_validKey_struct_entries", "map(notNil,struct),entries(notNil)", "and(map(isNotNil, struct), entries(isNotNil))"},
		{"map_validKey_struct_entries_strictVariants", "map(notNil,struct),entries(notNil)", "and(map(isNotNil, struct), entries(isNotNil))"},
		{"regexp", "regexp(^[^\\\\n]{0,64}$)", "regexp(^[^\\\\n]{0,64}$)"},
		{"maxLen_map_validKey_struct", "maxLen(25),map(notNil,struct)", "and(lengthMax(25), map(isNotNil, struct))"},
		{"maxLen_1024", "maxLen(1024)", "lengthMax(1024)"},
		{"maxLen_entries_satisfies", "maxLen(25),entries(notNil)", "and(lengthMax(25), entries(isNotNil))"},
		{"or_not_entries_requires", "or(not(notNil),entries(notNil))", "or(not(isNotNil), entries(isNotNil))"},
		{"elements_struct_validVariants", "elements(struct),notNil", "and(elements(struct), isNotNil)"},
		{"struct_constraintsMatchType_patternEnumMutuallyExclusive", "struct,notNil,notNil", "and(struct, isNotNil, isNotNil)"},
		{"or_nil_oneof", "or(nil,oneof(planned))", "or(isNil, oneOf(planned))"},
		{"or_nil_isDate", "or(nil,notNil)", "or(isNil, isNotNil)"},
		{"oneof_types", "oneof(string,number,boolean,array)", "oneOf(string, number, boolean, array)"},
		{"float_gte", "float(gte,1.23)", "float_gte(1.23)"},
		{"or_nil_isRegexp", "or(nil,isRegexp)", "or(isNil, isRegexp)"},
		{"or_nil_len", "or(nil,len(1,100))", "or(isNil, lengthBetween(1, 100))"},
		{"struct_constraintsMatchType", "struct,notNil", "and(struct, isNotNil)"},
		{"oneof_string_number", "oneof(string,number)", "oneOf(string, number)"},
		{"regexp_variant_name", "regexp(^[^\\\\n]{1,64}$)", "regexp(^[^\\\\n]{1,64}$)"},
		{"maxLen_entries_satisfies_variant", "maxLen(25),entries(notNil)", "and(lengthMax(25), entries(isNotNil))"},
		{"maxLen_16384", "maxLen(16384)", "lengthMax(16384)"},
		{"or_isZero_entries_requires", "or(notNil,entries(notNil))", "or(isNotNil, entries(isNotNil))"},
		{"elements_nested_param", "elements(len(1,16))", "elements(lengthBetween(1, 16))"},
		{"len_elements_nested", "len(0,10),elements(len(1,16))", "and(lengthBetween(0, 10), elements(lengthBetween(1, 16)))"},
		{"or_nil_regexp_with_braces", `or(nil,regexp(^\\$\\{[a-zA-Z][a-zA-Z0-9]{0,15}\\}$))`, `or(isNil, regexp(^\\$\\{[a-zA-Z][a-zA-Z0-9]{0,15}\\}$))`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sf, _ := reflect.TypeFor[test]().FieldByName("name")
			c, err := constraintFor(tt.constraint, none, sf)
			if err != nil {
				t.Fatalf("constraintFor() error: %v", err)
			}
			if got := c.String(); got != tt.expected {
				t.Errorf("constraintFor().String() = %v, want %v", got, tt.expected)
			}
		})
	}
}
