package constraint

import (
	"testing"
)

func TestRequiredConstraint(t *testing.T) {
	tests := []struct {
		name      string
		value     any
		wantError bool
	}{
		{"nil value", nil, true},
		{"nil pointer", (*string)(nil), true},
		{"zero int", 0, true},
		{"non-zero int", 42, false},
		{"zero string", "", true},
		{"non-zero string", "hello", false},
		{"false bool", false, true},
		{"true bool", true, false},
		{"empty slice", []string{}, false}, // empty but allocated is not zero
		{"non-empty slice", []string{"a"}, false},
		{"nil slice", []string(nil), true},
		{"empty map", map[string]string{}, false}, // empty but allocated is not zero
		{"non-empty map", map[string]string{"k": "v"}, false},
		{"nil map", map[string]string(nil), true},
		{"zero struct", struct{ Name string }{}, true},
		{"non-zero struct", struct{ Name string }{"test"}, false},
		{"pointer to zero", ptr(""), true},
		{"pointer to non-zero", ptr("hello"), false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := IsRequired.Test(tt.value)
			if tt.wantError && err == nil {
				t.Errorf("Expected error but got nil")
			}
			if !tt.wantError && err != nil {
				t.Errorf("Expected no error but got: %v", err)
			}
		})
	}
}

func TestRequiredFunc(t *testing.T) {
	tests := []struct {
		name      string
		value     any
		wantError bool
	}{
		{"nil value", nil, true},
		{"zero string", "", true},
		{"non-zero string", "hello", false},
		{"zero int", 0, true},
		{"non-zero int", 42, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v := tt.value
			c := Required(&v)
			err := c.Test(nil) // Test arg ignored, uses captured value
			if tt.wantError && err == nil {
				t.Errorf("Expected error but got nil")
			}
			if !tt.wantError && err != nil {
				t.Errorf("Expected no error but got: %v", err)
			}
		})
	}
}

func ptr[T any](v T) *T { return &v }

func TestNilConstraint(t *testing.T) {
	tests := []struct {
		name      string
		value     any
		wantError bool
		c         Constraint
	}{
		{"nil pointer - IsNil", (*struct{})(nil), false, IsNil},
		{"nil pointer - IsNotNil", (*struct{})(nil), true, IsNotNil},
		{"non-nil pointer - IsNil", &struct{}{}, true, IsNil},
		{"non-nil pointer - IsNotNil", &struct{}{}, false, IsNotNil},
		{"nil interface - IsNil", Constraint(nil), false, IsNil},
		{"nil interface - IsNotNil", Constraint(nil), true, IsNotNil},
		{"non-nil interface - IsNil", IsNil, true, IsNil},
		{"non-nil interface - IsNotNil", IsNotNil, false, IsNotNil},
		{"nil slice - IsNil", []struct{}(nil), false, IsNil},
		{"nil map - IsNil", map[string]string(nil), false, IsNil},
		{"nil channel - IsNil", (chan int)(nil), false, IsNil},
		{"non-nil-able type - IsNil", 42, true, IsNil},
		{"non-nil-able type - IsNotNil", 42, true, IsNotNil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.c.Test(tt.value)
			if tt.wantError && err == nil {
				t.Errorf("Expected error but got nil")
			}
			if !tt.wantError && err != nil {
				t.Errorf("Expected no error but got: %v", err)
			}
		})
	}
}
