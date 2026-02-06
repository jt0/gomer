package constraint

import (
	"testing"
)

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
