package bind_test

import (
	"testing"

	"github.com/jt0/gomer/_test/assert"
	"github.com/jt0/gomer/bind"
)

func TestCopyWithOptions(t *testing.T) {
	b1 := bind.NewConfiguration()
	b2 := bind.CopyConfigurationWithOptions(b1, bind.IncludeEmpty, bind.EmptyDirectiveIncludesField)

	assert.NotEquals(t, b1, b2)
}
