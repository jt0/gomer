package limiter

import (
	"testing"

	"github.com/jt0/gomer/gomerr"
	"github.com/jt0/gomer/limiter"
)

const (
	aLimit        = "MyLimit"
	aLimitDefault = 5
)

func TestCheckThenIncrement(t *testing.T) {
	l, ae := limiter.New([]limiter.Limit{{aLimit, 0, aLimitDefault}})
	shouldPass(ae, t)

	for i := 0; i <= aLimitDefault; i++ {
		l.CheckThenIncrementByOne(aLimit)
	}

	shouldFail(l.CheckThenIncrementByOne(aLimit), t, "Should have failed on increment past limit")
}

func TestDuplicateLimit(t *testing.T) {
	_, ae := limiter.New([]limiter.Limit{{aLimit, 0, aLimitDefault}, {aLimit, aLimitDefault, aLimitDefault}})
	shouldFail(ae, t, "Expected error from duplicate limit name")
}

func TestIncrementDecrementIncrement(t *testing.T) {
	l, ae := limiter.New([]limiter.Limit{{aLimit, 0, aLimitDefault}})
	shouldPass(ae, t)

	for i := 0; i <= aLimitDefault; i++ {
		l.CheckThenIncrementByOne(aLimit)
	}

	for i := 0; i <= aLimitDefault; i++ {
		l.DecrementByOne(aLimit)
	}

	shouldFail(l.CheckThenIncrementByOne(aLimit), t, "Should have failed on increment past limit")
}

func TestIncrementByDecrementByIncrementBy(t *testing.T) {
	// start count @ 4
	l, ae := limiter.New([]limiter.Limit{{aLimit, 4, aLimitDefault}})
	shouldPass(ae, t)

	// increment by 3 (fail)
	shouldFail(l.CheckThenIncrementBy(aLimit, 3), t, "Should have failed on increment past limit")
	// decrement by 2 (count should be @ 2)
	shouldPass(l.DecrementBy(aLimit, 2), t)
	// increment by 3 (succeed)
	shouldPass(l.CheckThenIncrementBy(aLimit, 3), t)
}

func TestInitialNegativeLimit(t *testing.T) {
	_, ae := limiter.New([]limiter.Limit{{aLimit, 0, -aLimitDefault}})
	shouldFail(ae, t, "Expected error from negative limit")
}

func TestInitialNegativeCount(t *testing.T) {
	_, ae := limiter.New([]limiter.Limit{{aLimit, -2, aLimitDefault}})
	shouldFail(ae, t, "Expected error from negative count")
}

func TestUpdateLimit(t *testing.T) {
	// Limit w/ count == limit
	l, ae := limiter.New([]limiter.Limit{{aLimit, aLimitDefault, aLimitDefault}})
	shouldPass(ae, t)

	shouldFail(l.CheckThenIncrementByOne(aLimit), t, "Count should have been at limit")
	shouldPass(l.UpdateLimit(aLimit, aLimitDefault + 1), t)
	shouldPass(l.CheckThenIncrementByOne(aLimit), t)
}

func TestUpdateLimitBelowCount(t *testing.T) {
	// Limit w/ count == limit
	l, ae := limiter.New([]limiter.Limit{{aLimit, aLimitDefault, aLimitDefault}})
	shouldPass(ae, t)

	shouldFail(l.UpdateLimit(aLimit, aLimitDefault - 1), t, "Limit can't go below count")
}


func TestUpdateLimitWithNegative(t *testing.T) {
	// Limit w/ count == limit
	l, ae := limiter.New([]limiter.Limit{{aLimit, aLimitDefault, aLimitDefault}})
	shouldPass(ae, t)

	shouldFail(l.UpdateLimit(aLimit, -1), t, "Limit can't be negative")
}

func TestUpdateCount(t *testing.T) {
	// Limit w/ count == limit
	l, ae := limiter.New([]limiter.Limit{{aLimit, aLimitDefault, aLimitDefault}})
	shouldPass(ae, t)

	shouldFail(l.CheckThenIncrementByOne(aLimit), t, "Count should have been at limit")
	shouldPass(l.UpdateCount(aLimit, aLimitDefault - 1), t)
	shouldPass(l.CheckThenIncrementByOne(aLimit), t)
}

func TestUpdateCountAboveLimit(t *testing.T) {
	// Limit w/ count == limit
	l, ae := limiter.New([]limiter.Limit{{aLimit, aLimitDefault, aLimitDefault}})
	shouldPass(ae, t)

	shouldFail(l.UpdateCount(aLimit, aLimitDefault + 1), t, "Count can't go above limit")
}


func TestUpdateCountWithNegative(t *testing.T) {
	// Limit w/ count == limit
	l, ae := limiter.New([]limiter.Limit{{aLimit, aLimitDefault, aLimitDefault}})
	shouldPass(ae, t)

	shouldFail(l.UpdateCount(aLimit, -1), t, "Count can't be negative")
}

func shouldFail(ae *gomerr.ApplicationError, t *testing.T, msg string) {
	if ae == nil {
		t.Fatal(msg)
	}
}

func shouldPass(ae *gomerr.ApplicationError, t *testing.T) {
	if ae != nil {
		t.Fatal(ae)
	}
}

// Note: okay for now to incrementBy/decrementBy 0 or w/ negative values