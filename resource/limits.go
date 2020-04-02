package resource

import (
	"reflect"

	"github.com/jt0/gomer/gomerr"
)

type Limited interface {
	Limiter() (Limiter, *gomerr.ApplicationError)
}

type Limiter interface {
	Count(limited Limited) int
	SetCount(limited Limited, count int)
	Limit(limited Limited) (int, *gomerr.ApplicationError)
	IncrementAmount(limited Limited) int
}

type LimiterInstance struct {
	BaseInstance

	Counts map[string]int `default:"$_emptyStringToIntMap"`
}

func (l *LimiterInstance) Count(limited Limited) int {
	return l.Counts[typeName(reflect.TypeOf(limited))] // returns 0 if no value for key
}

func (l *LimiterInstance) SetCount(limited Limited, count int) {
	l.Counts[typeName(reflect.TypeOf(limited))] = count
}

func (l *LimiterInstance) Limit(_ Limited) (int, *gomerr.ApplicationError) {
	return 0, gomerr.InternalServerError("Unknown limit value")
}

func (l *LimiterInstance) IncrementAmount(_ Limited) int {
	return 1
}

func checkAndIncrement(limiter Limiter, limited Limited) *gomerr.ApplicationError {
	count := limiter.Count(limited)
	incrementAmount := limiter.IncrementAmount(limited)
	limit, ae := limiter.Limit(limited)
	if ae != nil {
		return ae
	}

	if count+incrementAmount > limit {
		return gomerr.LimitExceeded("Resource limit exceeded", map[string]interface{}{"Type": typeName(reflect.TypeOf(limited)), "Limit": limit})
	}

	limiter.SetCount(limited, count+incrementAmount)

	return nil
}

func decrement(limiter Limiter, limited Limited) *gomerr.ApplicationError {
	count := limiter.Count(limited)
	incrementAmount := limiter.IncrementAmount(limited)

	// This could go below zero, though there may be valid application cases to support this. For now, no extra checks to verify.
	limiter.SetCount(limited, count-incrementAmount)

	return nil
}
