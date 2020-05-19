package resource

import (
	"reflect"
	"strings"

	"github.com/jt0/gomer/gomerr"
	"github.com/jt0/gomer/util"
)

type Limited interface {
	Limiter() (Limiter, *gomerr.ApplicationError)
}

type Limiter interface {
	Count(limited Limited) int
	SetCount(limited Limited, count int)
	Override(limited Limited) int
	SetOverride(limited Limited, override int)
	DefaultLimit(limited Limited) int
	IncrementAmount(limited Limited) int

	dirty() bool
	clearDirty()
}

type BaseLimiter struct {
	Counts    map[string]int
	Overrides map[string]int

	dirty_ bool
}

func (l *BaseLimiter) Count(limited Limited) int {
	if l.Counts == nil {
		return 0
	}

	return l.Counts[util.UnqualifiedTypeName(reflect.TypeOf(limited))] // returns 0 if no value for key
}

func (l *BaseLimiter) SetCount(limited Limited, count int) {
	if l.Counts == nil {
		l.Counts = make(map[string]int)
	}

	l.Counts[util.UnqualifiedTypeName(reflect.TypeOf(limited))] = count

	l.dirty_ = true
}

func (l *BaseLimiter) Override(limited Limited) int {
	if l.Overrides == nil {
		return 0
	}

	return l.Overrides[util.UnqualifiedTypeName(reflect.TypeOf(limited))] // returns 0 if no value for key
}

func (l *BaseLimiter) SetOverride(limited Limited, override int) {
	if l.Overrides == nil {
		l.Overrides = make(map[string]int)
	}

	if override > l.DefaultLimit(limited) {
		l.Overrides[util.UnqualifiedTypeName(reflect.TypeOf(limited))] = override
	} else {
		delete(l.Overrides, util.UnqualifiedTypeName(reflect.TypeOf(limited)))
	}

	l.dirty_ = true
}

func (l *BaseLimiter) DefaultLimit(_ Limited) int {
	return 0
}

func (l *BaseLimiter) IncrementAmount(_ Limited) int {
	return 1
}

func (l *BaseLimiter) dirty() bool {
	return l.dirty_
}

func (l *BaseLimiter) clearDirty() {
	l.dirty_ = false
}

type limitAction func(Limiter, Limited) *gomerr.ApplicationError

func checkAndIncrement(limiter Limiter, limited Limited) *gomerr.ApplicationError {
	count := limiter.Count(limited)
	incrementAmount := limiter.IncrementAmount(limited)
	limit := limitFor(limiter, limited)

	if count+incrementAmount > limit {
		return gomerr.LimitExceeded("Resource limit exceeded", map[string]interface{}{"Type": util.UnqualifiedTypeName(reflect.TypeOf(limited)), "Limit": limit})
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

func limitFor(limiter Limiter, limited Limited) int {
	overrideLimit := limiter.Override(limited)
	defaultLimit := limiter.DefaultLimit(limited)

	if overrideLimit > defaultLimit {
		return overrideLimit
	}

	return defaultLimit
}

func limit(limitAction limitAction, i Instance) (Limiter, *gomerr.ApplicationError) {
	limited, ok := i.(Limited)
	if !ok {
		return nil, nil
	}

	limiter, ae := limited.Limiter()
	if ae != nil {
		return nil, ae
	}

	limiterInstance, ok := limiter.(Instance)
	if !ok {
		return nil, gomerr.InternalServerError("limiter is not an instance type")
	}

	// If the metadata isn't set, then this is a new object and needs to be loaded
	var loaded bool
	if limiterInstance.metadata() == nil {
		resourceType := strings.ToLower(util.UnqualifiedTypeName(reflect.TypeOf(limiter)))
		metadata, ok := resourceMetadata[resourceType]
		if !ok {
			return nil, gomerr.BadRequest("Unknown type: " + resourceType)
		}

		limiterInstance.setMetadata(metadata)
		limiterInstance.setSubject(i.Subject())
		limiterInstance.OnSubject()

		// TODO: cache in case needed by more than one resource...
		// TODO: need an optimistic lock mechanism to avoid overwriting
		// TODO: check to see if should not be read...
		if ae := limiterInstance.metadata().dataStore.Read(limiterInstance); ae != nil {
			return nil, ae
		}

		loaded = true
	}

	if ae := limitAction(limiter, limited); ae != nil {
		return nil, ae
	}

	// If we didn't load the limiterInstance, we'll let other code handle the save
	if !loaded {
		limiter = nil
	}

	return limiter, nil
}

func saveLimiter(limiter Limiter, ae *gomerr.ApplicationError) {
	if limiter == nil || !limiter.dirty() || ae != nil {
		return
	}

	limiterInstance := limiter.(Instance) // Should always be true
	limiterInstance.metadata().dataStore.Update(limiterInstance, nil)

	limiter.clearDirty()
}
