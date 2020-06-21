package resource

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/jt0/gomer/constraint"
	"github.com/jt0/gomer/gomerr"
	"github.com/jt0/gomer/util"
)

type Limited interface {
	Limiter() (Limiter, gomerr.Gomerr)
	DefaultLimit() util.Amount
	LimitAmount() util.Amount
}

type Limiter interface {
	util.Dirtyable
	Current(limited Limited) util.Amount
	SetCurrent(limited Limited, current util.Amount)
	Override(limited Limited) util.Amount
	SetOverride(limited Limited, override util.Amount)
	Maximum(limited Limited) util.Amount
}

type limitAction func(Limiter, Limited) gomerr.Gomerr

func checkAndIncrement(limiter Limiter, limited Limited) gomerr.Gomerr {
	current := limiter.Current(limited)
	maximum := limiter.Maximum(limited)
	newAmount := current.Increment(limited.LimitAmount())

	if newAmount.Equals(current) {
		return nil
	}

	if newAmount.Exceeds(maximum) {
		return gomerr.LimitExceeded(util.UnqualifiedTypeName(reflect.TypeOf(limited)), maximum, current, newAmount)
	}

	limiter.SetCurrent(limited, newAmount)

	return nil
}

func decrement(limiter Limiter, limited Limited) gomerr.Gomerr {
	current := limiter.Current(limited)
	newAmount := current.Decrement(limited.LimitAmount())

	if newAmount.Equals(current) {
		return nil
	}

	// This could go below zero, though there may be valid application cases to support this. For now, no extra checks to verify.
	limiter.SetCurrent(limited, newAmount)

	return nil
}

func limit(limitAction limitAction, i Instance) (Limiter, gomerr.Gomerr) {
	limited, ok := i.(Limited)
	if !ok {
		return nil, nil
	}

	limiter, ge := limited.Limiter()
	if ge != nil {
		return nil, ge.AddCulprit(gomerr.Configuration)
	}

	limiterInstance, ok := limiter.(Instance)
	if !ok {
		return nil, gomerr.BadValue("limiter", limiter, constraint.TypeOf(limiterInstance)).AddNotes("limiter does not implement Instance").AddCulprit(gomerr.Configuration)
	}

	// If the metadata isn't set, then this is a new object and needs to be loaded
	var loaded bool
	if limiterInstance.metadata() == nil {
		resourceType := strings.ToLower(util.UnqualifiedTypeName(reflect.TypeOf(limiter)))
		metadata, ok := resourceMetadata[resourceType]
		if !ok {
			return nil, gomerr.NotFound("resource type", resourceType).AddCulprit(gomerr.Configuration)
		}

		limiterInstance.setMetadata(metadata)
		limiterInstance.setSubject(i.Subject())
		limiterInstance.OnSubject()

		// TODO: cache in case needed by more than one resource...
		if ge := limiterInstance.metadata().dataStore.Read(limiterInstance); ge != nil {
			return nil, ge
		}

		loaded = true
	}

	if ge := limitAction(limiter, limited); ge != nil {
		return nil, ge
	}

	// If we didn't load the limiterInstance, we'll let other code handle the save
	if !loaded {
		limiter = nil
	}

	return limiter, nil
}

func saveLimiter(limiter Limiter, ge gomerr.Gomerr) {
	// TODO: need an optimistic lock mechanism to avoid overwriting
	if limiter == nil || !limiter.IsDirty() || ge != nil {
		return
	}

	limiterInstance := limiter.(Instance) // Should always be true
	ge = limiterInstance.metadata().dataStore.Update(limiterInstance, nil)
	if ge != nil {
		// TODO: use provided logger
		fmt.Println("Failed to save limiter (type: %s, id: %s). Error: %s"+limiterInstance.PersistableTypeName(), limiterInstance.Id(), ge)
	}
}
