package resource

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/jt0/gomer/gomerr"
	"github.com/jt0/gomer/limit"
	"github.com/jt0/gomer/util"
)

type limitAction func(limit.Limiter, limit.Limited) gomerr.Gomerr

func checkAndIncrement(limiter limit.Limiter, limited limit.Limited) gomerr.Gomerr {
	current := limiter.Current(limited)
	maximum := limiter.Maximum(limited)
	newAmount := current.Increment(limited.LimitAmount())

	if newAmount.Equals(current) {
		return nil
	}

	if newAmount.Exceeds(maximum) {
		return limit.Exceeded(limiter, limited, maximum, current, newAmount)
	}

	limiter.SetCurrent(limited, newAmount)

	return nil
}

func decrement(limiter limit.Limiter, limited limit.Limited) gomerr.Gomerr {
	current := limiter.Current(limited)
	newAmount := current.Decrement(limited.LimitAmount())

	if newAmount.Equals(current) {
		return nil
	}

	// This could go below zero, though there may be valid application cases to support this. For now, no extra checks to verify.
	limiter.SetCurrent(limited, newAmount)

	return nil
}

func applyLimitAction(limitAction limitAction, i Instance) (limit.Limiter, gomerr.Gomerr) {
	limited, ok := i.(limit.Limited)
	if !ok {
		return nil, nil
	}

	limiter, ge := limited.Limiter()
	if ge != nil {
		return nil, gomerr.Configuration(i.metadata().instanceName + " did not provide a Limiter for itself.").Wrap(ge)
	}

	limiterInstance, ok := limiter.(Instance)
	if !ok {
		return nil, gomerr.Configuration("Limiter from " + i.metadata().instanceName + " does not implement resource.Instance")
	}

	// If the metadata isn't set, then this is a new object and needs to be loaded
	var loaded bool
	if limiterInstance.metadata() == nil {
		resourceType := util.UnqualifiedTypeName(reflect.TypeOf(limiter)) // PersistableTypeName() not yet available.
		metadata, ok := lowerCaseResourceTypeToMetadata[strings.ToLower(resourceType)]
		if !ok {
			return nil, gomerr.Configuration("Unregistered resource type.").Wrap(unknownResourceType(resourceType))
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

	// If we didn't load the updatable, we'll let other code handle the save
	if !loaded {
		limiter = nil
	}

	return limiter, nil
}

func saveLimiterIfDirty(limiter limit.Limiter) {
	// TODO: need an optimistic lock mechanism to avoid overwriting
	if limiter == nil || !limiter.IsDirty() {
		return
	}

	limiterInstance := limiter.(Instance) // Should always be true
	ge := limiterInstance.metadata().dataStore.Update(limiterInstance, nil)
	if ge != nil {
		// TODO: use provided logger
		fmt.Printf("Failed to save limiter (type: %s, id: %s). Error:\n%s\n", limiterInstance.metadata().instanceName, limiterInstance.Id(), ge)
		return
	}

	limiter.ClearDirty()
}
