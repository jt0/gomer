package resource

import (
	"context"
	"fmt"
	"reflect"

	"github.com/jt0/gomer/gomerr"
	"github.com/jt0/gomer/limit"
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

func applyLimitAction(ctx context.Context, limitAction limitAction, i Resource) (limit.Limiter, gomerr.Gomerr) {
	limited, ok := i.(limit.Limited)
	if !ok {
		return nil, nil
	}

	limiter, ge := limited.Limiter()
	if ge != nil {
		return nil, gomerr.Configuration(i.metadata().instanceName + " did not provide a Limiter for itself.").Wrap(ge)
	}

	li, ok := limiter.(Instance)
	if !ok {
		return nil, gomerr.Configuration("limiter from " + i.metadata().instanceName + " does not implement resource.Instance")
	}

	// If the metadata isn't set, then this is a New object and needs to be loaded
	var loaded bool
	if li.metadata() == nil {
		resourceType := reflect.TypeOf(limiter)
		md, exists := defaultDomain.Load().metadata[resourceType]
		if !exists {
			return nil, gomerr.Unprocessable("unknown Resource type. Was resource.Register() called for it?", resourceType)
		}

		li.setSelf(li)
		li.setMetadata(md)
		li.setSubject(i.Subject(ctx))

		// TODO: cache in case needed by more than one resource...
		if ge = li.metadata().dataStore.Read(ctx, li); ge != nil {
			return nil, ge
		}

		loaded = true
	}

	if ge = limitAction(limiter, limited); ge != nil {
		return nil, ge
	}

	// If we didn't load the updatable, we'll let other code handle the save
	if !loaded {
		limiter = nil
	}

	return limiter, nil
}

func saveLimiterIfDirty(ctx context.Context, limiter limit.Limiter) {
	// TODO: need an optimistic lock mechanism to avoid overwriting
	if limiter == nil || !limiter.IsDirty() {
		return
	}

	li := limiter.(Instance) // Should always be true
	ge := li.metadata().dataStore.Update(ctx, li, nil)
	if ge != nil {
		// TODO: use provided logger
		fmt.Printf("Failed to save limiter (type: %s, id: %s). Error:\n%s\n", li.metadata().instanceName, li.Id(), ge)
		return
	}

	limiter.ClearDirty()
}
