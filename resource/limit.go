package resource

import (
	"fmt"
	"reflect"

	"github.com/jt0/gomer/auth"
	"github.com/jt0/gomer/fields"
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

func applyLimitAction(limitAction limitAction, i Resource) (limit.Limiter, gomerr.Gomerr) {
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
		return nil, gomerr.Configuration("Limiter from " + i.metadata().instanceName + " does not implement resource.Instance")
	}

	// If the metadata isn't set, then this is a new object and needs to be loaded
	var loaded bool
	if li.metadata() == nil {
		resourceType := reflect.TypeOf(limiter)
		metadata, ok := resourceTypeToMetadata[resourceType]
		if !ok {
			return nil, gomerr.Unprocessable("Unknown Resource type. Was resource.Register() called for it?", resourceType)
		}

		li.setMetadata(metadata)
		li.setSubject(i.Subject())

		// Only works if the limiter has provided attributes that overlap with what the limiter needs. If any are
		// missing, it will need to be populated by the Limited
		tool := fields.ToolWithContext{auth.FieldAccessTool, auth.AddCopyProvidedAction(reflect.ValueOf(li).Elem())}
		if ge := li.metadata().fields.ApplyTools(reflect.ValueOf(i).Elem(), tool); ge != nil {
			return nil, ge
		}

		// TODO: cache in case needed by more than one resource...
		if ge := li.metadata().dataStore.Read(li); ge != nil {
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
		id, _ := Id(limiterInstance)
		fmt.Printf("Failed to save limiter (type: %s, id: %s). Error:\n%s\n", limiterInstance.metadata().instanceName, id, ge)
		return
	}

	limiter.ClearDirty()
}
