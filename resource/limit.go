package resource

import (
	"context"

	"github.com/jt0/gomer/gomerr"
	"github.com/jt0/gomer/limit"
	"github.com/jt0/gomer/log"
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

func applyLimitAction[I Instance[I]](_ context.Context, limitAction limitAction, i I) (limit.Limiter, gomerr.Gomerr) {
	limited, ok := any(i).(limit.Limited)
	if !ok {
		return nil, nil
	}

	limiter, ge := limited.Limiter()
	if ge != nil {
		return nil, gomerr.Configuration(i.TypeName() + " did not provide a Limiter for itself.").Wrap(ge)
	} else if limiter == nil {
		return nil, nil
	}

	return limiter, limitAction(limiter, limited)
}

func saveLimiterIfDirty(ctx context.Context, limiter limit.Limiter) {
	// TODO: need an optimistic lock mechanism to avoid overwriting
	if limiter == nil || !limiter.IsDirty() {
		return
	}

	li := limiter.(AnyInstance) // Should always be true
	//_, ge := b.DoAction(ctx, ReadAction[I]())

	ge := li.RegisteredType().Store().Update(ctx, li, li)

	if ge != nil {
		log.Logger().Error("failed to save limiter", "type", li.TypeName(), "id", li.Id(), "error", ge)
		return
	}

	limiter.ClearDirty()
}
