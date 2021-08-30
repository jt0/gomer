package limit

import (
	"reflect"
	"strings"
)

type TrackingLimiter struct {
	Currents  map[string]amount
	Overrides map[string]amount
	dirty     bool
}

type amount int64

func (l *TrackingLimiter) Current(limited Limited) Amount {
	if l.Currents == nil {
		return limited.LimitAmount().Zero()
	}

	current, ok := l.Currents[unqualifiedTypeName(reflect.TypeOf(limited))]
	if !ok {
		return limited.LimitAmount().Zero()
	}

	return limited.LimitAmount().convert(current)
}

func (l *TrackingLimiter) SetCurrent(limited Limited, current Amount) {
	if l.Currents == nil {
		l.Currents = make(map[string]amount)
	}
	l.Currents[unqualifiedTypeName(reflect.TypeOf(limited))] = current.amount()

	l.dirty = true
}

func (l *TrackingLimiter) Override(limited Limited) Amount {
	if l.Overrides == nil {
		return nil
	}

	override, ok := l.Overrides[unqualifiedTypeName(reflect.TypeOf(limited))]
	if !ok {
		return nil
	}

	return limited.LimitAmount().convert(override)
}

func (l *TrackingLimiter) SetOverride(limited Limited, override Amount) {
	if override.Exceeds(limited.DefaultLimit()) {
		if l.Overrides == nil {
			l.Overrides = make(map[string]amount)
		}
		l.Overrides[unqualifiedTypeName(reflect.TypeOf(limited))] = override.amount()
	} else {
		delete(l.Overrides, unqualifiedTypeName(reflect.TypeOf(limited)))
	}

	l.dirty = true
}

func (l *TrackingLimiter) Maximum(limited Limited) Amount {
	override := l.Override(limited)
	defaultLimit := limited.DefaultLimit()

	if override != nil && override.Exceeds(defaultLimit) {
		return override
	} else {
		return defaultLimit
	}
}

func (l *TrackingLimiter) IsDirty() bool {
	return l.dirty
}

func (l *TrackingLimiter) ClearDirty() {
	l.dirty = false
}

func unqualifiedTypeName(t reflect.Type) string {
	s := t.String()
	return s[strings.Index(s, ".")+1:]
}
