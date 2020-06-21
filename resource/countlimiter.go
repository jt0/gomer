package resource

import (
	"reflect"
	"strconv"

	"github.com/jt0/gomer/util"
)

type CountLimiter struct {
	Currents  map[string]Count
	Overrides map[string]Count

	dirty bool
}

func (l *CountLimiter) IsDirty() bool {
	return l.dirty
}

func (l *CountLimiter) ClearDirty() {
	l.dirty = false
}

func (l *CountLimiter) Current(limited Limited) util.Amount {
	if l.Currents == nil {
		return CountZero
	}

	current, ok := l.Currents[util.UnqualifiedTypeName(reflect.TypeOf(limited))]
	if !ok {
		return CountZero
	}

	return current
}

func (l *CountLimiter) SetCurrent(limited Limited, current util.Amount) {
	if l.Currents == nil {
		l.Currents = make(map[string]Count)
	}

	l.Currents[util.UnqualifiedTypeName(reflect.TypeOf(limited))] = current.(Count)

	l.dirty = true
}

func (l *CountLimiter) Override(limited Limited) util.Amount {
	if l.Overrides == nil {
		return CountZero
	}

	override, ok := l.Overrides[util.UnqualifiedTypeName(reflect.TypeOf(limited))]
	if !ok {
		return CountZero
	}

	return override
}

func (l *CountLimiter) SetOverride(limited Limited, override util.Amount) {
	if l.Overrides == nil {
		l.Overrides = make(map[string]Count)
	}

	if override.Exceeds(limited.DefaultLimit()) {
		l.Overrides[util.UnqualifiedTypeName(reflect.TypeOf(limited))] = override.(Count)
	} else {
		delete(l.Overrides, util.UnqualifiedTypeName(reflect.TypeOf(limited)))
	}

	l.dirty = true
}

func (l *CountLimiter) Maximum(limited Limited) util.Amount {
	override := l.Override(limited)
	defaultLimit := limited.DefaultLimit()

	if override.Exceeds(defaultLimit) {
		return override
	} else {
		return defaultLimit
	}
}

type Count int

const (
	CountZero Count = iota
	CountOne

	CountMeasure util.Measure = "Count"
)

func (c Count) Increment(amount util.Amount) util.Amount {
	return Count(int(c) + int(amount.(Count)))
}

func (c Count) Decrement(amount util.Amount) util.Amount {
	return Count(int(c) - int(amount.(Count)))
}

func (c Count) Equals(amount util.Amount) bool {
	return int(c) == int(amount.(Count))
}

func (c Count) Exceeds(amount util.Amount) bool {
	return int(c) > int(amount.(Count))
}

func (c Count) Measure() util.Measure {
	return CountMeasure
}

func (c Count) String() string {
	return strconv.Itoa(int(c))
}
