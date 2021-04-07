package constraint

import "time"

var expired = func(toTest interface{}) bool {
	var expiration time.Time
	switch tt := toTest.(type) {
	case time.Time:
		expiration = tt
	case interface{ ExpiresAt() time.Time }:
		expiration = tt.ExpiresAt()
	case interface{ Expiration() time.Time }:
		expiration = tt.Expiration()
	}

	return time.Now().UTC().After(expiration)
}

func Expired(value interface{}) Constraint {
	return &constraint{"Expired", value, expired}
}

func NotExpired(value interface{}) Constraint {
	return &constraint{"NotExpired", value, func(toTest interface{}) bool {
		return !expired(toTest)
	}}
}
