package gomerr

import (
	"time"
)

type ExpiredError struct {
	Gomerr
	What       string
	ExpiredAt  time.Time
	ServerTime time.Time
}

func Expired(what string, expiredAt time.Time) *ExpiredError {
	return Build(new(ExpiredError), what, expiredAt.UTC(), time.Now().UTC()).(*ExpiredError)
}
