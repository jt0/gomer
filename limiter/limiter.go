package limiter

import (
	"github.com/jt0/gomer/gomerr"
)

type Limiter interface {
	CheckThenIncrementByOne(limitName string) *gomerr.ApplicationError
	DecrementByOne(limitName string) *gomerr.ApplicationError
	CheckThenIncrementBy(limitName string, incrementAmount int) *gomerr.ApplicationError
	DecrementBy(limitName string, decrementAmount int) *gomerr.ApplicationError
	UpdateLimit(limitName string, newLimit int) *gomerr.ApplicationError
	UpdateCount(limitName string, newCount int) *gomerr.ApplicationError
}

type Limit struct {
	Name string
	Count int
	Limit int
}

type Limits map[string]*Limit

func New(provided []Limit) (Limiter, *gomerr.ApplicationError) {
	l := Limits(make(map[string]*Limit, len(provided)))

	for _, limit := range provided {
		if _, exists := l[limit.Name]; exists {
			gomerr.InternalServerError("Duplicate Limit found", map[string]interface{}{"Name": limit.Name})
		}

		l[limit.Name] = &limit
	}

	return l, nil
}

func (l Limits) CheckThenIncrementByOne(limitName string) *gomerr.ApplicationError {
	return l.CheckThenIncrementBy(limitName, 1)
}

func (l Limits) CheckThenIncrementBy(limitName string, incrementAmount int) *gomerr.ApplicationError {
	lim, ae := l.getLimit(limitName)
	if ae != nil {
		return ae
	}

	if lim.Count + incrementAmount > lim.Limit {
		return gomerr.LimitExceeded("Limit exceeded", map[string]interface{}{"Name": limitName, "Limit": lim.Limit})
	}

	lim.Count += incrementAmount

	return nil
}

func (l Limits) DecrementByOne(limitName string) *gomerr.ApplicationError {
	return l.DecrementBy(limitName, 1)
}

func (l Limits) DecrementBy(limitName string, decrementAmount int) *gomerr.ApplicationError {
	lim, ae := l.getLimit(limitName)
	if ae != nil {
		return ae
	}

	lim.Count -= decrementAmount

	return nil
}

func (l Limits) UpdateLimit(limitName string, newLimit int) *gomerr.ApplicationError {
	lim, ae := l.getLimit(limitName)
	if ae != nil {
		return ae
	}

	if newLimit < 0 {
		return gomerr.InternalServerError("Cannot set limit below zero", map[string]interface{}{"Name": limitName, "NewLimit": newLimit})
	}

	if newLimit < lim.Count {
		return gomerr.InternalServerError("Cannot set limit below current count", map[string]interface{}{"Name": limitName, "NewLimit": newLimit, "Count": lim.Count})
	}

	lim.Limit = newLimit

	return nil
}

func (l Limits) UpdateCount(limitName string, newCount int) *gomerr.ApplicationError {
	lim, ae := l.getLimit(limitName)
	if ae != nil {
		return ae
	}

	if newCount < 0 {
		return gomerr.InternalServerError("Cannot set count below zero", map[string]interface{}{"Name": limitName, "NewCount": newCount})
	}

	if newCount > lim.Limit {
		return gomerr.InternalServerError("Cannot set count above current limit", map[string]interface{}{"Name": limitName, "Limit": lim.Limit, "NewCount": newCount})
	}

	lim.Limit = newCount

	return nil
}

func (l Limits) getLimit(limitName string) (*Limit, *gomerr.ApplicationError) {
	lim, exists := l[limitName]
	if !exists {
		return nil, gomerr.InternalServerError("No such limit: " + limitName)
	}

	return lim, nil
}
