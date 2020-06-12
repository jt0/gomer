package data

import (
	"github.com/jt0/gomer/gomerr"
)

type CreateError struct {
	gomerr.Gomerr
	Persistable Persistable
}

type ReadError struct {
	gomerr.Gomerr
	Persistable Persistable
}

type UpdateError struct {
	gomerr.Gomerr
	Persistable Persistable
	Update      interface{}
}

type DeleteError struct {
	gomerr.Gomerr
	Persistable Persistable
}

type QueryError struct {
	gomerr.Gomerr
	Queryable Queryable
}

type IndexNotFoundError struct {
	gomerr.Gomerr
	TableName string
	Queryable Queryable
}

type ConstraintViolationError struct {
	gomerr.Gomerr
	ConstraintType ConstraintType
	On             string
	By             interface{}
}

type ConstraintType string

const (
	Unique ConstraintType = "Unique"
	Size                  = "Size"
)

func CreateFailed(cause error, persistable Persistable) gomerr.Gomerr {
	return gomerr.BuildWithCause(cause, new(CreateError), persistable)
}

func ReadFailed(cause error, persistable Persistable) gomerr.Gomerr {
	return gomerr.BuildWithCause(cause, new(ReadError), persistable)
}

func UpdateFailed(cause error, persistable Persistable, update interface{}) gomerr.Gomerr {
	return gomerr.BuildWithCause(cause, new(UpdateError), persistable, update)
}

func DeleteFailed(cause error, persistable Persistable) gomerr.Gomerr {
	return gomerr.BuildWithCause(cause, new(DeleteError), persistable)
}

func QueryFailed(cause error, queryable Queryable) gomerr.Gomerr {
	return gomerr.BuildWithCause(cause, new(QueryError), queryable)
}

func IndexNotFound(tableName string, queryable Queryable) gomerr.Gomerr {
	return gomerr.Build(new(IndexNotFoundError), tableName, queryable)
}

func ConstraintViolation(constraintType ConstraintType, on string, by interface{}) gomerr.Gomerr {
	return gomerr.Build(new(ConstraintViolationError), constraintType, on, by)
}
