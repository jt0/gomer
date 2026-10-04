package auth

import (
	"github.com/jt0/gomer/gomerr"
)

type Subject interface {
	Principal(principalType PrincipalType) Principal
	Release(errored bool) gomerr.Gomerr
}

type basicSubject struct {
	principals map[PrincipalType]Principal
}

func NewSubject(principals ...Principal) Subject {
	principalMap := make(map[PrincipalType]Principal, len(principals))

	for _, principal := range principals {
		principalMap[principal.Type()] = principal
	}

	return &basicSubject{principalMap}
}

func (b *basicSubject) Principal(principalType PrincipalType) Principal {
	return b.principals[principalType]
}

func (b *basicSubject) Release(errored bool) gomerr.Gomerr {
	var eb gomerr.ErrorBatch
	for _, principal := range b.principals {
		eb.Capture(principal.Release(errored))
	}
	return eb.GomerrOrNil()
}

type PrincipalType string

const (
	Account PrincipalType = "Account"
	Role    PrincipalType = "Role"
	User    PrincipalType = "User"
	Group   PrincipalType = "Group"
	Request PrincipalType = "Request"
)

type Principal interface {
	Id() string
	Type() PrincipalType
	Release(errored bool) gomerr.Gomerr
}
