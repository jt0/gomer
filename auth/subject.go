package auth

type Subject interface {
	Principal(principalType PrincipalType) Principal
	Release()
}

type basicSubject struct {
	principals map[PrincipalType]Principal
}

func NewSubject(principals ...Principal) *basicSubject {
	principalMap := make(map[PrincipalType]Principal, len(principals))

	for _, principal := range principals {
		principalMap[principal.Type()] = principal
	}

	return &basicSubject{principalMap}
}

func (b *basicSubject) Principal(principalType PrincipalType) Principal {
	return b.principals[principalType]
}

func (b *basicSubject) Release() {
	for _, principal := range b.principals {
		principal.Release()
	}
}

type PrincipalType string
const (
	Account PrincipalType = "Account"
	Role PrincipalType = "Role"
	User PrincipalType = "User"
	Group PrincipalType = "Group"
	Request PrincipalType = "Request"
)

type Principal interface {
	Id() string
	Type() PrincipalType
	Release()
}