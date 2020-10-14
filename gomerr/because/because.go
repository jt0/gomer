package because

type Because string

const (
	Duplicate  Because = "Duplicate"
	Exists             = "Exists"
	Immutable          = "Immutable"
	InUse              = "InUse"
	Locked             = "Locked"
	Mismatch           = "Mismatch"
	Modified           = "Modified"
	NotUnique          = "NotUnique"
	OutOfDate          = "OutOfDate"
	OutOfRange         = "OutOfRange"
	OutOfSync          = "OutOfSync"
)
