package limit

import (
	"github.com/jt0/gomer/gomerr"
)

type ExceededError struct {
	gomerr.Gomerr
	Limiter   interface{}
	Limited   interface{}
	Limit     Amount
	Current   Amount
	Attempted Amount
}

func Exceeded(limiter, limited interface{}, limit, current, attempted Amount) *ExceededError {
	return gomerr.Build(new(ExceededError), limiter, limited, limit, current, attempted).(*ExceededError)
}

func UnquantifiedExcess(limiter, limited interface{}) *ExceededError {
	return gomerr.Build(new(ExceededError), limiter, limited, Unknown, Unknown, Unknown).(*ExceededError)
}
