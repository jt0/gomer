package gomerr

type PanicError struct {
	Gomerr
	Recover interface{}
}

func Panic(recover interface{}) *PanicError {
	return Build(new(PanicError), recover).(*PanicError)
}
