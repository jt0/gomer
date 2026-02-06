package gomerr

type PanicError struct {
	Gomerr
	Recover any
}

func Panic(recover any) *PanicError {
	return Build(new(PanicError), recover).(*PanicError)
}
