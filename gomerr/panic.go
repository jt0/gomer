package gomerr

type PanicError struct {
	Gomerr
	Recover interface{}
}

func Panic(recover interface{}) *PanicError {
	return Build(new(PanicError), recover).(*PanicError)
}

//func (pe *PanicError) fillStack(stackSkip int) []string {
//	stack := debug.Stack()
//}
