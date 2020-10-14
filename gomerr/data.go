package gomerr

type DataError struct {
	Gomerr
	Operation string
	Data      interface{} `gomerr:"include_type"`
}

func Data(operation string, data interface{}) *DataError {
	return Build(new(DataError), operation, data).(*DataError)
}
