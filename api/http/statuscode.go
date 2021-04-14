package http

const StatusLimitExceeded = 402

type StatusCoder interface {
	StatusCode() int
}
