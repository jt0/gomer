package http

import (
	"net/http"
	"strconv"

	"github.com/jt0/gomer/log"
)

type Options struct {
	Port int16
}

// Serve starts the HTTP server
func Serve(handler http.Handler, optFns ...func(*Options)) {
	o := &Options{
		Port: 8080,
	}

	for _, optFn := range optFns {
		optFn(o)
	}

	addr := "127.0.0.1:" + strconv.Itoa(int(o.Port))
	log.Logger().Info("serving", "addr", addr)
	err := http.ListenAndServe(addr, handler)
	log.Logger().Info("server shutdown", "err", err)
}

func noOptFn(*Options) {}

func Port(p string) func(*Options) {
	i, err := strconv.ParseInt(p, 10, 16)
	if err != nil {
		if p != "" {
			log.Logger().Error("invalid port, ignoring", "provided", p)
		}
		return noOptFn
	}
	return func(o *Options) {
		o.Port = int16(i)
	}
}
