package middleware

import (
	"net"
	"net/http"
	"strings"
)

func ValidateHost(addr string) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		if addr == "" {
			return next
		}

		valid := validHosts(addr)

		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if !valid[hostFrom(r.Host)] {
				http.Error(w, "unrecognized host", http.StatusBadRequest)
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}

func hostFrom(hostPort string) string {
	host, _, err := net.SplitHostPort(hostPort)
	if err != nil {
		// SplitHostPort returns an error if there is no port. Fall back to the raw
		// host string (trimmed of whitespace).
		host = strings.TrimSpace(hostPort)
	}
	return strings.ToLower(host) // normalize
}

func validHosts(addr string) map[string]bool {
	if lenAddr := len(addr); addr[0] == '*' || (lenAddr >= len("localhost") && addr[0:lenAddr] == "localhost") {
		return map[string]bool{
			"localhost": true,
			"127.0.0.1": true,
			"[::1]":     true, // IPv6 loopback
			"::1":       true, // net.SplitHostPort may strip brackets depending on input
		}
	}
	return map[string]bool{
		hostFrom(addr): true,
	}
}
