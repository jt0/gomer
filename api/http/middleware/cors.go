package middleware

import (
	"maps"
	"net/http"
	"strings"
)

type CorsOption func(*corsConfig)

func AllowAllOrigins() CorsOption {
	return func(config *corsConfig) {
		if len(config.origins) != 0 {
			panic("conflicting origin definitions: cannot allow all when specific origins have been configured")
		}
		config.allowAllOrigins = true
	}
}

func Origins(origins ...string) CorsOption {
	return func(config *corsConfig) {
		if len(origins) == 0 {
			return
		} else if origins[0] == "*" {
			AllowAllOrigins()
		} else if config.allowAllOrigins {
			panic("conflicting origin definitions: cannot set origins if allow all has been configured")
		}
		for _, origin := range origins {
			if origin == "*" {
				panic("invalid origin: cannot mix '*' with other origins")
			}
			config.origins[origin] = true
		}
	}
}

func DoNotExposeHeaders(headers ...string) CorsOption {
	return func(config *corsConfig) {
		for _, header := range headers {
			config.excludedHeaders[strings.ToLower(header)] = true
		}
	}
}

// excludedHeaders is initialized with the CORS safe-listed response headers
var excludedHeaders = map[string]bool{
	"cache-control":                    true,
	"content-language":                 true,
	"content-length":                   true,
	"content-type":                     true,
	"expires":                          true,
	"last-modified":                    true,
	"pragma":                           true,
	"access-control-allow-origin":      true,
	"access-control-allow-credentials": true,
	"access-control-expose-headers":    true,
}

// Cors provides middleware to apply CORS headers to outbound responses, including
// both success and error cases.
func Cors(opts ...CorsOption) func(next http.Handler) http.Handler {
	cc := &corsConfig{excludedHeaders: maps.Clone(excludedHeaders)}
	for _, opt := range opts {
		opt(cc)
	}

	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if requestMethod := r.Header.Get("Access-Control-Request-Method"); r.Method == http.MethodOptions && requestMethod != "" {
				header := w.Header()
				header.Set("Access-Control-Allow-Origin", cc.allowOrigin(r))
				header.Set("Access-Control-Allow-Methods", requestMethod)
				if requestHeaders := r.Header.Get("Access-Control-Request-Headers"); requestHeaders != "" {
					header.Set("Access-Control-Allow-Headers", requestHeaders)
				}
				w.WriteHeader(http.StatusNoContent)
				return
			}

			next.ServeHTTP(w, r)

			header := w.Header()
			header.Set("Access-Control-Allow-Origin", cc.allowOrigin(r))

			var exposeHeaders []string
			for headerName := range w.Header() {
				if cc.excludedHeaders[strings.ToLower(headerName)] {
					continue
				}
				exposeHeaders = append(exposeHeaders, headerName)
			}
			header["Access-Control-Expose-Headers"] = []string{strings.Join(exposeHeaders, ",")}
		})
	}
}

type corsConfig struct {
	origins         map[string]bool
	allowAllOrigins bool
	excludedHeaders map[string]bool
}

func (cc corsConfig) allowOrigin(r *http.Request) string {
	origin := r.Header.Get("Origin")
	switch {
	case origin == "":
		return "" // Not a browser request; no header needed.
	case cc.allowAllOrigins:
		return "*"
	case cc.origins[origin]:
		return origin
	}
	return ""
}
