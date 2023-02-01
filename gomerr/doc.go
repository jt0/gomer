// Package gomerr provides an enhanced error framework that captures errors,
// error attributes, stack traces, and more. It includes a "pretty-print"
// mechanism, and can be used by renderers to produce content suitable for
// distribution across process boundaries (i.e. as part of an API error
// response).
//
// The package defines an interface, `Gomerr`, that extends the Error,
// interface, includes the the `Is(err Error)` and `Unwrap()` functions, and
// provides other generally useful functions. The gomerr package also
// includes a base implementation, `*gomerr`, and a builder function that
// can be used to build specific Gomerr implementation types. Let's take
// ConfigurationError as an example:
//
//   type BooBooError struct {
//     Gomerr
//     Ouchie string
//   }
//
//   func BooBoo(ouchie string) *BooBooError {
//     return Build(new(BooBooError), ouchie).(*BooBooError)
//   }
//
//

package gomerr
