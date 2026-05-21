package gomerr

// notAnError is a sentinel Gomerr that represents the absence of a real error.
// It satisfies the Gomerr interface but is fully inert — all methods are no-ops.
type notAnError struct{}

// NotAnError is a sentinel value that looks like a Gomerr but represents "no error."
// Use errors.Is(err, NotAnError) to detect it explicitly.
var NotAnError Gomerr = &notAnError{}

func (n *notAnError) Error() string                        { return "not an error" }
func (n *notAnError) String() string                       { return "not an error" }
func (n *notAnError) Unwrap() error                        { return nil }
func (n *notAnError) Is(err error) bool                    { return err == NotAnError }
func (n *notAnError) Wrap(error) Gomerr                    { return n }
func (n *notAnError) AddAttribute(string, any) Gomerr      { return n }
func (n *notAnError) ReplaceAttribute(string, any) Gomerr  { return n }
func (n *notAnError) DeleteAttribute(string) Gomerr        { return n }
func (n *notAnError) AddAttributes(...any) Gomerr          { return n }
func (n *notAnError) WithAttributes(map[string]any) Gomerr { return n }
func (n *notAnError) Attribute(string) any                 { return nil }
func (n *notAnError) AttributeLookup(string) (any, bool)   { return nil, false }
func (n *notAnError) Attributes() map[string]any           { return nil }
func (n *notAnError) ToMap() map[string]any                { return nil }
func (n *notAnError) isFromBuildFunc() bool                { return true }
