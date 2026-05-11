package bind

import (
	"github.com/jt0/gomer/gomerr"
	"github.com/jt0/gomer/structs"
)

// Binder holds a configured pair of in/out tools for binding data to and from structs.
// Create one via NewBinder and pass it explicitly to the components that need it.
type Binder struct {
	InTool  *structs.Tool
	OutTool *structs.Tool
}

// NewBinder creates a Binder with the provided configuration options.
//
//	binder := bind.NewBinder(bind.CamelCaseFields, bind.OmitEmpty)
func NewBinder(options ...func(*Configuration)) *Binder {
	config := NewConfiguration(options...)
	return &Binder{
		InTool:  NewInTool(config, structs.StructTagDirectiveProvider{"in"}),
		OutTool: NewOutTool(config, structs.StructTagDirectiveProvider{"out"}),
	}
}

// In binds data into v using this Binder's InTool.
func (b *Binder) In(data map[string]any, v any, optional ...structs.ToolContext) gomerr.Gomerr {
	return structs.ApplyTools(v, structs.EnsureContext(optional...).With(InKey, data), b.InTool)
}

// Out binds v's fields into a map using this Binder's OutTool.
func (b *Binder) Out(v any, optional ...structs.ToolContext) (map[string]any, gomerr.Gomerr) {
	tc := structs.EnsureContext(optional...).With(OutKey, make(map[string]any))
	if ge := structs.ApplyTools(v, tc, b.OutTool); ge != nil {
		return nil, ge
	}
	return tc.Get(OutKey).(map[string]any), nil
}

const (
	InKey  = "$_gomer_bind_in"
	OutKey = "$_gomer_bind_out"

	skipField    = "-"
	includeField = "+"

	omitEmpty    = "omitempty"
	includeEmpty = "includeempty"
)

type casingFn func(s string) string

var (
	PascalCaseFn casingFn = func(fieldName string) string { return fieldName } // Exported fields are already PascalCase
	CamelCaseFn  casingFn = func(fieldName string) string {
		// NB: only supports field names with an initial ASCII character
		firstChar := fieldName[0]
		if 'A' <= firstChar && firstChar <= 'Z' {
			firstChar += 'a' - 'A'
		}
		return string(firstChar) + fieldName[1:]
	}
	// Feature:p2 support case insensitive. Additionally consider support matching output case to input
)

type Configuration struct {
	// Empty field value default
	emptyValue string

	// Specifies the casing used for inbound and outbound data. Simplifies the naming configuration if the only
	// difference between a data attribute and struct field name is the casing.
	// TODO:p3 I can imagine a situation where the casing might vary depending on where it's coming from or going to,
	//  so perhaps should be map[string]FieldCaseType where the key = scope...
	toCase *casingFn

	extension ExtensionProvider

	// TODO:p2 RawBytesBindingDirective string
}

func (bc *Configuration) withOptions(options ...func(*Configuration)) Configuration {
	for _, option := range options {
		option(bc)
	}
	return *bc
}

func NewConfiguration(options ...func(*Configuration)) Configuration {
	bc := &Configuration{
		emptyValue: omitEmpty,
		toCase:     &CamelCaseFn,
		// strictMode: false,  // true if should fail on extra input values, false otherwise
	}

	return bc.withOptions(options...)
}

func CopyConfigurationWithOptions(b Configuration, options ...func(*Configuration)) Configuration {
	return (&b).withOptions(options...)
}

func OmitEmpty(c *Configuration) {
	c.emptyValue = omitEmpty
}

func IncludeEmpty(c *Configuration) {
	c.emptyValue = includeEmpty
}

func PascalCaseFields(c *Configuration) {
	c.toCase = &PascalCaseFn
}

func CamelCaseFields(c *Configuration) {
	c.toCase = &CamelCaseFn
}

type ExtensionProvider interface {
	structs.ApplierProvider
	Type() string
}

func ExtendsWith(extension ExtensionProvider) func(*Configuration) {
	return func(c *Configuration) {
		if c.extension != nil {
			// panic may be too severe, but we do want people to be aware that the value may be overwritten
			panic("Configuration already has an extension configured. Consider chaining if more than one is needed.")
		}
		c.extension = extension
	}
}
