package bind

import (
	"github.com/jt0/gomer/structs"
)

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
	PascalCase casingFn = func(fieldName string) string { return fieldName } // Exported fields are already PascalCase
	CamelCase  casingFn = func(fieldName string) string {
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
	// Empty directive default
	emptyDirective string

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
		emptyDirective: skipField,
		emptyValue:     omitEmpty,
		toCase:         &PascalCase,
		// strictMode: false,  // true if should fail on extra input values, false otherwise
	}

	return bc.withOptions(options...)
}

func CopyConfigurationWithOptions(b Configuration, options ...func(*Configuration)) Configuration {
	return (&b).withOptions(options...)
}

func EmptyDirectiveSkipsField(c *Configuration) {
	c.emptyDirective = skipField
}

func EmptyDirectiveIncludesField(c *Configuration) {
	c.emptyDirective = includeField
}

func OmitEmpty(c *Configuration) {
	c.emptyValue = omitEmpty
}

func IncludeEmpty(c *Configuration) {
	c.emptyValue = includeEmpty
}

func PascalCaseData(c *Configuration) {
	c.toCase = &PascalCase
}

func CamelCaseData(c *Configuration) {
	c.toCase = &CamelCase
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
