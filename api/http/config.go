package http

import (
	"encoding/json"

	"github.com/jt0/gomer/bind"
)

// BindingOption configures HTTP binding for both request and response.
type BindingOption func(*BindingConfiguration)

// BindingConfiguration holds unified configuration for request/response binding.
type BindingConfiguration struct {
	bindConfig      bind.Configuration
	directiveConfig BindDirectiveConfiguration

	// Request-specific overrides (applied after common options)
	requestOverrides []BindingOption
	// Response-specific overrides (applied after common options)
	responseOverrides []BindingOption
}

// NewBindingConfiguration creates a new BindingConfiguration with the provided options.
func NewBindingConfiguration(options ...BindingOption) *BindingConfiguration {
	bc := &BindingConfiguration{
		bindConfig:      bind.NewConfiguration(),
		directiveConfig: NewBindDirectiveConfiguration(),
	}

	for _, opt := range options {
		opt(bc)
	}

	return bc
}

// SetAsDefault sets this configuration as the global default for both request and response binding.
// Returns the configuration for method chaining.
func (bc *BindingConfiguration) SetAsDefault() *BindingConfiguration {
	// Build request configuration
	reqConfig := BindFromRequestConfiguration{
		BindConfiguration:                bc.buildBindConfig(bc.requestOverrides),
		BindDirectiveConfiguration:       bc.directiveConfig,
		defaultContentType:               DefaultContentType,
		perContentTypeUnmarshalFunctions: make(map[string]Unmarshal),
		defaultUnmarshalFunction:         defaultUnmarshal,
	}
	SetBindFromRequestConfiguration(reqConfig)

	// Build response configuration
	respConfig := BindToResponseConfiguration{
		BindConfiguration:              bc.buildBindConfig(bc.responseOverrides),
		BindDirectiveConfiguration:     bc.directiveConfig,
		defaultContentType:             DefaultContentType,
		perContentTypeMarshalFunctions: make(map[string]Marshal),
		defaultMarshalFunction:         defaultMarshal,
	}
	SetBindToResponseConfiguration(respConfig)

	return bc
}

// buildBindConfig creates a bind.Configuration with optional overrides applied.
func (bc *BindingConfiguration) buildBindConfig(overrides []BindingOption) bind.Configuration {
	if len(overrides) == 0 {
		return bc.bindConfig
	}

	// Create a temporary BindingConfiguration to collect override options
	temp := &BindingConfiguration{
		bindConfig:      bc.bindConfig,
		directiveConfig: bc.directiveConfig,
	}
	for _, opt := range overrides {
		opt(temp)
	}
	return temp.bindConfig
}

// Configure applies options to the global default binding configuration for both request and response.
// This is the simplest way to configure HTTP binding.
//
// Example:
//
//	http.Configure(http.CamelCaseFields)
//	http.Configure(http.CamelCaseFields, http.OmitEmptyValues)
func Configure(options ...BindingOption) {
	NewBindingConfiguration(options...).SetAsDefault()
}

// Re-exported options from bind package for convenient access.
var (
	// CamelCaseFields configures field names to use camelCase in JSON (e.g., "firstName").
	CamelCaseFields BindingOption = wrapBindOption(bind.CamelCaseFields)

	// PascalCaseFields configures field names to use PascalCase in JSON (e.g., "FirstName").
	// This is the default behavior.
	PascalCaseFields BindingOption = wrapBindOption(bind.PascalCaseFields)

	// OmitEmptyValues configures binding to omit empty/zero values from output.
	// This is the default behavior.
	OmitEmptyValues BindingOption = wrapBindOption(bind.OmitEmpty)

	// IncludeEmptyValues configures binding to include empty/zero values in output.
	IncludeEmptyValues BindingOption = wrapBindOption(bind.IncludeEmpty)

	// SkipEmptyDirective configures fields without binding directives to be skipped.
	// This is the default behavior.
	SkipEmptyDirective BindingOption = wrapBindOption(bind.EmptyDirectiveSkipsField)

	// IncludeEmptyDirective configures fields without binding directives to be included
	// using the field name.
	IncludeEmptyDirective BindingOption = wrapBindOption(bind.EmptyDirectiveIncludesField)
)

// wrapBindOption converts a bind.Configuration option to a BindingOption.
func wrapBindOption(bindOpt func(*bind.Configuration)) BindingOption {
	return func(bc *BindingConfiguration) {
		bindOpt(&bc.bindConfig)
	}
}

// RequestOption returns a BindingOption that applies the given options only to request binding.
// Use this when you need different configuration for requests vs responses.
//
// Example:
//
//	http.Configure(
//	    http.CamelCaseFields,                        // applies to both
//	    http.RequestOption(http.IncludeEmptyValues), // override for request only
//	)
func RequestOption(opts ...BindingOption) BindingOption {
	return func(bc *BindingConfiguration) {
		bc.requestOverrides = append(bc.requestOverrides, opts...)
	}
}

// ResponseOption returns a BindingOption that applies the given options only to response binding.
// Use this when you need different configuration for requests vs responses.
//
// Example:
//
//	http.Configure(
//	    http.CamelCaseFields,                       // applies to both
//	    http.ResponseOption(http.OmitEmptyValues),  // override for response only
//	)
func ResponseOption(opts ...BindingOption) BindingOption {
	return func(bc *BindingConfiguration) {
		bc.responseOverrides = append(bc.responseOverrides, opts...)
	}
}

// defaultUnmarshal and defaultMarshal reference the default functions from the existing code.
// These are defined at package level to avoid circular imports.
var (
	defaultUnmarshal Unmarshal
	defaultMarshal   Marshal
)

func init() {
	// Import the default functions from encoding/json
	// These match the defaults in NewBindFromRequestConfiguration and NewBindToResponseConfiguration
	defaultUnmarshal = func(data []byte, v any) error {
		return json.Unmarshal(data, v)
	}
	defaultMarshal = func(v any) ([]byte, error) {
		return json.Marshal(v)
	}
}
