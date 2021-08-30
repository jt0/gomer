package http

type BindDirectiveConfiguration struct {
	// Default prefixes for qualified directives
	PathBindingPrefix       string
	HeaderBindingPrefix     string
	QueryParamBindingPrefix string
	PayloadBindingPrefix    string

	// Default values for unqualified directives
	SkipField    string
	IncludeField string
	BindBody     string

	// Defines how a field's binding be handled if no directive is specified. Default is to skip.
	EmptyDirectiveHandling EmptyDirectiveHandlingType

	OmitEmptyDirective    string
	IncludeEmptyDirective string

	// Defines how an empty value is marshaled unless overridden by OmitEmptyDirective or IncludeEmptyDirective. Default
	// is to omit.
	EmptyValueHandlingDefault EmptyValueHandlingType
}

func NewBindDirectiveConfiguration() BindDirectiveConfiguration {
	return BindDirectiveConfiguration{
		PathBindingPrefix:         DefaultPathBindingPrefix,
		HeaderBindingPrefix:       DefaultHeaderBindingPrefix,
		QueryParamBindingPrefix:   DefaultQueryParamBindingPrefix,
		PayloadBindingPrefix:      DefaultPayloadBindingPrefix,
		SkipField:                 DefaultSkipFieldDirective,
		IncludeField:              DefaultBindToFieldNameDirective,
		BindBody:                  DefaultBodyBindingDirective,
		EmptyDirectiveHandling:    DefaultEmptyDirectiveHandling,
		OmitEmptyDirective:        DefaultOmitEmptyDirective,
		IncludeEmptyDirective:     DefaultIncludeEmptyDirective,
		EmptyValueHandlingDefault: DefaultEmptyValueHandlingDefault,
	}
}

type EmptyDirectiveHandlingType int

const (
	SkipField EmptyDirectiveHandlingType = iota
	BindToFieldName
)

type EmptyValueHandlingType int

const (
	OmitEmpty EmptyValueHandlingType = iota
	IncludeEmpty
)

const (
	DefaultContentType               = "application/json"
	DefaultPathBindingPrefix         = "path."
	DefaultHeaderBindingPrefix       = "header."
	DefaultQueryParamBindingPrefix   = "query."
	DefaultPayloadBindingPrefix      = ""
	DefaultSkipFieldDirective        = "-"
	DefaultBindToFieldNameDirective  = "+"
	DefaultBodyBindingDirective      = "body"
	DefaultEmptyDirectiveHandling    = SkipField
	DefaultOmitEmptyDirective        = "omitempty"
	DefaultIncludeEmptyDirective     = "includeempty"
	DefaultEmptyValueHandlingDefault = OmitEmpty

	ContentTypeHeader = "Content-Type"
	AcceptsHeader     = "Accepts"

	AcceptLanguageKey = "$_accept_language"

	pathPartsKey   = "$_path_parts"
	queryParamsKey = "$_query_params"
	headersKey     = "$_headers"
	bodyBytesKey   = "$_body_bytes"

	// toolsWithContextKey = "$_tools_with_context"
)

//
// func toApplications(tc fields.ToolContext, toolNames ...string) []fields.Application {
// 	applications := make([]fields.Application, len(toolNames))
// 	for i, toolName := range toolNames {
// 		applications[i] = fields.Application{toolName, tc}
// 	}
// 	return applications
// }
