package http

import (
	"github.com/jt0/gomer/resource"
)

// Op has 8 bits where the bottom five bits correspond to the action type, the sixth and seventh bit specifies what
// resource type the action is against (instance or a collection; a singleton counts as an instance) and the highest bit
// indicates if the op is built-in or customer-defined. The seventh bit is reserved. Use NewOp rather than try to
// construct manually to allow for the introduction of new, built-in Method values.
type Op byte

const InvalidHttpOp = 0b00000000

func NewOp(method Method, category resource.Category) Op {
	rtBits := toCategoryBits(category)
	if method&^methodMask != 0 || rtBits&^categoryMask != 0 {
		return InvalidHttpOp
	}

	return customer | rtBits | method
}

func (o Op) IsValid() bool {
	return o != InvalidHttpOp
}

func (o Op) Method() string {
	return methods[o&methodMask]
}

func (o Op) ResourceType() resource.Category {
	return toCategoryConst(o & categoryMask)
}

func (o Op) IsBuiltIn() bool {
	return o&creatorTypeMask == builtIn
}

type Method = Op

const (
	methodNone    Method = iota // 0b00000000
	MethodPut                   // 0b00000001
	MethodPost                  // 0b00000010
	MethodGet                   // 0b00000011
	MethodPatch                 // 0b00000100
	MethodDelete                // 0b00000101
	MethodHead                  // 0b00000110
	MethodOptions               // 0b00000111

	methodBitsCount = 5
	methodMask      = 1<<methodBitsCount - 1 // 0b00011111
)

// Should be applied in order to consts above
var methods = [1 << methodBitsCount]string{
	"", // methodNone
	"PUT",
	"POST",
	"GET",
	"PATCH",
	"DELETE",
	"HEAD",
	"OPTIONS",
}

type resourceCategory = Op

const (
	noResource resourceCategory = iota << methodBitsCount // 0b00000000
	collection                                            // 0b00100000
	instance                                              // 0b01000000
	reserved                                              // 0b01100000

	categoryBitsCount = 2
	categoryMask      = (1<<categoryBitsCount - 1) << methodBitsCount
)

func toCategoryBits(category resource.Category) resourceCategory {
	switch category {
	case resource.CollectionCategory:
		return collection
	case resource.InstanceCategory:
		return instance
	default:
		return noResource
	}
}

func toCategoryConst(resourceType resourceCategory) resource.Category {
	switch resourceType {
	case collection:
		return resource.CollectionCategory
	case instance:
		return resource.InstanceCategory
	default:
		return ""
	}
}

const (
	builtIn  = iota << (methodBitsCount + categoryBitsCount) // 0b00000000
	customer                                                 // 0b10000000

	creatorTypeMask = 1 << (methodBitsCount + categoryBitsCount) // 0b10000000
)

const (
	PutCollection     = MethodPut + collection
	PostCollection    = MethodPost + collection
	GetCollection     = MethodGet + collection
	PatchCollection   = MethodPatch + collection
	DeleteCollection  = MethodDelete + collection
	HeadCollection    = MethodHead + collection
	OptionsCollection = MethodOptions + collection

	PutInstance     = MethodPut + instance
	PostInstance    = MethodPost + instance
	GetInstance     = MethodGet + instance
	PatchInstance   = MethodPatch + instance
	DeleteInstance  = MethodDelete + instance
	HeadInstance    = MethodHead + instance
	OptionsInstance = MethodOptions + instance
)
