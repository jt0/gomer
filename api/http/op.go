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

func NewOp(method Method, resourceType resource.Type) Op {
	rtBits := toResourceTypeBits(resourceType)
	if method&^methodMask != 0 || rtBits&^resourceTypeMask != 0 {
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

func (o Op) ResourceType() resource.Type {
	return toResourceType(o & resourceTypeMask)
}

func (o Op) IsBuiltIn() bool {
	return o&creatorTypeMask == builtIn
}

type Method = Op

const (
	noMethod Method = iota // 0b00000000
	Put                    // 0b00000001
	Post                   // 0b00000010
	Get                    // 0b00000011
	Patch                  // 0b00000100
	Delete_                // 0b00000101
	Head                   // 0b00000110
	Options                // 0b00000111

	methodBitsCount = 5
	methodMask      = 1<<methodBitsCount - 1 // 0b00011111
)

// Should be applied in order to consts above
var methods = [1 << methodBitsCount]string{
	"", // noMethod
	"PUT",
	"POST",
	"GET",
	"PATCH",
	"DELETE",
	"HEAD",
	"OPTIONS",
}

type resourceType = Op

const (
	noResource resourceType = iota << methodBitsCount // 0b00000000
	collection                                        // 0b00100000
	instance                                          // 0b01000000
	reserved                                          // 0b01100000

	resourceTypeBitsCount = 2
	resourceTypeMask      = (1<<resourceTypeBitsCount - 1) << methodBitsCount
)

func toResourceTypeBits(resourceType resource.Type) resourceType {
	switch resourceType {
	case resource.CollectionType:
		return collection
	case resource.InstanceType:
		return instance
	default:
		return noResource
	}
}

func toResourceType(resourceType resourceType) resource.Type {
	switch resourceType {
	case collection:
		return resource.CollectionType
	case instance:
		return resource.InstanceType
	default:
		return ""
	}
}

const (
	builtIn  = iota << (methodBitsCount + resourceTypeBitsCount) // 0b00000000
	customer                                                     // 0b10000000

	creatorTypeMask = 1 << (methodBitsCount + resourceTypeBitsCount) // 0b10000000
)

const (
	PutCollection     = Put + collection
	PostCollection    = Post + collection
	GetCollection     = Get + collection
	PatchCollection   = Patch + collection
	DeleteCollection  = Delete_ + collection
	HeadCollection    = Head + collection
	OptionsCollection = Options + collection

	PutInstance     = Put + instance
	PostInstance    = Post + instance
	GetInstance     = Get + instance
	PatchInstance   = Patch + instance
	DeleteInstance  = Delete_ + instance
	HeadInstance    = Head + instance
	OptionsInstance = Options + instance
)
