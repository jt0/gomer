package bind_test

import (
	"encoding/json"
	"testing"

	"github.com/jt0/gomer/_test/assert"
	"github.com/jt0/gomer/bind"
)

type OutStruct struct {
	Foo string       `out:"+"`
	Bar BarInterface `out:"+"`
}

type BarInterface interface {
	Bar()
}

type StringBar string

func (*StringBar) Bar() {}

type IntBar int

func (*IntBar) Bar() {}

func TestInterface(t *testing.T) {
	var barInterface BarInterface
	barOut := OutStruct{"foo", barInterface}
	outBytes := bindToJson(t, barOut)
	assert.JsonEqual(t, []byte(`{"Foo": "foo"}`), outBytes)

	var stringBar StringBar
	barInterface = &stringBar
	stringBarOut := OutStruct{"foo", barInterface}
	stringBarBytes := bindToJson(t, stringBarOut)
	assert.JsonEqual(t, []byte(`{"Foo": "foo", "Bar": ""}`), stringBarBytes)

	stringBar = "bar"
	stringBarOut = OutStruct{"foo", barInterface}
	stringBarBytes = bindToJson(t, stringBarOut)
	assert.JsonEqual(t, []byte(`{"Foo": "foo", "Bar": "bar"}`), stringBarBytes)

	var intBar IntBar
	barInterface = &intBar
	intBarOut := OutStruct{"foo", barInterface}
	intBarBytes := bindToJson(t, intBarOut)
	assert.JsonEqual(t, []byte(`{"Foo": "foo", "Bar": 0}`), intBarBytes)

	intBar = 1
	intBarOut = OutStruct{"foo", barInterface}
	intBarBytes = bindToJson(t, intBarOut)
	assert.JsonEqual(t, []byte(`{"Foo": "foo", "Bar": 1}`), intBarBytes)
}

func bindToJson(t *testing.T, stringBarOut OutStruct) []byte {
	data, ge := bind.Out(stringBarOut, bind.DefaultOutTool)
	assert.Success(t, ge)
	bytes, err := json.MarshalIndent(data, "", "  ")
	assert.Success(t, err)

	return bytes
}
