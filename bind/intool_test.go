package bind_test

import (
	"encoding/json"
	"testing"

	"github.com/jt0/gomer/_test/assert"
	"github.com/jt0/gomer/bind"
)

type Foo struct {
	A int `in:"+"`
	a int `in:"+"`
	B int `in:"+"`
	c int `in:"+"`
}

type bar struct {
	Z string `in:"+"`
	z string `in:"+"`
}

type Bat struct {
	Foo `in:"+"`
	bar `in:"+"`

	Foo2 Foo `in:"+"`
	Bar2 bar `in:"+"`
}

func TestAnonymous(t *testing.T) {
	var v Bat
	inData := dataFrom(t, in1)
	ge := bind.In(inData, &v, bind.DefaultInTool)
	assert.Success(t, ge)
}

func dataFrom(t *testing.T, b []byte) map[string]interface{} {
	d := map[string]interface{}{}
	err := json.Unmarshal(b, &d)
	assert.Success(t, err)
	return d
}

var in1 = []byte(`
  {
    "A": 1,
    "a": 2,
    "b": 3,
    "B": 4,
    "c": 5,
    "Foo2": {
      "A": 6,
      "a": 7,
      "b": 8,
      "B": 9,
      "c": 10
    },
    "Z": "bar.Z",
    "z": "bar.z",
    "Bar2": {
      "Z": "Bar2.Z",
      "z": "Bar2.z"
    }
  }
`)

// Lists, maps, structs, pointers, pointer-pointers
// functions
