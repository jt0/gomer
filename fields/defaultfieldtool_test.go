package fields_test

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/jt0/gomer/_test/helpers/fields_test"
	"github.com/jt0/gomer/fields"
)

type DefaultTest struct {
	StringWithDefaultFunction string `default:"$color"`
	StringWithDefaultValue    string `default:"=123"`
	IntWithDefaultValue       int    `default:"=123"`
}

const orange = "orange"

func init() {
	fields.RegisterFieldFunctions(map[string]func(reflect.Value) interface{}{
		"$color": func(_ reflect.Value) interface{} {
			return orange
		},
		"$next_jersey": func(_ reflect.Value) interface{} {
			return 64
		},
		"$open_position": func(_ reflect.Value) interface{} {
			return "outfielder"
		},
	})

	fields.FieldToolConfigProvider = fields.StructTagConfigProvider{}.WithKey("default", defaultTool)
}

var defaultTool = fields.DefaultFieldTool()

func TestDefaultTool(t *testing.T) {
	fields_test.RunTests(t, []fields_test.TestCase{
		{"Simple test", defaultTool, fields.EnsureContext(), &DefaultTest{}, &DefaultTest{orange, "123", 123}},
	})
}

func ExampleDefaultFieldTool() {
	type BaseballPlayer struct {
		Team           string  `default:"?=<unassigned>"`
		FieldPosition  string  `default:"?$open_position"`
		JerseyNumber   int     `default:"$next_jersey"`
		BattingAverage float32 `default:"?=.500"`
	}

	fields.FieldToolConfigProvider = fields.StructTagConfigProvider{}.WithKey("default", fields.DefaultFieldTool())

	newPlayer := BaseballPlayer{FieldPosition: "pitcher"}
	baseballPlayerFields, _ := fields.Process(reflect.TypeOf(newPlayer), fields.AsNew)
	baseballPlayerFields.ApplyTools(reflect.ValueOf(&newPlayer).Elem(), fields.Application{ToolName: fields.DefaultFieldTool().Name()})

	fmt.Printf("Player #%d (BA: %.3f) playing as a %s for %s",
		newPlayer.JerseyNumber, newPlayer.BattingAverage, newPlayer.FieldPosition, newPlayer.Team)
	// Output: Player #64 (BA: 0.500) playing as a pitcher for <unassigned>
}
