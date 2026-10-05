package structs_test

import (
	"reflect"
	"sync"
	"testing"

	"github.com/jt0/gomer/bind"
	"github.com/jt0/gomer/gomerr"
	"github.com/jt0/gomer/structs"
)

type (
	allTools1 struct {
		A string `in:"a" out:"a" validate:"len(1,8)"`
	}
	allTools2 struct {
		A string `in:"a" out:"a" validate:"len(1,8)"`
	}
	allTools3 struct {
		A string `in:"a" out:"a" validate:"len(1,8)"`
	}
	allTools4 struct {
		A string `in:"a" out:"a" validate:"len(1,8)"`
	}
	allTools5 struct {
		A string `in:"a" out:"a" validate:"len(1,8)"`
	}
	allTools6 struct {
		A string `in:"a" out:"a" validate:"len(1,8)"`
	}
	allTools7 struct {
		A string `in:"a" out:"a" validate:"len(1,8)"`
	}
	allTools8 struct {
		A string `in:"a" out:"a" validate:"len(1,8)"`
	}
)

// TestApplyTools_ConcurrentFirstUseAllTools drives the bind in and out tools through
// structs.ApplyTools, which prepares types in the shared preparedStructs map. Each of eight
// never-seen types is prepared by both tools at once, so the first use of every (type, tool) pair
// races on the map and on the per-type prepared state. Run under -race.
func TestApplyTools_ConcurrentFirstUseAllTools(t *testing.T) {
	binder := bind.NewBinder()
	fresh := []func() any{
		func() any { return &allTools1{"a"} },
		func() any { return &allTools2{"a"} },
		func() any { return &allTools3{"a"} },
		func() any { return &allTools4{"a"} },
		func() any { return &allTools5{"a"} },
		func() any { return &allTools6{"a"} },
		func() any { return &allTools7{"a"} },
		func() any { return &allTools8{"a"} },
	}
	tools := []*structs.Tool{binder.InTool, binder.OutTool}

	var wg sync.WaitGroup
	for _, newValue := range fresh {
		for _, tool := range tools {
			wg.Add(1)
			go func() {
				defer wg.Done()
				tc := structs.EnsureContext().
					With(bind.InKey, map[string]any{"a": "a"}).
					With(bind.OutKey, map[string]any{})
				if ge := structs.ApplyTools(newValue(), tc, tool); ge != nil {
					t.Errorf("%T: %v", newValue(), ge)
				}
			}()
		}
	}
	wg.Wait()
}

type composedIn struct {
	A string `in:"+&$composedNoop"`
}

// A composed directive whose sides both resolve prepares without error.
func TestPreprocessComposedDirective(t *testing.T) {
	noop := func(reflect.Value, reflect.Value, structs.ToolContext) (any, gomerr.Gomerr) { return nil, nil }
	if ge := structs.RegisterToolFunction("$composedNoop", noop); ge != nil {
		t.Fatal(ge)
	}
	inTool := bind.NewInTool(bind.NewConfiguration(), structs.StructTagDirectiveProvider{"in"})
	if ge := structs.Preprocess(&composedIn{}, inTool); ge != nil {
		t.Fatal(ge)
	}
}
