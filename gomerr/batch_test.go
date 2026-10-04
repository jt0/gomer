package gomerr

import (
	"strconv"
	"testing"
)

func errs(n int) []Gomerr {
	out := make([]Gomerr, n)
	for i := range out {
		out[i] = Internal(strconv.Itoa(i))
	}
	return out
}

//goland:noinspection ALL
func TestBatch_ErrorOrNil(t *testing.T) {
	for _, n := range []int{0, 1, 2, 5} {
		t.Run(strconv.Itoa(n), func(t *testing.T) {
			in := errs(n)
			var eb ErrorBatch
			for _, ge := range in {
				eb.Capture(ge)
			}
			ge := eb.GomerrOrNil()
			switch n {
			case 0:
				if ge != nil {
					t.Fatalf("want nil, got %v", ge)
				}
				return
			case 1:
				if ge != in[0] {
					t.Fatalf("want the single error, got %v", ge)
				}
				return
			}
			be, ok := ge.(*BatchError)
			if !ok {
				t.Fatalf("want *BatchError, got %T", ge)
			}
			got := be.Errors()
			if len(got) != n {
				t.Fatalf("Errors() has %d, want %d", len(got), n)
			}
			for i := range in {
				if got[i] != in[i] {
					t.Fatalf("Errors()[%d] = %v, want %v", i, got[i], in[i])
				}
			}
			if items := be.ToMap()["Errors"].([]map[string]any); len(items) != n {
				t.Fatalf("ToMap has %d errors, want %d", len(items), n)
			}
			if _, truncated := be.Attributes()[TruncatedAttribute]; truncated {
				t.Fatal("unexpected truncated attribute")
			}
		})
	}
}

func TestBatch_AddFlattensNestedBatches(t *testing.T) {
	in := errs(3)
	var eb ErrorBatch
	eb.Capture(in[0])
	eb.Capture(ToGomerrOrNil(in[1], in[2]))
	if got := eb.GomerrOrNil().(*BatchError).Errors(); len(got) != 3 || got[1] != in[1] || got[2] != in[2] {
		t.Fatalf("Errors() = %v, want the three errors flattened", got)
	}
}

func TestBatch_DropsErrorsPastTheLimit(t *testing.T) {
	// Exactly at the limit the batch is full but nothing was dropped, so it is not truncated.
	var eb ErrorBatch
	for _, ge := range errs(errorBatchCapacity) {
		eb.Capture(ge)
	}
	if eb.Truncated() {
		t.Fatal("truncated at exactly the limit, want not truncated")
	}
	//goland:noinspection GoTypeAssertionOnErrors
	be := eb.GomerrOrNil().(*BatchError)
	if len(be.Errors()) != errorBatchCapacity {
		t.Fatalf("kept %d errors, want %d", len(be.Errors()), errorBatchCapacity)
	}
	if _, truncated := be.Attributes()[TruncatedAttribute]; truncated {
		t.Fatal("unexpected truncated attribute at exactly the limit")
	}

	// Past the limit the extra errors are dropped and the batch is marked truncated.
	eb = ErrorBatch{}
	for _, ge := range errs(errorBatchCapacity + 5) {
		eb.Capture(ge)
	}
	if !eb.Truncated() {
		t.Fatal("want truncated after errors past the limit")
	}
	//goland:noinspection GoTypeAssertionOnErrors
	be = eb.GomerrOrNil().(*BatchError)
	if len(be.Errors()) != errorBatchCapacity {
		t.Fatalf("kept %d errors, want %d", len(be.Errors()), errorBatchCapacity)
	}
	if be.Attributes()[TruncatedAttribute] != true {
		t.Fatal("want the truncated attribute")
	}
}

var sink Gomerr

func TestBatch_EmptyCostsNothing(t *testing.T) {
	if got := testing.AllocsPerRun(100, func() {
		var eb ErrorBatch
		eb.Capture(nil)
		sink = eb.GomerrOrNil()
	}); got != 0 {
		t.Fatalf("an empty batch made %.0f allocations, want 0", got)
	}
}
