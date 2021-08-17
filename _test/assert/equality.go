package assert

import (
	"encoding/json"
	"fmt"
	"strconv"
	"testing"
)

// JsonEqual accepts two JSON-containing byte arrays and compares their content equality (rather than their byte
// equality). This allows ordering to be ignored
func JsonEqual(tb testing.TB, expected, actual []byte, msg ...interface{}) {
	var expectedMap map[string]interface{}
	err := json.Unmarshal(expected, &expectedMap)
	Success(tb, err)

	var actualMap map[string]interface{}
	err = json.Unmarshal(actual, &actualMap)
	Success(tb, err)

	if !mapsEqual(expectedMap, actualMap, "") {
		fmt.Println(append([]interface{}{"Failed equality check"}, msg...)...)
		tb.FailNow()
	}
}

// mapsEqual accepts two map[string]interface{}s, walking both looking at per-key and value equivalency. Any mismatches
// will be printed to standard out and overall inequality will result in a
func mapsEqual(expected, actual map[string]interface{}, path string) bool {
	if expected == nil || actual == nil {
		if expected == nil && actual == nil {
			return true
		}
		fmt.Println("\tmap and nil cannot be compared")
		return false
	}

	m1 := expected
	m2 := actual
	equal := true
	if len(expected) > len(actual) {
		fmt.Printf("\t'expected' has more attributes than 'actual' (%d vs %d)\n", len(m1), len(m2))
		equal = false
	} else if len(expected) < len(actual) {
		fmt.Printf("\t'actual' has more attributes than 'expected' (%d vs %d)\n", len(m2), len(m1))
		equal = false
		m1, m2 = m2, m1 // flip maps so we can emit the missing key
	}

	for k, v1 := range m1 {
		v2, ok := m2[k]
		if !ok {
			println("\tkey missing:", path+k)
			continue
		}
		switch a1 := v1.(type) {
		case map[string]interface{}:
			equal = equal && mapsEqual(v1.(map[string]interface{}), v2.(map[string]interface{}), path+k+".")
		case []interface{}:
			a2 := v2.([]interface{})
			if len(a1) != len(a2) {
				fmt.Printf("\tlen(%s) are not equal (%d vs %d)", path+k, len(a1), len(a2))
				equal = false
			}
			for i, _ := range a1 {
				if i >= len(a2) {
					break
				}
				e2 := a2[i]
				switch e1 := a1[i].(type) {
				case map[string]interface{}:
					// TODO: verify e2's type
					equal = equal && mapsEqual(e1, e2.(map[string]interface{}), path+k+"["+strconv.Itoa(i)+"].")
				default:
					if e1 != e2 {
						fmt.Printf("\tkey %s[%d]'s values not equal: %v != %v\n", path+k, i, e1, e2)
						equal = false
					}
				}
			}
		default:
			if v1 != v2 {
				fmt.Printf("\tkey %s's values not equal: %v != %v\n", path+k, v1, v2)
				equal = false
			}
		}
	}

	return equal
}
