package fields

import (
	"strings"

	"github.com/jt0/gomer/gomerr"
)

func (f *field) idTag(idTag string) gomerr.Gomerr {
	if idTag == "" {
		return nil
	}

	parts := strings.Split(idTag, ",")
	if len(parts) > 2 {
		return gomerr.Configuration("Expected format '(<defaultTagValue>)?(,<externalName>)?', but got: " + idTag)
	}

	f.defaultTag(parts[0])

	if len(parts) == 2 {
		f.externalName = strings.TrimSpace(parts[1])
	}

	return nil
}
