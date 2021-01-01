package fields

import (
	"strings"
)

func (f *field) externalNameTag(nameTag string) {
	if nameTag == "" {
		f.externalName = f.name
		return
	}

	nameTagParts := strings.Split(nameTag, ",")
	name := strings.TrimSpace(nameTagParts[0])

	if name == "" {
		f.externalName = f.name
		return
	}

	f.externalName = name
}

func (fs *Fields) ExternalNameToFieldName(externalName string) (string, bool) {
	if field, ok := fs.fieldMap[externalName]; ok {
		return field.name, ok
	} else {
		return externalName, ok
	}
}
