package dynamodb

import (
	"fmt"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"unicode"

	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/jt0/gomer/data"
	"github.com/jt0/gomer/data/dataerr"
	"github.com/jt0/gomer/flect"
	"github.com/jt0/gomer/gomerr"
)

type persistableType struct {
	name             string
	dbNames          map[string]string // field name -> storage name
	keyFields        map[string]bool   // field names that participate in keys (should not be stored as separate attributes)
	constraintFields map[string]bool   // field names that participate in any constraint (used for update optimization)
	resolver         ItemResolver
}

func newPersistableType(table *table, persistableName string, pType reflect.Type) (*persistableType, gomerr.Gomerr) {
	pt := &persistableType{
		name:             persistableName,
		dbNames:          make(map[string]string),
		keyFields:        make(map[string]bool),
		constraintFields: make(map[string]bool),
		resolver:         resolver(pType),
	}

	if errors := pt.processFields(pType, "", table, make([]gomerr.Gomerr, 0)); len(errors) > 0 {
		return nil, gomerr.Configuration("'db' tag errors found for type: " + persistableName).Wrap(gomerr.Batcher(errors))
	}

	return pt, nil
}

func resolver(pt reflect.Type) func(any) (any, gomerr.Gomerr) {
	return func(i any) (any, gomerr.Gomerr) {
		m, ok := i.(map[string]types.AttributeValue)
		if !ok {
			return nil, gomerr.Internal("dynamodb item is not a map[string]types.AttributeValue").AddAttribute("actual", i)
		}

		resolved := reflect.New(pt).Interface().(data.Persistable)

		err := attributevalue.UnmarshalMap(m, resolved)
		if err != nil {
			return nil, gomerr.Unmarshal(resolved.TypeName(), m, resolved).Wrap(err)
		}

		return resolved, nil
	}
}

func (pt *persistableType) processFields(structType reflect.Type, fieldPath string, table *table, errors []gomerr.Gomerr) []gomerr.Gomerr {
	for field := range structType.Fields() {
		fieldName := field.Name

		if field.Type.Kind() == reflect.Struct && field.Anonymous {
			errors = pt.processFields(field.Type, fieldPath+fieldName+".", table, errors)
		} else if unicode.IsLower([]rune(fieldName)[0]) {
			continue
		} else {
			pt.processNameTag(fieldName, field.Tag.Get("db.name"))

			errors = pt.processConstraintsTag(fieldName, field.Tag.Get("db.constraints"), errors)
			errors = pt.processKeysTag(fieldName, field.Tag.Get("db.keys"), table.indexes, errors)
		}
	}

	return errors
}

func (pt *persistableType) processNameTag(fieldName string, tag string) {
	if tag == "" {
		return
	}

	pt.dbNames[fieldName] = tag
}

func (pt *persistableType) processConstraintsTag(fieldName string, tag string, errors []gomerr.Gomerr) []gomerr.Gomerr {
	if tag == "" {
		return errors
	}

	constraints := constraintsRegexp.FindAllStringSubmatch(tag, -1)
	if constraints == nil {
		return append(errors, gomerr.Configuration("invalid `db.constraints` value: "+tag).AddAttribute("field", fieldName))
	}

	for _, c := range constraints {
		switch c[1] {
		case "unique":
			fieldTuple := []string{fieldName}
			if c[3] != "" {
				additionalFields := strings.Split(strings.ReplaceAll(c[3], " ", ""), ",")
				fieldTuple = append(fieldTuple, additionalFields...)
			}

			// Mark ALL fields in the tuple as participating in constraints (for update optimization)
			for _, f := range fieldTuple {
				pt.constraintFields[f] = true
			}
		}
	}

	return errors
}

var ddbKeyStatementRegexp = regexp.MustCompile(`(!)?([+-])?(?:([\w-.]+):)?(pk|sk)(?:.(\d))?(?:=(_|'\w+'))?`)

// processKeysTag parses a field's `db.keys` tag and registers it as a key field for the appropriate
// index(es). Each comma-separated statement in the tag assigns the field (or a static value) to a
// position in an index's partition or sort key.
//
// The `=_` (reset) syntax allows a child persistable type to clear inherited key field assignments
// for a specific index key and redefine them. This is needed in single-table designs where a child
// type shares an index with its parent but requires different key composition for queries to work
// correctly against that type. Resets must appear before any non-reset statements in the tag.
func (pt *persistableType) processKeysTag(fieldName string, tag string, indexes map[string]*index, errors []gomerr.Gomerr) []gomerr.Gomerr {
	if tag == "" {
		return errors
	}

	var resetsComplete bool
	for keyStatement := range strings.SplitSeq(strings.ReplaceAll(tag, " ", ""), ",") {
		groups := ddbKeyStatementRegexp.FindStringSubmatch(keyStatement)
		if groups == nil {
			errors = append(errors, gomerr.Configuration("invalid `db.keys` value: "+keyStatement).AddAttribute("field", fieldName))
			continue
		}

		idx, ok := indexes[groups[3]]
		if !ok {
			errors = append(errors, gomerr.Configuration("undefined index: "+groups[3]).AddAttribute("field", fieldName))
			continue
		}

		var key *keyAttribute
		if groups[4] == "pk" {
			key = idx.pk
		} else {
			key = idx.sk
		}

		var partIndex int // default to index 0
		if groups[5] != "" {
			partIndex, _ = strconv.Atoi(groups[5])
		}

		kfName := fieldName   // Use local variable to avoid modifying parameter across iterations
		if groups[6] == "_" { // Underscore indicates the fields in idx's key should be re-evaluated as key fields
			if resetsComplete {
				errors = append(errors, gomerr.Configuration("resets must be ordered first in a tag: "+tag).AddAttribute("field", fieldName))
				continue
			}
			notKeyFields := make(map[string]struct{})
			for _, field := range key.keyFieldsByPersistable[pt.name] {
				if field.name[0] != '\'' { // exclude static keys
					notKeyFields[field.name] = struct{}{}
				}
			}
			identifyKeyFields(indexes, key, pt.name, notKeyFields)
			for notKeyField := range notKeyFields {
				delete(pt.keyFields, notKeyField)
			}

			key.keyFieldsByPersistable[pt.name] = nil
			continue
		} else if groups[6] != "" { // If non-empty, this field has a static value. Replace with that value.
			kfName = groups[6]
		} else {
			// Mark non-static field as a key field (should not be stored as separate attribute)
			pt.keyFields[fieldName] = true
		}
		resetsComplete = true

		// TODO: Determine scenarios where skLength/skMissing don't map to desired behavior. May need preferred
		//       priority levels to compensate
		kf := keyField{name: kfName, preferred: groups[1] == "!", ascending: groups[2] != "-"}
		var ge gomerr.Gomerr
		key.keyFieldsByPersistable[pt.name], ge = insertAtIndex(key.keyFieldsByPersistable[pt.name], &kf, partIndex)
		if ge != nil {
			errors = append(errors, ge.AddAttribute("field", fieldName).AddAttribute("idx", idx.friendlyName()))
		}
	}

	return errors
}

func identifyKeyFields(indexes map[string]*index, resetKey *keyAttribute, persistable string, notKeyFields map[string]struct{}) {
	for _, idx := range indexes {
		for _, ka := range idx.keyAttributes() {
			if ka == resetKey {
				continue
			}
			for _, field := range ka.keyFieldsByPersistable[persistable] {
				if field == nil {
					continue
				}
				delete(notKeyFields, field.name)
			}
		}
	}
}

func insertAtIndex(slice []*keyField, value *keyField, index int) ([]*keyField, gomerr.Gomerr) {
	if slice == nil || cap(slice) == 0 {
		slice = make([]*keyField, 0, index+1)
	}

	lenKeyFields := len(slice)
	capKeyFields := cap(slice)
	if index < lenKeyFields {
		if slice[index] != nil {
			return nil, gomerr.Configuration(fmt.Sprintf("already found value '%v' at index %d", (*slice[index]).name, index))
		}
	} else if index < capKeyFields {
		slice = slice[0 : index+1]
	} else {
		slice = append(slice, make([]*keyField, index+1-capKeyFields)...)
	}

	slice[index] = value

	return slice, nil
}

func (pt *persistableType) dbNameToFieldName(dbName string) string {
	for k, v := range pt.dbNames {
		if v == dbName {
			return k
		}
	}

	return dbName // If we reach here, no alternative dbName was offered so must be the same as the field name
}

func (pt *persistableType) convertFieldNamesToDbNames(av *map[string]types.AttributeValue) {
	if len(pt.dbNames) == 0 {
		return
	}

	cv := make(map[string]types.AttributeValue, len(*av))
	for k, v := range *av {
		if dbName, ok := pt.dbNames[k]; ok {
			if dbName != "-" {
				cv[dbName] = v
			}
		} else {
			cv[k] = v
		}
	}

	*av = cv
}

func (pt *persistableType) removeKeyFieldsFromAttributes(av *map[string]types.AttributeValue) {
	if len(pt.keyFields) == 0 {
		return
	}

	for fieldName := range pt.keyFields {
		// Get the actual attribute name (might be different due to db.name tag)
		attrName := fieldName
		if dbName, ok := pt.dbNames[fieldName]; ok && dbName != "-" {
			attrName = dbName
		}
		delete(*av, attrName)
	}
}

type populatedFieldInfo struct {
	value   string
	keyName string
}

// populateKeyFieldsFromAttributes extracts key field values from composite keys and sets them on the struct.
// This is the reverse of removeKeyFieldsFromAttributes - it populates fields that were not stored as separate attributes.
func (pt *persistableType) populateKeyFieldsFromAttributes(p data.Persistable, av map[string]types.AttributeValue, indexes map[string]*index, separator byte, validateConsistency bool) gomerr.Gomerr {
	if len(pt.keyFields) == 0 {
		return nil
	}

	escape := separator + 1
	pv := reflect.ValueOf(p).Elem()
	populated := make(map[string]*populatedFieldInfo) // Track field values and which key they came from

	// Iterate through all indexes to extract key values
	for _, idx := range indexes {
		for _, keyAttr := range idx.keyAttributes() {
			keyFields := keyAttr.keyFieldsByPersistable[pt.name]
			if keyFields == nil {
				continue
			}

			// Get the composite key value from attributes
			keyValue, ok := av[keyAttr.name]
			if !ok {
				continue
			}

			var keyString string
			switch v := keyValue.(type) {
			case *types.AttributeValueMemberS:
				keyString = v.Value
			case *types.AttributeValueMemberN:
				keyString = v.Value
			default:
				continue
			}

			// Split and unescape the composite key
			segments := unescapeAndSplit(keyString, separator, escape)

			// Map segments back to struct fields
			segmentIndex := 0
			for _, kf := range keyFields {
				// Skip static values (they start with `'`)
				if kf.name[:1] == "'" {
					segmentIndex++
					continue
				}

				// Get the segment value
				if segmentIndex >= len(segments) {
					break
				}
				segmentValue := segments[segmentIndex]
				segmentIndex++

				// Check if we've already populated this field
				if existing, alreadyPopulated := populated[kf.name]; alreadyPopulated {
					// Validate consistency if enabled
					if validateConsistency && existing.value != segmentValue {
						return dataerr.KeyFieldMismatch(kf.name, existing.value, segmentValue, existing.keyName, keyAttr.name, p)
					}
					continue
				}

				// Set the field value
				field := pv.FieldByName(kf.name)
				if !field.IsValid() || !field.CanSet() {
					continue
				}

				// Handle empty segments - set to zero value (matches write behavior)
				var valueToSet any
				if segmentValue == "" {
					valueToSet = flect.ZeroVal
				} else {
					valueToSet = segmentValue
				}

				// Convert string to appropriate type and set using flect
				if ge := flect.SetValue(field, valueToSet); ge != nil {
					return gomerr.Unmarshal(pt.name, av, p).AddAttribute("field", kf.name).AddAttribute("value", segmentValue).Wrap(ge)
				}

				populated[kf.name] = &populatedFieldInfo{value: segmentValue, keyName: keyAttr.name}
			}
		}
	}

	return nil
}
