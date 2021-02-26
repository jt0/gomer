package dynamodb

import (
	"reflect"

	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"

	"github.com/jt0/gomer/data"
	"github.com/jt0/gomer/fields"
	"github.com/jt0/gomer/gomerr"
)

type persistableType struct {
	name     string
	fields   fields.Fields
	resolver ItemResolver
}

func newPersistableType(table *table, persistableName string, pType reflect.Type) (pt *persistableType, ge gomerr.Gomerr) {
	pt = &persistableType{
		name:     persistableName,
		resolver: resolver(pType),
	}

	if pt.fields, ge = fields.Get(pType); ge != nil {
		return nil, ge
	}

	return pt, nil
}

func resolver(pt reflect.Type) func(interface{}) (interface{}, gomerr.Gomerr) {
	return func(i interface{}) (interface{}, gomerr.Gomerr) {
		m, ok := i.(map[string]*dynamodb.AttributeValue)
		if !ok {
			return nil, gomerr.Internal("DynamoDB Item is not a map[string]*dynamodb.AttributeValue").AddAttribute("Actual", i)
		}

		resolved := reflect.New(pt).Interface().(data.Persistable)

		err := dynamodbattribute.UnmarshalMap(m, resolved)
		if err != nil {
			return nil, gomerr.Unmarshal(resolved.TypeName(), m, resolved).Wrap(err)
		}

		return resolved, nil
	}
}
