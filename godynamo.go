// Package godynamo provides database/sql driver for AWS DynamoDB.
package godynamo

import (
	"database/sql/driver"
	"reflect"
	"strconv"

	"github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/smithy-go"
)

const (
	// Version of package godynamo.
	Version = "0.2.0"
)

var (
	dataTypes = map[string]types.ScalarAttributeType{
		"BINARY": "B",
		"B":      "B",
		"NUMBER": "N",
		"N":      "N",
		"STRING": "S",
		"S":      "S",
	}

	keyTypes = map[string]types.KeyType{
		"HASH":  "HASH",
		"RANGE": "RANGE",
	}

	tableClasses = map[string]types.TableClass{
		"STANDARD":    types.TableClassStandard,
		"STANDARD_IA": types.TableClassStandardInfrequentAccess,
	}
)

// IsAwsError returns true if err is an AWS-specific error, and it matches awsErrCode.
func IsAwsError(err error, awsErrCode string) bool {
	if aerr, ok := err.(*smithy.OperationError); ok {
		if herr, ok := aerr.Err.(*http.ResponseError); ok {
			return reflect.TypeOf(herr.Err).Elem().Name() == awsErrCode
		}
	}
	return false
}

// ValuesToNamedValues transforms a []driver.Value to []driver.NamedValue.
//
// @Available since v0.2.0
func ValuesToNamedValues(values []driver.Value) []driver.NamedValue {
	result := make([]driver.NamedValue, len(values))
	for i, v := range values {
		result[i] = driver.NamedValue{Name: "$" + strconv.Itoa(i+1), Ordinal: i, Value: v}
	}
	return result
}

// // NamedValuesToValues transforms a []driver.NamedValue to []driver.Value.
// //
// // @Available since v0.2.0
// func NamedValuesToValues(values []driver.NamedValue) []driver.Value {
// 	result := make([]driver.Value, len(values))
// 	for i, v := range values {
// 		result[i] = v.Value
// 	}
// 	return result
// }

// ToAttributeValue marshals a Go value to AWS AttributeValue.
func ToAttributeValue(value interface{}) (types.AttributeValue, error) {
	if av, ok := value.(types.AttributeValue); ok {
		return av, nil
	}
	switch value.(type) {
	case types.AttributeValueMemberB:
		v := value.(types.AttributeValueMemberB)
		return &v, nil
	case types.AttributeValueMemberBOOL:
		v := value.(types.AttributeValueMemberBOOL)
		return &v, nil
	case types.AttributeValueMemberBS:
		v := value.(types.AttributeValueMemberBS)
		return &v, nil
	case types.AttributeValueMemberL:
		v := value.(types.AttributeValueMemberL)
		return &v, nil
	case types.AttributeValueMemberM:
		v := value.(types.AttributeValueMemberM)
		return &v, nil
	case types.AttributeValueMemberN:
		v := value.(types.AttributeValueMemberN)
		return &v, nil
	case types.AttributeValueMemberNS:
		v := value.(types.AttributeValueMemberNS)
		return &v, nil
	case types.AttributeValueMemberNULL:
		v := value.(types.AttributeValueMemberNULL)
		return &v, nil
	case types.AttributeValueMemberS:
		v := value.(types.AttributeValueMemberS)
		return &v, nil
	case types.AttributeValueMemberSS:
		v := value.(types.AttributeValueMemberSS)
		return &v, nil
	}
	return attributevalue.Marshal(value)
}

func goTypeToDynamodbType(typ reflect.Type) string {
	if typ == nil {
		return ""
	}
	switch typ.Kind() {
	case reflect.Bool:
		return "BOOLEAN"
	case reflect.String:
		return "STRING"
	case reflect.Float32, reflect.Float64:
		return "NUMBER"
	case reflect.Array, reflect.Slice:
		return "ARRAY"
	case reflect.Map:
		return "MAP"
	}
	return ""
}

// nameFromAttributeValue returns the name of the attribute value.
// e.g.
// types.AttributeValueMemberB -> "B"
// types.AttributeValueMemberBOOL -> "BOOL"
func nameFromAttributeValue(v interface{}) string {
	// De-reference pointer
	if reflect.TypeOf(v).Kind() == reflect.Ptr {
		v = reflect.ValueOf(v).Elem().Interface()
	}

	switch v.(type) {
	case types.AttributeValueMemberB:
		return "B"
	case types.AttributeValueMemberBOOL:
		return "BOOL"
	case types.AttributeValueMemberBS:
		return "BS"
	case types.AttributeValueMemberL:
		return "L"
	case types.AttributeValueMemberM:
		return "M"
	case types.AttributeValueMemberN:
		return "N"
	case types.AttributeValueMemberNS:
		return "NS"
	case types.AttributeValueMemberNULL:
		return "NULL"
	case types.AttributeValueMemberS:
		return "S"
	case types.AttributeValueMemberSS:
		return "SS"
	}
	return ""

}
