package godynamo

import (
	"database/sql/driver"
	"errors"
	"reflect"
	"strconv"

	"github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/smithy-go"
	"github.com/btnguyen2k/consu/reddo"
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

var (
	typeM    = reflect.TypeOf(make(map[string]interface{}))
	typeL    = reflect.TypeOf(make([]interface{}, 0))
	typeS    = reddo.TypeString
	typeBool = reddo.TypeBool
	typeN    = reddo.TypeFloat
	typeTime = reddo.TypeTime
)

// IsAwsError returns true if err is an AWS-specific error, and it matches awsErrCode.
func IsAwsError(err error, awsErrCode string) bool {
	var aerr *smithy.OperationError
	if errors.As(err, &aerr) {
		var herr *http.ResponseError
		if errors.As(aerr.Err, &herr) {
			return reflect.TypeOf(herr.Err).Elem().Name() == awsErrCode
		}
	}
	//if aerr, ok := err.(*smithy.OperationError); ok {
	//	if herr, ok := aerr.Err.(*http.ResponseError); ok {
	//		return reflect.TypeOf(herr.Err).Elem().Name() == awsErrCode
	//	}
	//}
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

// ToAttributeValueUnsafe marshals a Go value to AWS AttributeValue, ignoring error.
//
// @Available since v0.2.0
func ToAttributeValueUnsafe(value interface{}) types.AttributeValue {
	av, _ := ToAttributeValue(value)
	return av
}

// ToAttributeValue marshals a Go value to AWS AttributeValue.
func ToAttributeValue(value interface{}) (types.AttributeValue, error) {
	switch v := value.(type) {
	case driver.Valuer:
		dv, err := v.Value()
		if err != nil {
			return &types.AttributeValueMemberNULL{Value: true}, err
		}
		return ToAttributeValue(dv)
	case types.AttributeValue:
		return v, nil
	case types.AttributeValueMemberB:
		return &v, nil
	case types.AttributeValueMemberBOOL:
		return &v, nil
	case types.AttributeValueMemberBS:
		return &v, nil
	case types.AttributeValueMemberL:
		return &v, nil
	case types.AttributeValueMemberM:
		return &v, nil
	case types.AttributeValueMemberN:
		return &v, nil
	case types.AttributeValueMemberNS:
		return &v, nil
	case types.AttributeValueMemberNULL:
		return &v, nil
	case types.AttributeValueMemberS:
		return &v, nil
	case types.AttributeValueMemberSS:
		return &v, nil
	default:
		return attributevalue.Marshal(value)
	}
}

// nameFromAttributeValue returns the name of the attribute value.
//
// e.g.
//
//	types.AttributeValueMemberB -> "B"
//	types.AttributeValueMemberBOOL -> "BOOL"
func nameFromAttributeValue(v interface{}) string {
	// De-reference pointer
	for reflect.TypeOf(v).Kind() == reflect.Ptr {
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
	default:
		return ""
	}
}
