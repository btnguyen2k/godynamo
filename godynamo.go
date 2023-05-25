// Package godynamo provides database/sql driver for AWS DynamoDB.
package godynamo

import (
	"reflect"

	"github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/smithy-go"
)

const (
	// Version of package godynamo.
	Version = "0.1.0"
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

// IsAwsError returns true if err is an AWS-specific error and it matches awsErrCode.
func IsAwsError(err error, awsErrCode string) bool {
	if aerr, ok := err.(*smithy.OperationError); ok {
		if herr, ok := aerr.Err.(*http.ResponseError); ok {
			return reflect.TypeOf(herr.Err).Elem().Name() == awsErrCode
		}
	}
	return false
}

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
