package godynamo

import (
	"database/sql"
	"database/sql/driver"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/smithy-go"
)

// init is automatically invoked when the driver is imported
func init() {
	sql.Register("godynamo", &Driver{})
}

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

// Driver is AWS DynamoDB driver for database/sql.
type Driver struct {
}

// Open implements driver.Driver.Open.
//
// connStr is expected in the following format:
//
//	Region=<region>;AkId=<aws-key-id>;Secret_Key=<aws-secret-key>[;Endpoint=<dynamodb-endpoint>]
//
// If not supplied, default value for TimeoutMs is 10 seconds, Version is defaultApiVersion (which is "2018-12-31"), AutoId is true, and InsecureSkipVerify is false
//
// - DefaultDb is added since v0.1.1
// - AutoId is added since v0.1.2
// - InsecureSkipVerify is added since v0.1.4
func (d *Driver) Open(connStr string) (driver.Conn, error) {
	params := make(map[string]string)
	parts := strings.Split(connStr, ";")
	for _, part := range parts {
		tokens := strings.SplitN(strings.TrimSpace(part), "=", 2)
		key := strings.ToUpper(strings.TrimSpace(tokens[0]))
		if len(tokens) == 2 {
			params[key] = strings.TrimSpace(tokens[1])
		} else {
			params[key] = ""
		}
	}

	timeoutMs, err := strconv.Atoi(params["TIMEOUTMS"])
	if err != nil || timeoutMs < 0 {
		timeoutMs = 10000
	}
	opts := dynamodb.Options{
		Credentials: credentials.NewStaticCredentialsProvider(params["AKID"], params["SECRET_KEY"], ""),
		HTTPClient:  http.NewBuildableClient().WithTimeout(time.Millisecond * time.Duration(timeoutMs)),
		Region:      params["REGION"],
	}
	if params["ENDPOINT"] != "" {
		opts.EndpointResolver = dynamodb.EndpointResolverFromURL(params["ENDPOINT"])
		if strings.HasPrefix(params["ENDPOINT"], "http://") {
			opts.EndpointOptions.DisableHTTPS = true
		}
	}
	client := dynamodb.New(opts)
	return &Conn{client: client}, nil
}
