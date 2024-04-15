package godynamo

import (
	"database/sql"
	"database/sql/driver"
	"github.com/aws/aws-sdk-go-v2/aws"
	"os"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/btnguyen2k/consu/reddo"
)

// init is automatically invoked when the driver is imported
func init() {
	sql.Register("godynamo", &Driver{})
}

// Driver is AWS DynamoDB implementation of driver.Driver.
type Driver struct {
}

func parseParamValue(params map[string]string, typ reflect.Type, validator func(val interface{}) bool,
	defaultVal interface{}, pkeys []string, ekeys []string) interface{} {
	for _, key := range pkeys {
		val, ok := params[key]
		if ok {
			pval, err := reddo.Convert(val, typ)
			if pval == nil || err != nil || (validator != nil && !validator(pval)) {
				return defaultVal
			}
			return pval
		}
	}
	for _, key := range ekeys {
		val := os.Getenv(key)
		if val != "" {
			pval, err := reddo.Convert(val, typ)
			if pval == nil || err != nil || (validator != nil && !validator(pval)) {
				return defaultVal
			}
			return pval
		}
	}
	return defaultVal
}

func parseConnString(connStr string) map[string]string {
	params := make(map[string]string)
	parts := strings.Split(connStr, ";")
	for _, part := range parts {
		tokens := strings.SplitN(strings.TrimSpace(part), "=", 2)
		key := strings.ToUpper(strings.TrimSpace(tokens[0]))
		if len(tokens) >= 2 {
			params[key] = strings.TrimSpace(tokens[1])
		} else {
			params[key] = ""
		}
	}
	return params
}

// Open implements driver.Driver/Open.
//
// connStr is expected in the following format:
//
//	Region=<region>;AkId=<aws-key-id>;Secret_Key=<aws-secret-key>[;Endpoint=<dynamodb-endpoint>][;TimeoutMs=<timeout-in-milliseconds>]
//
// If not supplied, default value for TimeoutMs is 10 seconds.
func (d *Driver) Open(connStr string) (driver.Conn, error) {
	params := parseConnString(connStr)
	timeoutMs := parseParamValue(params, reddo.TypeInt, func(val interface{}) bool {
		return val.(int64) >= 0
	}, int64(10000), []string{"TIMEOUTMS"}, nil).(int64)
	region := parseParamValue(params, reddo.TypeString, nil, "", []string{"REGION"}, []string{"AWS_REGION"}).(string)
	akid := parseParamValue(params, reddo.TypeString, nil, "", []string{"AKID"}, []string{"AWS_ACCESS_KEY_ID", "AWS_AKID"}).(string)
	secretKey := parseParamValue(params, reddo.TypeString, nil, "", []string{"SECRET_KEY", "SECRETKEY"}, []string{"AWS_SECRET_KEY", "AWS_SECRET_ACCESS_KEY"}).(string)
	opts := dynamodb.Options{
		Credentials: credentials.NewStaticCredentialsProvider(akid, secretKey, ""),
		HTTPClient:  http.NewBuildableClient().WithTimeout(time.Millisecond * time.Duration(timeoutMs)),
		Region:      region,
	}
	endpoint := parseParamValue(params, reddo.TypeString, nil, "", []string{"ENDPOINT"}, []string{"AWS_DYNAMODB_ENDPOINT"}).(string)
	if endpoint != "" {
		//opts.EndpointResolver = dynamodb.EndpointResolverFromURL(endpoint)
		opts.BaseEndpoint = aws.String(endpoint)
		if strings.HasPrefix(endpoint, "http://") {
			opts.EndpointOptions.DisableHTTPS = true
		}
	}
	awsConfigLock.RLock()
	defer awsConfigLock.Unlock()
	conf := awsConfig
	if conf != nil {
		client := dynamodb.NewFromConfig(*conf, mergeDynamoDBOptions(opts))
		return &Conn{client: client, timeout: time.Duration(timeoutMs) * time.Millisecond}, nil
	}
	client := dynamodb.New(opts)
	return &Conn{client: client, timeout: time.Duration(timeoutMs) * time.Millisecond}, nil
}

// awsConfig is the AWS configuration to be used by the dynamodb client.
var (
	awsConfigLock = &sync.RWMutex{}
	awsConfig     *aws.Config
)

// RegisterAWSConfig registers aws.Config to be used by the dynamodb client.
//
// The following configurations do not apply even if they are set in aws.Config.
//   - HTTPClient
func RegisterAWSConfig(conf aws.Config) {
	awsConfigLock.Lock()
	defer awsConfigLock.Unlock()
	awsConfig = &conf
}

// DeregisterAWSConfig removes the registered aws.Config.
func DeregisterAWSConfig() {
	awsConfigLock.Lock()
	defer awsConfigLock.Unlock()
	awsConfig = nil
}

// mergeDynamoDBOptions merges the provided dynamodb.Options into the default dynamodb.Options.
func mergeDynamoDBOptions(opts dynamodb.Options) func(options *dynamodb.Options) {
	return func(defaultOpts *dynamodb.Options) {
		if defaultOpts.Credentials == nil {
			defaultOpts.Credentials = opts.Credentials
		}
		if defaultOpts.Region == "" {
			defaultOpts.Region = opts.Region
		}
		defaultOpts.HTTPClient = opts.HTTPClient
	}
}
