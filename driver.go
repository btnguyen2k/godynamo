package godynamo

import (
	"database/sql"
	"database/sql/driver"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
)

// init is automatically invoked when the driver is imported
func init() {
	sql.Register("godynamo", &Driver{})
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
	region := params["REGION"]
	if region == "" {
		region = os.Getenv("AWS_REGION")
	}
	akid := params["AKID"]
	if akid == "" {
		akid = os.Getenv("AWS_ACCESS_KEY_ID")
	}
	secretKey := params["SECRET_KEY"]
	if secretKey == "" {
		secretKey = params["SECRETKEY"]
		if secretKey == "" {
			secretKey = os.Getenv("AWS_SECRET_ACCESS_KEY")
		}
	}
	opts := dynamodb.Options{
		Credentials: credentials.NewStaticCredentialsProvider(akid, secretKey, ""),
		HTTPClient:  http.NewBuildableClient().WithTimeout(time.Millisecond * time.Duration(timeoutMs)),
		Region:      region,
	}
	endpoint := params["ENDPOINT"]
	if endpoint == "" {
		endpoint = os.Getenv("AWS_DYNAMODB_ENDPOINT")
	}
	if endpoint != "" {
		opts.EndpointResolver = dynamodb.EndpointResolverFromURL(endpoint)
		if strings.HasPrefix(endpoint, "http://") {
			opts.EndpointOptions.DisableHTTPS = true
		}
	}
	client := dynamodb.New(opts)
	return &Conn{client: client}, nil
}
