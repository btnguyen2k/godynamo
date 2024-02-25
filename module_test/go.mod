module godynamo_test

go 1.18

replace github.com/btnguyen2k/godynamo => ../

require (
	github.com/aws/aws-sdk-go-v2 v1.25.0
	github.com/aws/aws-sdk-go-v2/service/dynamodb v1.29.0
	github.com/aws/smithy-go v1.20.1
	github.com/btnguyen2k/consu/reddo v0.1.8
	github.com/btnguyen2k/consu/semita v0.1.5
	github.com/btnguyen2k/godynamo v0.0.0-00010101000000-000000000000
)

require (
	github.com/aws/aws-sdk-go-v2/credentials v1.16.14 // indirect
	github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue v1.12.14 // indirect
	github.com/aws/aws-sdk-go-v2/internal/configsources v1.3.0 // indirect
	github.com/aws/aws-sdk-go-v2/internal/endpoints/v2 v2.6.0 // indirect
	github.com/aws/aws-sdk-go-v2/service/dynamodbstreams v1.18.7 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/accept-encoding v1.11.0 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/endpoint-discovery v1.9.0 // indirect
	github.com/btnguyen2k/consu/g18 v0.0.2 // indirect
	github.com/jmespath/go-jmespath v0.4.0 // indirect
)
