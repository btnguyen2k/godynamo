module github.com/btnguyen2k/godynamo

go 1.18

require github.com/aws/aws-sdk-go-v2/service/dynamodb v1.20.1

require (
	github.com/aws/aws-sdk-go-v2 v1.19.0
	github.com/aws/aws-sdk-go-v2/credentials v1.13.27
	github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue v1.10.31
	github.com/aws/smithy-go v1.13.5
	github.com/btnguyen2k/consu/reddo v0.1.8
)

require (
	github.com/aws/aws-sdk-go-v2/internal/configsources v1.1.35 // indirect
	github.com/aws/aws-sdk-go-v2/internal/endpoints/v2 v2.4.29 // indirect
	github.com/aws/aws-sdk-go-v2/service/dynamodbstreams v1.14.15 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/accept-encoding v1.9.11 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/endpoint-discovery v1.7.29 // indirect
	github.com/jmespath/go-jmespath v0.4.0 // indirect
)
