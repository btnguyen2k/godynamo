module godynamo_test

go 1.18

replace github.com/btnguyen2k/godynamo => ../

require (
	github.com/aws/aws-sdk-go-v2 v1.24.0
	github.com/aws/aws-sdk-go-v2/service/dynamodb v1.26.7
	github.com/btnguyen2k/consu/reddo v0.1.8
	github.com/btnguyen2k/consu/semita v0.1.5
)

require github.com/aws/smithy-go v1.19.0 // indirect
