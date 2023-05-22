# godynamo - Supported statements for index

- `DESCRIBE LSI`

## DESCRIBE LSI

Syntax:
```sql
DESCRIBE LSI <index-name> ON <table-name>
```

Example:
```go
result, err := db.Query(`DESCRIBE LSI idxos ON session`)
if err == nil {
	...
}
```

Description: return info of a Local Secondary Index specified by `index-name` on a DynamoDB table specified by `table-name`.

Sample result:
|IndexArn|IndexName|IndexSizeBytes|ItemCount|KeySchema|Projection|
|--------|---------|--------------|---------|---------|----------|
|"arn:aws:dynamodb:ddblocal:000000000000:table/session/index/idxos"|"idxos"|0|0|[{"AttributeName":"app","KeyType":"HASH"},{"AttributeName":"os","KeyType":"RANGE"}]|{"NonKeyAttributes":["os_name","os_version"],"ProjectionType":"INCLUDE"}|
