# godynamo - Supported statements for table

- `CREATE TABLE`
- `LIST TABLES`
- `ALTER TABLE`
- `DROP TABLE`
- `DESCRIBE TABLE`

## CREATE TABLE

Syntax:
```sql
CREATE TABLE [IF NOT EXIST] <table-name>
WITH PK=<partition-key-name>:<data-type>
[[,] WITH SK=<sort-key-name>:<data-type>]
[[,] WITH wcu=<number>[,] WITH rcu=<number>]
[[,] WITH LSI=index-name1:attr-name1:data-type]
[[,] WITH LSI=index-name2:attr-name2:data-type:*]
[[,] WITH LSI=index-name2:attr-name2:data-type:nonKeyAttr1,nonKeyAttr2,nonKeyAttr3,...]
[[,] WITH LSI...]
[[,] WITH CLASS=<table-class>]
```

Example:
```go
result, err := db.Exec(`CREATE TABLE demo WITH PK=id:string WITH rcu=3 WITH wcu=5`)
if err == nil {
	numAffectedRow, err := result.RowsAffected()
	...
}
```

Description: create a DynamoDB table specified by `table-name`.

- If the statement is executed successfully, `RowsAffected()` returns `1, nil`.
- If the specified table already existed:
  - If `IF NOT EXISTS` is supplied: `RowsAffected()` returns `0, nil`.
  - If `IF NOT EXISTS` is _not_ supplied: `RowsAffected()` returns `_, error`.
- `RCU`: read capacity unit.
- `WCU`: write capacity unit.
- `PK`: partition key, mandatory.
- `SK`: sort key, optional.
- `LSI`: local secondary index, format `index-name:attr-name:data-type[:projectionAttrs]`
  - `projectionAttrs=*`: all attributes from the original table are included in projection (`ProjectionType=ALL`).
  - `projectionAttrs=attr1,attr2,...`: specified attributes from the original table are included in projection (`ProjectionType=INCLUDE`).
  - _projectionAttrs is not specified_: only key attributes are included in projection (`ProjectionType=KEYS_ONLY`).
- `data-type`: must be one of `BINARY`, `NUMBER` or `STRING`.
- `table-class` is either `STANDARD` (default) or `STANDARD_IA`.
- Note: if `RCU` and `WRU` are both `0` or not specified, table will be created with `PAY_PER_REQUEST` billing mode; otherwise table will be creatd with `PROVISIONED` mode.
- Note: there must be _at least one space_ before the `WITH` keyword.

## LIST TABLES

Syntax:
```sql
LIST TABLES
```

Example:
```go
result, err := db.Query(`LIST TABLES`)
if err == nil {
	...
}
```

Description: return list of all DynamoDB tables.

Sample result:
|$1|
|--------|
|tbltest0|
|tbltest1|
|tbltest2|
|tbltest3|

## ALTER TABLE

Syntax:
```sql
ALTER TABLE <table-name>
[WITH wcu=<number>[,] WITH rcu=<number>]
[[,] WITH CLASS=<table-class>]
```

Example:
```go
result, err := db.Exec(`ALTER TABLE demo WITH rcu=5 WITH wcu=7`)
if err == nil {
	numAffectedRow, err := result.RowsAffected()
	...
}
```

Description: update WCU/RCU or table-class of an existing DynamoDB table specified by `table-name`.

- If the statement is executed successfully, `RowsAffected()` returns `1, nil`.
- `RCU`: read capacity unit.
- `WCU`: write capacity unit.
- `table-class` is either `STANDARD` (default) or `STANDARD_IA`.
- Note: if `RCU` and `WRU` are both `0`, table's billing mode will be updated to `PAY_PER_REQUEST`; otherwise billing mode will be updated to `PROVISIONED`.
- Note: there must be _at least one space_ before the `WITH` keyword.

## DROP TABLE

Syntax:
```sql
DROP TABLE [IF EXIST] <table-name>
```

Alias: `DELETE TABLE`

Example:
```go
result, err := db.Exec(`DROP TABLE IF EXISTS demo`)
if err == nil {
	numAffectedRow, err := result.RowsAffected()
	...
}
```

Description: delete an existing DynamoDB table specified by `table-name`.

- If the statement is executed successfully, `RowsAffected()` returns `1, nil`.
- If the specified table does not exist:
  - If `IF EXISTS` is supplied: `RowsAffected()` returns `0, nil`
  - If `IF EXISTS` is _not_ supplied: `RowsAffected()` returns `_, error`

## DESCRIBE TABLE

Syntax:
```sql
DESCRIBE TABLE <table-name>
```

Example:
```go
result, err := db.Query(`DESCRIBE TABLE demo`)
if err == nil {
	...
}
```

Description: return info of a DynamoDB table specified by `table-name`.

Sample result:
|ArchivalSummary|AttributeDefinitions|BillingModeSummary|CreationDateTime|DeletionProtectionEnabled|GlobalSecondaryIndexes|GlobalTableVersion|ItemCount|KeySchema|LatestStreamArn|LatestStreamLabel|LocalSecondaryIndexes|ProvisionedThroughput|Replicas|RestoreSummary|SSEDescription|StreamSpecification|TableArn|TableClassSummary|TableId|TableName|TableSizeBytes|TableStatus|
|---------------|--------------------|------------------|----------------|-------------------------|----------------------|------------------|---------|---------|---------------|-----------------|---------------------|---------------------|--------|--------------|--------------|-------------------|--------|-----------------|-------|---------|--------------|-----------|
|null|[{"AttributeName":"app","AttributeType":"S"},{"AttributeName":"user","AttributeType":"S"},{"AttributeName":"timestamp","AttributeType":"N"},{"AttributeName":"browser","AttributeType":"S"},{"AttributeName":"os","AttributeType":"S"}]|{"BillingMode":"PAY_PER_REQUEST","LastUpdateToPayPerRequestDateTime":"2023-05-23T01:58:27.352Z"}|"2023-05-23T01:58:27.352Z"|null|null|null|0|[{"AttributeName":"app","KeyType":"HASH"},{"AttributeName":"user","KeyType":"RANGE"}]|null|null|[{"IndexArn":"arn:aws:dynamodb:ddblocal:000000000000:table/tbltemp/index/idxos","IndexName":"idxos","IndexSizeBytes":0,"ItemCount":0,"KeySchema":[{"AttributeName":"app","KeyType":"HASH"},{"AttributeName":"os","KeyType":"RANGE"}],"Projection":{"NonKeyAttributes":["os_name","os_version"],"ProjectionType":"INCLUDE"}},{"IndexArn":"arn:aws:dynamodb:ddblocal:000000000000:table/tbltemp/index/idxbrowser","IndexName":"idxbrowser","IndexSizeBytes":0,"ItemCount":0,"KeySchema":[{"AttributeName":"app","KeyType":"HASH"},{"AttributeName":"browser","KeyType":"RANGE"}],"Projection":{"NonKeyAttributes":null,"ProjectionType":"ALL"}},{"IndexArn":"arn:aws:dynamodb:ddblocal:000000000000:table/tbltemp/index/idxtime","IndexName":"idxtime","IndexSizeBytes":0,"ItemCount":0,"KeySchema":[{"AttributeName":"app","KeyType":"HASH"},{"AttributeName":"timestamp","KeyType":"RANGE"}],"Projection":{"NonKeyAttributes":null,"ProjectionType":"KEYS_ONLY"}}]|{"LastDecreaseDateTime":"1970-01-01T00:00:00Z","LastIncreaseDateTime":"1970-01-01T00:00:00Z","NumberOfDecreasesToday":0,"ReadCapacityUnits":0,"WriteCapacityUnits":0}|null|null|null|null|"arn:aws:dynamodb:ddblocal:000000000000:table/tbltemp"|null|null|"tbltemp"|0|"ACTIVE"|
