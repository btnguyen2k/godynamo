# godynamo - Supported statements for index

- `DESCRIBE LSI`
- `CREATE GSI`
- `DESCRIBE GSI`
- `ALTER GSI`
- `DROP GSI`

## DESCRIBE LSI

Syntax:
```sql
DESCRIBE LSI <index-name> ON <table-name>
```

Example:
```go
dbrows, err := db.Query(`DESCRIBE LSI idxos ON session`)
if err == nil {
	fetchAndPrintAllRows(dbrows)
}
```

Description: return info of a Local Secondary Index specified by `index-name` on a DynamoDB table specified by `table-name`.

Sample result:

| IndexArn                                                           | IndexName | IndexSizeBytes | ItemCount | KeySchema                                                                           | Projection                                                               |
|--------------------------------------------------------------------|-----------|----------------|-----------|-------------------------------------------------------------------------------------|--------------------------------------------------------------------------|
| "arn:aws:dynamodb:ddblocal:000000000000:table/session/index/idxos" | "idxos"   | 0              | 0         | [{"AttributeName":"app","KeyType":"HASH"},{"AttributeName":"os","KeyType":"RANGE"}] | {"NonKeyAttributes":["os_name","os_version"],"ProjectionType":"INCLUDE"} |

## CREATE GSI

Syntax:
```sql
CREATE GSI [IF NOT EXISTS] <index-name> ON <table-name>
<WITH PK=pk-attr-name:data-type>
[[,] WITH SK=sk-attr-name:data-type]
[[,] WITH wcu=<number>[,] WITH rcu=<number>]
[[,] WITH projection=*|attr1,attr2,attr3,...]
```

Example:
```go
result, err := db.Exec(`CREATE GSI idxname ON tablename WITH pk=grade:number, WITH rcu=1 WITH wru=2`)
if err == nil {
	numAffectedRow, err := result.RowsAffected()
	...
}
```

Description: create a Global Secondary Index on an existing DynamoDB table.

- If the statement is executed successfully, `RowsAffected()` returns `1, nil`.
- If the specified GSI already existed:
  - If `IF NOT EXISTS` is supplied: `RowsAffected()` returns `0, nil`.
  - If `IF NOT EXISTS` is _not_ supplied: `RowsAffected()` returns `_, error`.
- `RCU`: GSI's read capacity unit.
- `WCU`: GSI's write capacity unit.
- `PK`: GSI's partition key, mandatory.
- `SK`: GSI's sort key, optional.
- `data-type`: must be one of `BINARY`, `NUMBER` or `STRING`.
- `PROJECTION`:
  - `*`: all attributes from the original table are included in projection (`ProjectionType=ALL`).
  - `attr1,attr2,...`: specified attributes from the original table are included in projection (`ProjectionType=INCLUDE`).
  - _not specified_: only key attributes are included in projection (`ProjectionType=KEYS_ONLY`).
- Note: The provisioned throughput settings of a GSI are separate from those of its base table.
- Note: GSI inherit the RCU and WCU mode from the base table. That means if the base table is in on-demand mode, then DynamoDB also creates the GSI in on-demand mode. 
- Note: there must be at least one space before the WITH keyword.

## DESCRIBE GSI

Syntax:
```sql
DESCRIBE GSI <index-name> ON <table-name>
```

Example:
```go
dbrows, err := db.Query(`DESCRIBE GSI idxos ON session`)
if err == nil {
	fetchAndPrintAllRows(dbrows)
}
```

Description: return info of a Local Secondary Index specified by `index-name` on a DynamoDB table specified by `table-name`.

Sample result:

| Backfilling | IndexArn                                                                | IndexName    | IndexSizeBytes | IndexStatus | ItemCount | KeySchema                                      | Projection                                       | ProvisionedThroughput                                                                                                                |
|-------------|-------------------------------------------------------------------------|--------------|----------------|-------------|-----------|------------------------------------------------|--------------------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------|
| null        | "arn:aws:dynamodb:ddblocal:000000000000:table/session/index/idxbrowser" | "idxbrowser" | 0              | "ACTIVE"    | 0         | [{"AttributeName":"browser","KeyType":"HASH"}] | {"NonKeyAttributes":null,"ProjectionType":"ALL"} | {"LastDecreaseDateTime":null,"LastIncreaseDateTime":null,"NumberOfDecreasesToday":null,"ReadCapacityUnits":1,"WriteCapacityUnits":1} |

## ALTER GSI

Syntax:
```sql
ALTER GSI <index-name> ON <table-name>
WITH wcu=<number>[,] WITH rcu=<number>
```

Example:
```go
result, err := db.Exec(`ALTER GSI idxname ON tablename WITH rcu=1 WITH wru=2`)
if err == nil {
	numAffectedRow, err := result.RowsAffected()
	...
}
```

Description: update WRU/RCU of a Global Secondary Index on an existing DynamoDB table.

- If the statement is executed successfully, `RowsAffected()` returns `1, nil`.
- `RCU`: GSI's read capacity unit.
- `WCU`: GSI's write capacity unit.
- Note: The provisioned throughput settings of a GSI are separate from those of its base table.
- Note: GSI inherit the RCU and WCU mode from the base table. That means if the base table is in on-demand mode, then DynamoDB also creates the GSI in on-demand mode. 
- Note: there must be at least one space before the WITH keyword.

## DROP GSI

Syntax:
```sql
DROP GSI [IF EXIST] <index-name> ON <table-name>
```

Alias: `DELETE GSI`

Example:
```go
result, err := db.Exec(`DROP GSI IF EXISTS index ON table`)
if err == nil {
	numAffectedRow, err := result.RowsAffected()
	...
}
```

Description: delete an existing GSI from a DynamoDB table.

- If the statement is executed successfully, `RowsAffected()` returns `1, nil`.
- If the specified table does not exist:
  - If `IF EXISTS` is supplied: `RowsAffected()` returns `0, nil`
  - If `IF EXISTS` is _not_ supplied: `RowsAffected()` returns `_, error`
