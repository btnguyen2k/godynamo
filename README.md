# godynamo

[![Go Report Card](https://goreportcard.com/badge/github.com/btnguyen2k/godynamo)](https://goreportcard.com/report/github.com/btnguyen2k/godynamo)
[![PkgGoDev](https://pkg.go.dev/badge/github.com/btnguyen2k/godynamo)](https://pkg.go.dev/github.com/btnguyen2k/godynamo)
[![Actions Status](https://github.com/btnguyen2k/godynamo/workflows/godynamo/badge.svg)](https://github.com/btnguyen2k/godynamo/actions)
[![codecov](https://codecov.io/gh/btnguyen2k/godynamo/branch/main/graph/badge.svg?token=pYdHuxbIiI)](https://codecov.io/gh/btnguyen2k/godynamo)
[![Release](https://img.shields.io/github/release/btnguyen2k/godynamo.svg?style=flat-square)](RELEASE-NOTES.md)

Go driver for [AWS DynamoDB](https://aws.amazon.com/dynamodb/) which can be used with the standard [database/sql](https://golang.org/pkg/database/sql/) package.

## Usage

```go
package main

import (
	"database/sql"
	"fmt"

	_ "github.com/btnguyen2k/gocosmos"
)

func main() {
	driver := "godynamo"
	dsn := "Region=<aws-region>;AkId=<access-key-id>;SecretKey=<secret-key>"
	db, err := sql.Open(driver, dsn)
	if err != nil {
		panic(err)
	}
	defer db.Close()

	// db instance is ready to use
	dbrows, err := db.Query(`LIST TABLES`)
	if err != nil {
		panic(err)
	}
	for dbRows.Next() {
		var val interface{}
		err := dbRows.Scan(&val)
		if err != nil {
			panic(err)
		}
		fmt.Println(val)
	}
}
```

## Data Source Name (DSN) format for AWS Dynamo DB

// TODO

## Supported statements:

- Table:
  - `CREATE TABLE`
  - `LIST TABLES`
  - `ALTER TABLE`
  - `DROP TABLE`
  - `DESCRIBE TABLE`

- [Index](SQL_INDEX.md):
  - `DESCRIBE LSI`

### CREATE TABLE

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

### LIST TABLES

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

### ALTER TABLE

Syntax:
```sql
ALTER TABLE <table-name>
[WITH wcu=<number>[,] WITH rcu=<number>]
[[,] WITH CLASS=<table-class>]
```

Example:
```go
result, err := db.Exec(`ALTER TABLE demo WITH rcu=0 WITH wcu=0 WITH CLASS=STANDARD_IA`)
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
- Note: if `RCU` and `WRU` are both `0`, table will be created with `PAY_PER_REQUEST` billing mode; otherwise table will be creatd with `PROVISIONED` mode.
- Note: there must be _at least one space_ before the `WITH` keyword.

### DROP TABLE

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

### DESCRIBE TABLE

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
