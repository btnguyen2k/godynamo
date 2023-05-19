# godynamo

[![Go Report Card](https://goreportcard.com/badge/github.com/btnguyen2k/godynamo)](https://goreportcard.com/report/github.com/btnguyen2k/godynamo)
[![PkgGoDev](https://pkg.go.dev/badge/github.com/btnguyen2k/godynamo)](https://pkg.go.dev/github.com/btnguyen2k/godynamo)
[![Actions Status](https://github.com/btnguyen2k/godynamo/workflows/godynamo/badge.svg)](https://github.com/btnguyen2k/godynamo/actions)
[![codecov](https://codecov.io/gh/btnguyen2k/godynamo/branch/main/graph/badge.svg?token=pYdHuxbIiI)](https://codecov.io/gh/btnguyen2k/godynamo)
[![Release](https://img.shields.io/github/release/btnguyen2k/godynamo.svg?style=flat-square)](RELEASE-NOTES.md)

Go driver for [AWS DynamoDB](https://aws.amazon.com/dynamodb/) which can be used with the standard [database/sql](https://golang.org/pkg/database/sql/) package.

## Usage

```go
import (
	"database/sql"
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

## Supported statements:

- Tables:
  - `CREATE TABLE`
  - `DROP TABLE`
  - `LIST TABLES`

### CREATE TABLE

Syntax:
```sql
CREATE TABLE [IF NOT EXIST] <table-name>
WITH PK=<partition-key-name>:<data-type>
[, WITH SK=<sort-key-name>:<data-type>]
[, WITH wcu=<number>]
[, WITH rcu=<number>]
```

Example:
```go
result, err := db.Exec(`CREATE TABLE...`)
if err == nil {
    numAffectedRow, err := result.RowsAffected()
    ...
}
```

Description: create a DynamoDB table specified by `table-name`.

- If the statement is executed successfully, `RowsAffected()` returns `1, nil`.
- If the specified table already existed:
  - If `IF NOT EXISTS` is supplied: `RowsAffected()` returns `0, nil`
  - If `IF NOT EXISTS` is _not_ supplied: `RowsAffected()` returns `_, error`
- `RCU`: read capacity unit. If not specified or equal to 0, default value of 1 will be used.
- `WCU`: write capacity unit. If not specified or equal to 0, default value of 1 will be used.
- `PK`: partition key, mandatory.
- `SK`: sort key, optional.
- `data-type`: must be one of `BINARY`, `NUMBER` or `STRING`

Example:
```sql
CREATE TABLE demo WITH PK=id:string WITH rcu=3 WITH wcu=5
```

### DROP TABLE

Syntax:
```sql
DROP TABLE [IF EXIST] <table-name>
```

Alias: `DELETE TABLE`

Example:
```go
result, err := db.Exec(`DROP TABLE...`)
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

Example:
```sql
DROP TABLE IF EXISTS demo
```

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
