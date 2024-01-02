# godynamo

[![Go Report Card](https://goreportcard.com/badge/github.com/btnguyen2k/godynamo)](https://goreportcard.com/report/github.com/btnguyen2k/godynamo)
[![PkgGoDev](https://pkg.go.dev/badge/github.com/btnguyen2k/godynamo)](https://pkg.go.dev/github.com/btnguyen2k/godynamo)
[![Actions Status](https://github.com/btnguyen2k/godynamo/workflows/godynamo/badge.svg)](https://github.com/btnguyen2k/godynamo/actions)
[![codecov](https://codecov.io/gh/btnguyen2k/godynamo/branch/main/graph/badge.svg)](https://codecov.io/gh/btnguyen2k/godynamo)
[![Release](https://img.shields.io/github/release/btnguyen2k/godynamo.svg?style=flat-square)](RELEASE-NOTES.md)

Go driver for [AWS DynamoDB](https://aws.amazon.com/dynamodb/) which can be used with the standard [database/sql](https://golang.org/pkg/database/sql/) package.

## Usage

```go
package main

import (
	"database/sql"
	"fmt"

	_ "github.com/btnguyen2k/godynamo"
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
	dbRows, err := db.Query(`LIST TABLES`)
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

## Data Source Name (DSN) format for AWS DynamoDB

_Note: line-break is for readability only!_

```connection
Region=<aws-region>
;AkId=<aws-access-key-id>
;Secret_Key=<aws-secret-key>
[;Endpoint=<aws-dynamodb-endpoint>]
[TimeoutMs=<timeout-in-milliseconds>]
```

- `Region`: AWS region, for example `us-east-1`. If not supplied, the value of the environment `AWS_REGION` is used.
- `AkId`: AWS Access Key ID, for example `AKIA1234567890ABCDEF`. If not supplied, the value of the environment `AWS_ACCESS_KEY_ID` is used.
- `Secret_Key`: AWS Secret Key, for example `0A1B2C3D4E5F`. If not supplied, the value of the environment `AWS_SECRET_ACCESS_KEY` is used.
- `Endpoint`: (optional) AWS DynamoDB endpoint, for example `http://localhost:8000`; useful when AWS DynamoDB is running on local machine.
- `TimeoutMs`: (optional) timeout in milliseconds. If not specified, default value is `10000`.

## Supported statements:

- [Table](SQL_TABLE.md):
  - `CREATE TABLE`
  - `LIST TABLES`
  - `DESCRIBE TABLE`
  - `ALTER TABLE`
  - `DROP TABLE`

- [Index](SQL_INDEX.md):
  - `DESCRIBE LSI`
  - `CREATE GSI`
  - `DESCRIBE GSI`
  - `ALTER GSI`
  - `DROP GSI`

- [Document](SQL_DOCUMENT.md):
  - `INSERT`
  - `SELECT`
  - `UPDATE`
  - `DELETE`

## Transaction support

`godynamo` supports transactions that consist of write statements (e.g. `INSERT`, `UPDATE` and `DELETE`) since [v0.2.0](RELEASE-NOTES.md). Please note the following:

- Any limitation set by [DynamoDB/PartiQL](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/ql-reference.multiplestatements.transactions.html) will apply.
- [Table](SQL_TABLE.md) and [Index](SQL_INDEX.md) statements are not supported.
- `UPDATE`/`DELETE` with `RETURNING` and `SELECT` statements are not supported.

Example:
```go
tx, err := db.Begin()
if err != nil {
	panic(err)
}
defer tx.Rollback()
result1, _ := tx.Exec(`INSERT INTO "tbltest" VALUE {'app': ?, 'user': ?, 'active': ?}`, "app0", "user1", true)
result2, _ := tx.Exec(`INSERT INTO "tbltest" VALUE {'app': ?, 'user': ?, 'duration': ?}`, "app0", "user2", 1.23)
err = tx.Commit()
if err != nil {
	panic(err)
}
rowsAffected1, err1 := fmt.Println(result1.RowsAffected())
if err1 != nil {
	panic(err1)
}
fmt.Println("RowsAffected:", rowsAffected1) // output "RowsAffected: 1"

rowsAffected2, err2 := fmt.Println(result2.RowsAffected())
if err2 != nil {
	panic(err2)
}
fmt.Println("RowsAffected:", rowsAffected2) // output "RowsAffected: 1"
```

> If a statement's condition check fails (e.g. deleting non-existing item), the whole transaction will also fail. This behaviour is different from executing statements in non-transactional mode where failed condition check results in `0` affected row without error.
>
> You can use [EXISTS function](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/ql-functions.exists.html) for condition checking.

## Caveats

**Numerical values** are stored in DynamoDB as floating point numbers. Hence, numbers are always read back as `float64`. 
See [DynamoDB document](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/HowItWorks.NamingRulesDataTypes.html#HowItWorks.DataTypes) for details on DynamoDB's supported data types.

**A single query can only return up to [1MB of data](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Query.Pagination.html)**.
In the case of `SELECT` query, the driver automatically issues additional queries to fetch the remaining data if needed.
However, returned rows may not be in the expected order specified by `ORDER BY` clause. 
That means, rows returned from the query `SELECT * FROM table_name WHERE category='Laptop' ORDER BY id` may not be in
the expected order if all matched rows do not fit in 1MB of data.

## License

This project is licensed under the MIT License - see the [LICENSE.md](LICENSE.md) file for details.

## Support and Contribution

Feel free to create [pull requests](https://github.com/btnguyen2k/godynamo/pulls) or [issues](https://github.com/btnguyen2k/godynamo/issues) to report bugs or suggest new features.
Please search the existing issues before filing new issues to avoid duplicates. For new issues, file your bug or feature request as a new issue.

If you find this project useful, please start it.
