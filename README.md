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

- [Table](SQL_TABLE.md):
  - `CREATE TABLE`
  - `LIST TABLES`
  - `ALTER TABLE`
  - `DROP TABLE`
  - `DESCRIBE TABLE`

- [Index](SQL_INDEX.md):
  - `DESCRIBE LSI`
  - `CREATE GSI`
  - `DESCRIBE GSI`
  - `ALTER GSI`
  - `DROP GSI`

- [Document](SQL_DOCUMENT.md)
  - `INSERT`
  - `SELECT`
  - `UPDATE`
  - `DELETE`

## License

MIT - See [LICENSE.md](LICENSE.md).
