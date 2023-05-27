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

`Region=<aws-region>;AkId=<aws-access-key-id>;Secret_Key=<aws-secret-key>[;Endpoint=<aws-dynamodb-endpoint>][TimeoutMs=<timeout-in-milliseconds>]`

- `Region`: AWS region, for example `us-east-1`. If not supplied, the value of the environment `AWS_REGION` is used.
- `AkId`: AWS Access Key ID, for example `AKIA1234567890ABCDEF`. If not supplied, the value of the environment `AWS_ACCESS_KEY_ID` is used.
- `Secret_Key`: AWS Secret Key, for example `0***F`. If not supplied, the value of the environment `AWS_SECRET_ACCESS_KEY` is used.
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

## License

MIT - See [LICENSE.md](LICENSE.md).
