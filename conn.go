package godynamo

import (
	"database/sql/driver"
	"errors"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
)

// Conn is AWS DynamoDB connection handler.
type Conn struct {
	client *dynamodb.Client //AWS DynamoDB client
}

// Close implements driver.Conn.Close.
func (c *Conn) Close() error {
	return nil
}

// Begin implements driver.Conn.Begin.
func (c *Conn) Begin() (driver.Tx, error) {
	return nil, errors.New("transaction is not supported")
}

// // CheckNamedValue implements driver.NamedValueChecker.CheckNamedValue.
// func (c *Conn) CheckNamedValue(value *driver.NamedValue) error {
// 	// since DynamoDB is document db, it accepts any value types
// 	return nil
// }

// Prepare implements driver.Conn.Prepare.
func (c *Conn) Prepare(query string) (driver.Stmt, error) {
	return parseQuery(c, query)
}
