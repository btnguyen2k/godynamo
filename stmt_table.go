package godynamo

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

/*----------------------------------------------------------------------*/

// StmtCreateTable implements "CREATE TABLE" operation.
//
// Syntax:
//
//	CREATE TABLE [IF NOT EXISTS] <table-name> <WITH PK=pk-name:pk-type>[, WITH SK=sk-name:sk-type][, WITH RCU=rcu][, WITH WCU=wcu]
//
//	- PK: partition key, format name:type (type is one of String, Number, Binary)
//	- SK: sort key, format name:type (type is one of String, Number, Binary)
//	- rcu: an integer specifying DynamoDB's read capacity, default value is 1.
//	- wcu: an integer specifying DynamoDB's write capacity, default value is 1.
//	- If "IF NOT EXISTS" is specified, Exec will silently swallow the error "409 Conflict".
type StmtCreateTable struct {
	*Stmt
	tableName      string
	ifNotExists    bool
	pkName, pkType string
	skName, skType string
	rcu, wcu       int64
	withOptsStr    string
}

func (s *StmtCreateTable) parse() error {
	if err := s.Stmt.parseWithOpts(s.withOptsStr); err != nil {
		return err
	}

	// partition key
	pkTokens := strings.SplitN(s.withOpts["PK"], ":", 2)
	s.pkName = strings.TrimSpace(pkTokens[0])
	if len(pkTokens) > 1 {
		s.pkType = strings.TrimSpace(strings.ToUpper(pkTokens[1]))
	}
	if s.pkName == "" {
		return fmt.Errorf("no PartitionKey, specify one using WITH pk=pkname:pktype")
	}
	if _, ok := dataTypes[s.pkType]; !ok {
		return fmt.Errorf("invalid type <%s> for PartitionKey, accepts values are BINARY, NUMBER and STRING", s.pkType)
	}

	// sort key
	skTokens := strings.SplitN(s.withOpts["SK"], ":", 2)
	s.skName = strings.TrimSpace(skTokens[0])
	if len(skTokens) > 1 {
		s.skType = strings.TrimSpace(strings.ToUpper(skTokens[1]))
	}
	if _, ok := dataTypes[s.skType]; !ok && s.skName != "" {
		return fmt.Errorf("invalid type SortKey <%s>, accepts values are BINARY, NUMBER and STRING", s.skType)
	}

	// RCU
	if _, ok := s.withOpts["RCU"]; ok {
		rcu, err := strconv.ParseInt(s.withOpts["RCU"], 10, 64)
		if err != nil || rcu <= 0 {
			return fmt.Errorf("invalid RCU value: %s", s.withOpts["RCU"])
		}
		s.rcu = rcu
	}
	// WCU
	if _, ok := s.withOpts["WCU"]; ok {
		wcu, err := strconv.ParseInt(s.withOpts["WCU"], 10, 64)
		if err != nil || wcu <= 0 {
			return fmt.Errorf("invalid WCU value: %s", s.withOpts["WCU"])
		}
		s.wcu = wcu
	}
	if s.rcu < 1 {
		s.rcu = 1
	}
	if s.wcu < 1 {
		s.wcu = 1
	}

	return nil
}

func (s *StmtCreateTable) validate() error {
	if s.tableName == "" {
		return errors.New("table name is missing")
	}
	return nil
}

// Query implements driver.Stmt.Query.
// This function is not implemented, use Exec instead.
func (s *StmtCreateTable) Query(_ []driver.Value) (driver.Rows, error) {
	return nil, errors.New("this operation is not supported, please use Exec")
}

// Exec implements driver.Stmt.Exec.
func (s *StmtCreateTable) Exec(_ []driver.Value) (driver.Result, error) {
	attrDefs := make([]types.AttributeDefinition, 0, 2)
	attrDefs = append(attrDefs, types.AttributeDefinition{AttributeName: &s.pkName, AttributeType: dataTypes[s.pkType]})
	if s.skName != "" {
		attrDefs = append(attrDefs, types.AttributeDefinition{AttributeName: &s.skName, AttributeType: dataTypes[s.skType]})
	}

	keySchema := make([]types.KeySchemaElement, 0, 2)
	keySchema = append(keySchema, types.KeySchemaElement{AttributeName: &s.pkName, KeyType: keyTypes["HASH"]})
	if s.skName != "" {
		keySchema = append(keySchema, types.KeySchemaElement{AttributeName: &s.skName, KeyType: keyTypes["RANGE"]})
	}

	input := &dynamodb.CreateTableInput{
		TableName:            &s.tableName,
		AttributeDefinitions: attrDefs,
		KeySchema:            keySchema,
		ProvisionedThroughput: &types.ProvisionedThroughput{
			ReadCapacityUnits:  &s.rcu,
			WriteCapacityUnits: &s.rcu,
		},
	}
	_, err := s.conn.client.CreateTable(context.Background(), input)
	result := &ResultCreateTable{Successful: err == nil}
	if s.ifNotExists && IsAwsError(err, "ResourceInUseException") {
		err = nil
	}
	return result, err
}

// ResultCreateTable captures the result from CREATE TABLE operation.
type ResultCreateTable struct {
	// Successful flags if the operation was successful or not.
	Successful bool
}

// LastInsertId implements driver.Result.LastInsertId.
func (r *ResultCreateTable) LastInsertId() (int64, error) {
	return 0, fmt.Errorf("this operation is not supported.")
}

// RowsAffected implements driver.Result.RowsAffected.
func (r *ResultCreateTable) RowsAffected() (int64, error) {
	if r.Successful {
		return 1, nil
	}
	return 0, nil
}

/*----------------------------------------------------------------------*/

// StmtListTables implements "LIST TABLES" operation.
//
// Syntax:
//
//	LIST TABLES|TABLE
type StmtListTables struct {
	*Stmt
}

func (s *StmtListTables) validate() error {
	return nil
}

// Exec implements driver.Stmt.Exec.
// This function is not implemented, use Query instead.
func (s *StmtListTables) Exec(_ []driver.Value) (driver.Result, error) {
	return nil, errors.New("this operation is not supported, please use Query")
}

// Query implements driver.Stmt.Query.
func (s *StmtListTables) Query(_ []driver.Value) (driver.Rows, error) {
	output, err := s.conn.client.ListTables(context.Background(), &dynamodb.ListTablesInput{})
	var rows driver.Rows
	if err == nil {
		rows = &RowsListTables{
			count:       len(output.TableNames),
			tables:      output.TableNames,
			cursorCount: 0,
		}
	}
	return rows, err
}

// RowsListTables captures the result from LIST TABLES operation.
type RowsListTables struct {
	count       int
	tables      []string
	cursorCount int
}

// Columns implements driver.Rows.Columns.
func (r *RowsListTables) Columns() []string {
	return []string{"$1"}
}

// Close implements driver.Rows.Close.
func (r *RowsListTables) Close() error {
	return nil
}

// Next implements driver.Rows.Next.
func (r *RowsListTables) Next(dest []driver.Value) error {
	if r.cursorCount >= r.count {
		return io.EOF
	}
	rowData := r.tables[r.cursorCount]
	r.cursorCount++
	dest[0] = rowData
	return nil
}
