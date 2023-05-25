package godynamo

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"reflect"
	"regexp"
	"sort"

	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

var rePlaceholder = regexp.MustCompile(`(?m)\?\s*[\,\]\}\s]`)

/*----------------------------------------------------------------------*/

// StmtInsert implements "INSERT" statement.
//
// Syntax: follow "PartiQL insert statements for DynamoDB" https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/ql-reference.insert.html
type StmtInsert struct {
	*Stmt
}

func (s *StmtInsert) parse() error {
	matches := rePlaceholder.FindAllString(s.query, -1)
	s.numInput = len(matches)
	return nil
}

func (s *StmtInsert) validate() error {
	return nil
}

// Query implements driver.Stmt.Query.
// This function is not implemented, use Exec instead.
func (s *StmtInsert) Query(_ []driver.Value) (driver.Rows, error) {
	return nil, errors.New("this operation is not supported, please use Exec")
}

// Exec implements driver.Stmt.Exec.
func (s *StmtInsert) Exec(values []driver.Value) (driver.Result, error) {
	params := make([]types.AttributeValue, len(values))
	var err error
	for i, v := range values {
		params[i], err = ToAttributeValue(v)
		if err != nil {
			return &ResultInsert{Successful: false}, fmt.Errorf("error marshalling parameter %d-th: %s", i+1, err)
		}
	}
	input := &dynamodb.ExecuteStatementInput{
		Statement:              &s.query,
		ReturnConsumedCapacity: types.ReturnConsumedCapacityTotal,
		Parameters:             params,
	}
	_, err = s.conn.client.ExecuteStatement(context.Background(), input)
	result := &ResultInsert{Successful: err == nil}
	return result, err
}

// ResultInsert captures the result from INSERT statement.
type ResultInsert struct {
	// Successful flags if the operation was successful or not.
	Successful bool
}

// LastInsertId implements driver.Result.LastInsertId.
func (r *ResultInsert) LastInsertId() (int64, error) {
	return 0, fmt.Errorf("this operation is not supported.")
}

// RowsAffected implements driver.Result.RowsAffected.
func (r *ResultInsert) RowsAffected() (int64, error) {
	if r.Successful {
		return 1, nil
	}
	return 0, nil
}

/*----------------------------------------------------------------------*/

// StmtSelect implements "SELECT" statement.
//
// Syntax: follow "PartiQL select statements for DynamoDB" https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/ql-reference.select.html
type StmtSelect struct {
	*Stmt
}

func (s *StmtSelect) parse() error {
	matches := rePlaceholder.FindAllString(s.query+" ", -1)
	s.numInput = len(matches)
	return nil
}

func (s *StmtSelect) validate() error {
	return nil
}

// Query implements driver.Stmt.Query.
func (s *StmtSelect) Query(values []driver.Value) (driver.Rows, error) {
	params := make([]types.AttributeValue, len(values))
	var err error
	for i, v := range values {
		params[i], err = ToAttributeValue(v)
		if err != nil {
			return &ResultSelect{}, fmt.Errorf("error marshalling parameter %d-th: %s", i+1, err)
		}
	}
	// fmt.Printf("DEBUG: %T - %#v\n", values[0], values[0])
	// fmt.Printf("DEBUG: %T - %#v\n", params[0], params[0])
	input := &dynamodb.ExecuteStatementInput{
		Statement:              &s.query,
		ReturnConsumedCapacity: types.ReturnConsumedCapacityTotal,
		Parameters:             params,
	}
	dbResult, err := s.conn.client.ExecuteStatement(context.Background(), input)
	result := &ResultSelect{dbResult: dbResult, columnTypes: make(map[string]reflect.Type)}
	if err == nil {
		result.count = len(dbResult.Items)
		colMap := make(map[string]bool)
		for _, item := range dbResult.Items {
			for col, av := range item {
				colMap[col] = true
				if result.columnTypes[col] == nil {
					var value interface{}
					attributevalue.Unmarshal(av, &value)
					result.columnTypes[col] = reflect.TypeOf(value)
				}
			}
		}
		result.columnList = make([]string, 0, len(colMap))
		for col := range colMap {
			result.columnList = append(result.columnList, col)
		}
		sort.Strings(result.columnList)
	}
	return result, err
}

// Exec implements driver.Stmt.Exec.
// This function is not implemented, use Query instead.
func (s *StmtSelect) Exec(_ []driver.Value) (driver.Result, error) {
	return nil, errors.New("this operation is not supported, please use Query")
}

// ResultSelect captures the result from SELECT statement.
type ResultSelect struct {
	count       int
	dbResult    *dynamodb.ExecuteStatementOutput
	cursorCount int
	columnList  []string
	columnTypes map[string]reflect.Type
}

// Columns implements driver.Rows.Columns.
func (r *ResultSelect) Columns() []string {
	return r.columnList
}

// ColumnTypeScanType implements driver.RowsColumnTypeScanType.ColumnTypeScanType
func (r *ResultSelect) ColumnTypeScanType(index int) reflect.Type {
	return r.columnTypes[r.columnList[index]]
}

// ColumnTypeDatabaseTypeName implements driver.RowsColumnTypeDatabaseTypeName.ColumnTypeDatabaseTypeName
func (r *ResultSelect) ColumnTypeDatabaseTypeName(index int) string {
	return goTypeToDynamodbType(r.columnTypes[r.columnList[index]])
}

// Close implements driver.Rows.Close.
func (r *ResultSelect) Close() error {
	return nil
}

// Next implements driver.Rows.Next.
func (r *ResultSelect) Next(dest []driver.Value) error {
	if r.cursorCount >= r.count {
		return io.EOF
	}
	rowData := r.dbResult.Items[r.cursorCount]
	r.cursorCount++
	for i, colName := range r.columnList {
		var value interface{}
		attributevalue.Unmarshal(rowData[colName], &value)
		dest[i] = value
	}
	return nil
}
