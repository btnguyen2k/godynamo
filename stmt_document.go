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

// StmtExecutable is the base implementation for INSERT, SELECT, UPDATE and DELETE statements.
type StmtExecutable struct {
	*Stmt
}

func (s *StmtExecutable) parse() error {
	matches := rePlaceholder.FindAllString(s.query+" ", -1)
	s.numInput = len(matches)
	return nil
}

func (s *StmtExecutable) validate() error {
	return nil
}

func (s *StmtExecutable) Execute(values []driver.Value) (*dynamodb.ExecuteStatementOutput, error) {
	params := make([]types.AttributeValue, len(values))
	var err error
	for i, v := range values {
		params[i], err = ToAttributeValue(v)
		if err != nil {
			return nil, fmt.Errorf("error marshalling parameter %d-th: %s", i+1, err)
		}
	}
	input := &dynamodb.ExecuteStatementInput{
		Statement:              &s.query,
		ReturnConsumedCapacity: types.ReturnConsumedCapacityTotal,
		Parameters:             params,
	}
	return s.conn.client.ExecuteStatement(context.Background(), input)
}

// ResultNoResultSet captures the result from statements that do not expect a ResultSet to be returned.
type ResultNoResultSet struct {
	// Successful flags if the operation was successful or not.
	Successful   bool
	AffectedRows int64
}

// LastInsertId implements driver.Result.LastInsertId.
func (r *ResultNoResultSet) LastInsertId() (int64, error) {
	return 0, fmt.Errorf("this operation is not supported.")
}

// RowsAffected implements driver.Result.RowsAffected.
func (r *ResultNoResultSet) RowsAffected() (int64, error) {
	return r.AffectedRows, nil
}

// ResultResultSet captures the result from statements that expect a ResultSet to be returned.
type ResultResultSet struct {
	count       int
	dbResult    *dynamodb.ExecuteStatementOutput
	cursorCount int
	columnList  []string
	columnTypes map[string]reflect.Type
}

func (r *ResultResultSet) init() *ResultResultSet {
	if r.dbResult == nil {
		return r
	}
	if r.columnTypes == nil {
		r.columnTypes = make(map[string]reflect.Type)
	}
	r.count = len(r.dbResult.Items)
	colMap := make(map[string]bool)
	for _, item := range r.dbResult.Items {
		for col, av := range item {
			colMap[col] = true
			if r.columnTypes[col] == nil {
				var value interface{}
				attributevalue.Unmarshal(av, &value)
				r.columnTypes[col] = reflect.TypeOf(value)
			}
		}
	}
	r.columnList = make([]string, 0, len(colMap))
	for col := range colMap {
		r.columnList = append(r.columnList, col)
	}
	sort.Strings(r.columnList)

	return r
}

// Columns implements driver.Rows.Columns.
func (r *ResultResultSet) Columns() []string {
	return r.columnList
}

// ColumnTypeScanType implements driver.RowsColumnTypeScanType.ColumnTypeScanType
func (r *ResultResultSet) ColumnTypeScanType(index int) reflect.Type {
	return r.columnTypes[r.columnList[index]]
}

// ColumnTypeDatabaseTypeName implements driver.RowsColumnTypeDatabaseTypeName.ColumnTypeDatabaseTypeName
func (r *ResultResultSet) ColumnTypeDatabaseTypeName(index int) string {
	return goTypeToDynamodbType(r.columnTypes[r.columnList[index]])
}

// Close implements driver.Rows.Close.
func (r *ResultResultSet) Close() error {
	return nil
}

// Next implements driver.Rows.Next.
func (r *ResultResultSet) Next(dest []driver.Value) error {
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

/*----------------------------------------------------------------------*/

// StmtInsert implements "INSERT" statement.
//
// Syntax: follow "PartiQL insert statements for DynamoDB" https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/ql-reference.insert.html
type StmtInsert struct {
	*StmtExecutable
}

// Query implements driver.Stmt.Query.
// This function is not implemented, use Exec instead.
func (s *StmtInsert) Query(_ []driver.Value) (driver.Rows, error) {
	return nil, errors.New("this operation is not supported, please use Exec")
}

// Exec implements driver.Stmt.Exec.
func (s *StmtInsert) Exec(values []driver.Value) (driver.Result, error) {
	_, err := s.Execute(values)
	result := &ResultNoResultSet{Successful: err == nil}
	return result, err
}

/*----------------------------------------------------------------------*/

// StmtSelect implements "SELECT" statement.
//
// Syntax: follow "PartiQL select statements for DynamoDB" https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/ql-reference.select.html
type StmtSelect struct {
	*StmtExecutable
}

// Query implements driver.Stmt.Query.
func (s *StmtSelect) Query(values []driver.Value) (driver.Rows, error) {
	output, err := s.Execute(values)
	result := &ResultResultSet{dbResult: output}
	if err == nil {
		result.init()
	}
	return result, err
}

// Exec implements driver.Stmt.Exec.
// This function is not implemented, use Query instead.
func (s *StmtSelect) Exec(_ []driver.Value) (driver.Result, error) {
	return nil, errors.New("this operation is not supported, please use Query")
}

/*----------------------------------------------------------------------*/

// StmtUpdate implements "UPDATE" statement.
//
// Syntax: follow "PartiQL update statements for DynamoDB" https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/ql-reference.update.html
type StmtUpdate struct {
	*StmtExecutable
}

// Query implements driver.Stmt.Query.
func (s *StmtUpdate) Query(values []driver.Value) (driver.Rows, error) {
	output, err := s.Execute(values)
	result := &ResultResultSet{dbResult: output, columnTypes: make(map[string]reflect.Type)}
	if err == nil {
		result.init()
	}
	return result, err
}

// Exec implements driver.Stmt.Exec.
func (s *StmtUpdate) Exec(values []driver.Value) (driver.Result, error) {
	_, err := s.Execute(values)
	result := &ResultNoResultSet{Successful: err == nil}
	if err != nil {
		result.AffectedRows = 0
	} else {
		result.AffectedRows = 1
	}
	return result, err
}

/*----------------------------------------------------------------------*/

// StmtDelete implements "DELETE" statement.
//
// Syntax: follow "PartiQL delete statements for DynamoDB" https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/ql-reference.delete.html
//
// Note: StmtDelete returns the deleted item by appending "RETURNING RETURNING ALL OLD *" to the statement.
type StmtDelete struct {
	*StmtExecutable
}

var (
	reReturning = regexp.MustCompile(`(?im)\s+RETURNING\s+((ALL\s+OLD)|(MODIFIED\s+OLD)|(ALL\s+NEW)|(MODIFIED\s+NEW))\s+\*\s*$`)
)

func (s *StmtDelete) parse() error {
	if !reReturning.MatchString(s.query) {
		s.query += " RETURNING ALL OLD *"
	}
	return s.StmtExecutable.parse()
}

// Query implements driver.Stmt.Query.
func (s *StmtDelete) Query(values []driver.Value) (driver.Rows, error) {
	output, err := s.Execute(values)
	result := &ResultResultSet{dbResult: output, columnTypes: make(map[string]reflect.Type)}
	if err == nil {
		result.init()
	}
	return result, err
}

// Exec implements driver.Stmt.Exec.
func (s *StmtDelete) Exec(values []driver.Value) (driver.Result, error) {
	output, err := s.Execute(values)
	if IsAwsError(err, "ConditionalCheckFailedException") {
		return &ResultNoResultSet{Successful: true, AffectedRows: 0}, nil
	}
	if err != nil {
		return &ResultNoResultSet{Successful: false, AffectedRows: 0}, err
	}
	return &ResultNoResultSet{Successful: true, AffectedRows: int64(len(output.Items))}, nil
}
