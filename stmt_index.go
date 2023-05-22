package godynamo

import (
	"context"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"io"
	"reflect"
	"sort"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
)

// RowsDescribeIndex captures the result from DESCRIBE LSI or DESCRIBE GSI operation.
type RowsDescribeIndex struct {
	count          int
	columnList     []string
	columnTypeList []reflect.Type
	indexInfo      map[string]interface{}
	cursorCount    int
}

// Columns implements driver.Rows.Columns.
func (r *RowsDescribeIndex) Columns() []string {
	return r.columnList
}

// Close implements driver.Rows.Close.
func (r *RowsDescribeIndex) Close() error {
	return nil
}

// Next implements driver.Rows.Next.
func (r *RowsDescribeIndex) Next(dest []driver.Value) error {
	if r.cursorCount >= r.count {
		return io.EOF
	}
	for i, colName := range r.columnList {
		dest[i] = r.indexInfo[colName]
	}
	r.cursorCount++
	return nil
}

// ColumnTypeScanType implements driver.RowsColumnTypeScanType.ColumnTypeScanType
func (r *RowsDescribeIndex) ColumnTypeScanType(index int) reflect.Type {
	return r.columnTypeList[index]
}

// ColumnTypeDatabaseTypeName implements driver.RowsColumnTypeDatabaseTypeName.ColumnTypeDatabaseTypeName
func (r *RowsDescribeIndex) ColumnTypeDatabaseTypeName(index int) string {
	return goTypeToDynamodbType(r.columnTypeList[index])
}

/*----------------------------------------------------------------------*/

// StmtDescribeLSI implements "DESCRIBE LSI" operation.
//
// Syntax:
//
//	DESCRIBE LSI <index-name> ON <table-name>
type StmtDescribeLSI struct {
	*Stmt
	tableName, indexName string
}

func (s *StmtDescribeLSI) validate() error {
	if s.tableName == "" {
		return errors.New("table name is missing")
	}
	if s.indexName == "" {
		return errors.New("index name is missing")
	}
	return nil
}

// Query implements driver.Stmt.Query.
func (s *StmtDescribeLSI) Query(_ []driver.Value) (driver.Rows, error) {
	input := &dynamodb.DescribeTableInput{
		TableName: &s.tableName,
	}
	output, err := s.conn.client.DescribeTable(context.Background(), input)
	result := &RowsDescribeIndex{count: 0}
	if err == nil {
		for _, lsi := range output.Table.LocalSecondaryIndexes {
			if lsi.IndexName != nil && *lsi.IndexName == s.indexName {
				result.count = 1
				js, _ := json.Marshal(lsi)
				json.Unmarshal(js, &result.indexInfo)
				for k := range result.indexInfo {
					result.columnList = append(result.columnList, k)
				}
				sort.Strings(result.columnList)
				result.columnTypeList = make([]reflect.Type, len(result.columnList))
				for i, col := range result.columnList {
					result.columnTypeList[i] = reflect.TypeOf(result.indexInfo[col])
				}
				break
			}
		}
	}
	return result, err
}

// Exec implements driver.Stmt.Exec.
// This function is not implemented, use Query instead.
func (s *StmtDescribeLSI) Exec(_ []driver.Value) (driver.Result, error) {
	return nil, errors.New("this operation is not supported, please use Query")
}
