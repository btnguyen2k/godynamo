package godynamo

import (
	"context"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"reflect"
	"sort"
	"strconv"
	"strings"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/btnguyen2k/consu/reddo"
)

type lsiDef struct {
	indexName, fieldName, fieldType string
	projectedFields                 string
}

/*----------------------------------------------------------------------*/

// StmtCreateTable implements "CREATE TABLE" operation.
//
// Syntax:
//
//		CREATE TABLE [IF NOT EXISTS] <table-name>
//		<WITH PK=pk-attr-name:data-type>
//		[[,] WITH SK=sk-attr-name:data-type]
//		[[,] WITH wcu=<number>[,] WITH rcu=<number>]
//		[[,] WITH LSI=index-name1:attr-name1:data-type]
//		[[,] WITH LSI=index-name2:attr-name2:data-type:*]
//		[[,] WITH LSI=index-name2:attr-name2:data-type:nonKeyAttr1,nonKeyAttr2,nonKeyAttr3,...]
//		[[,] WITH LSI...]
//		[[,] WITH CLASS=<table-class>]
//
//	- PK: partition key, format name:type (type is one of String, Number, Binary).
//	- SK: sort key, format name:type (type is one of String, Number, Binary).
//	- LSI: local secondary index, format index-name:attr-name:type[:projectionAttrs], where:
//		- type is one of String, Number, Binary.
//		- projectionAttrs=*: all attributes from the original table are included in projection (ProjectionType=ALL).
//		- projectionAttrs=attr1,attr2,...: specified attributes from the original table are included in projection (ProjectionType=INCLUDE).
//		- projectionAttrs is not specified: only key attributes are included in projection (ProjectionType=KEYS_ONLY).
//	- RCU: an integer specifying DynamoDB's read capacity.
//	- WCU: an integer specifying DynamoDB's write capacity.
//	- CLASS: table class, either STANDARD (default) or STANDARD_IA.
//	- If "IF NOT EXISTS" is specified, Exec will silently swallow the error "ResourceInUseException".
//	- Note: if RCU and WRU are both 0 or not specified, table will be created with PAY_PER_REQUEST billing mode; otherwise table will be creatd with PROVISIONED mode.
//	- Note: there must be at least one space before the WITH keyword.
type StmtCreateTable struct {
	*Stmt
	tableName      string
	ifNotExists    bool
	pkName, pkType string
	tableClass     *string
	skName, skType *string
	rcu, wcu       *int64
	lsi            []lsiDef
	withOptsStr    string
}

func (s *StmtCreateTable) parse() error {
	if err := s.Stmt.parseWithOpts(s.withOptsStr); err != nil {
		return err
	}

	// partition key
	pkTokens := strings.SplitN(s.withOpts["PK"].FirstString(), ":", 2)
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
	skTokens := strings.SplitN(s.withOpts["SK"].FirstString(), ":", 2)
	skName := strings.TrimSpace(skTokens[0])
	if skName != "" {
		s.skName = &skName
		skType := ""
		if len(skTokens) > 1 {
			skType = strings.TrimSpace(strings.ToUpper(skTokens[1]))
		}
		if _, ok := dataTypes[skType]; !ok {
			return fmt.Errorf("invalid type SortKey <%s>, accepts values are BINARY, NUMBER and STRING", skType)
		}
		s.skType = &skType
	}

	// local secondary index
	for _, lsiStr := range s.withOpts["LSI"] {
		lsiTokens := strings.SplitN(lsiStr, ":", 4)
		lsiDef := lsiDef{indexName: strings.TrimSpace(lsiTokens[0])}
		if len(lsiTokens) > 1 {
			lsiDef.fieldName = strings.TrimSpace(lsiTokens[1])
		}
		if len(lsiTokens) > 2 {
			lsiDef.fieldType = strings.TrimSpace(strings.ToUpper(lsiTokens[2]))
		}
		if len(lsiTokens) > 3 {
			lsiDef.projectedFields = strings.TrimSpace(lsiTokens[3])
		}
		if lsiDef.indexName != "" {
			if lsiDef.fieldName == "" {
				return fmt.Errorf("invalid LSI definition <%s>: empty field name", lsiDef.indexName)
			}
			if _, ok := dataTypes[lsiDef.fieldType]; !ok {
				return fmt.Errorf("invalid type <%s> of LSI <%s>, accepts values are BINARY, NUMBER and STRING", lsiDef.fieldType, lsiDef.indexName)
			}
		}
		s.lsi = append(s.lsi, lsiDef)
	}

	// table class
	if _, ok := s.withOpts["CLASS"]; ok {
		tableClass := strings.ToUpper(s.withOpts["CLASS"].FirstString())
		if tableClasses[tableClass] == "" {
			return fmt.Errorf("invalid table class <%s>, accepts values are STANDARD, STANDARD_IA", s.withOpts["CLASS"].FirstString())
		}
		s.tableClass = &tableClass
	}

	// RCU
	if _, ok := s.withOpts["RCU"]; ok {
		rcu, err := strconv.ParseInt(s.withOpts["RCU"].FirstString(), 10, 64)
		if err != nil || rcu < 0 {
			return fmt.Errorf("invalid RCU value: %s", s.withOpts["RCU"])
		}
		s.rcu = &rcu
	}
	// WCU
	if _, ok := s.withOpts["WCU"]; ok {
		wcu, err := strconv.ParseInt(s.withOpts["WCU"].FirstString(), 10, 64)
		if err != nil || wcu < 0 {
			return fmt.Errorf("invalid WCU value: %s", s.withOpts["WCU"])
		}
		s.wcu = &wcu
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
	keySchema := make([]types.KeySchemaElement, 0, 2)
	keySchema = append(keySchema, types.KeySchemaElement{AttributeName: &s.pkName, KeyType: keyTypes["HASH"]})

	if s.skName != nil {
		attrDefs = append(attrDefs, types.AttributeDefinition{AttributeName: s.skName, AttributeType: dataTypes[*s.skType]})
		keySchema = append(keySchema, types.KeySchemaElement{AttributeName: s.skName, KeyType: keyTypes["RANGE"]})
	}

	lsi := make([]types.LocalSecondaryIndex, len(s.lsi))
	for i := range s.lsi {
		attrDefs = append(attrDefs, types.AttributeDefinition{AttributeName: &s.lsi[i].fieldName, AttributeType: dataTypes[s.lsi[i].fieldType]})
		lsi[i] = types.LocalSecondaryIndex{
			IndexName: &s.lsi[i].indexName,
			KeySchema: []types.KeySchemaElement{
				{AttributeName: &s.pkName, KeyType: keyTypes["HASH"]},
				{AttributeName: &s.lsi[i].fieldName, KeyType: keyTypes["RANGE"]},
			},
			Projection: &types.Projection{ProjectionType: types.ProjectionTypeKeysOnly},
		}
		if s.lsi[i].projectedFields == "*" {
			lsi[i].Projection.ProjectionType = types.ProjectionTypeAll
		} else if s.lsi[i].projectedFields != "" {
			lsi[i].Projection.ProjectionType = types.ProjectionTypeInclude
			nonKeyAttrs := strings.Split(s.lsi[i].projectedFields, ",")
			lsi[i].Projection.NonKeyAttributes = nonKeyAttrs
		}
	}

	input := &dynamodb.CreateTableInput{
		TableName:             &s.tableName,
		AttributeDefinitions:  attrDefs,
		KeySchema:             keySchema,
		LocalSecondaryIndexes: lsi,
	}
	if s.tableClass != nil {
		input.TableClass = tableClasses[*s.tableClass]
	}
	if (s.rcu == nil || *s.rcu == 0) && (s.wcu == nil || *s.wcu == 0) {
		input.BillingMode = types.BillingModePayPerRequest
	} else {
		input.BillingMode = types.BillingModeProvisioned
		input.ProvisionedThroughput = &types.ProvisionedThroughput{
			ReadCapacityUnits:  s.rcu,
			WriteCapacityUnits: s.wcu,
		}
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
		sort.Strings(rows.(*RowsListTables).tables)
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

// ColumnTypeScanType implements driver.RowsColumnTypeScanType.ColumnTypeScanType
func (r *RowsListTables) ColumnTypeScanType(index int) reflect.Type {
	return reddo.TypeString
}

// ColumnTypeDatabaseTypeName implements driver.RowsColumnTypeDatabaseTypeName.ColumnTypeDatabaseTypeName
func (r *RowsListTables) ColumnTypeDatabaseTypeName(index int) string {
	return "STRING"
}

// ColumnTypeLength implements driver.RowsColumnTypeLength.ColumnTypeLength
func (r *RowsListTables) ColumnTypeLength(index int) (length int64, ok bool) {
	return math.MaxInt64, true
}

// ColumnTypeNullable implements driver.RowsColumnTypeNullable.ColumnTypeNullable
func (r *RowsListTables) ColumnTypeNullable(index int) (nullable, ok bool) {
	return false, true
}

// ColumnTypePrecisionScale implements driver.RowsColumnTypePrecisionScale.ColumnTypePrecisionScale
func (r *RowsListTables) ColumnTypePrecisionScale(index int) (precision, scale int64, ok bool) {
	return 0, 0, false
}

/*----------------------------------------------------------------------*/

// StmtAlterTable implements "ALTER TABLE" operation.
//
// Syntax:
//
//		ALTER TABLE <table-name>
//		[WITH RCU=rcu[,] WITH WCU=wcu]
//		[[,] WITH CLASS=<table-class>]
//
//	- RCU: an integer specifying DynamoDB's read capacity.
//	- WCU: an integer specifying DynamoDB's write capacity.
//	- CLASS: table class, either STANDARD (default) or STANDARD_IA.
//	- Note: if RCU and WRU are both 0, table will be created with PAY_PER_REQUEST billing mode; otherwise table will be creatd with PROVISIONED mode.
//	- Note: there must be at least one space before the WITH keyword.
type StmtAlterTable struct {
	*Stmt
	tableName   string
	rcu, wcu    *int64
	tableClass  *string
	withOptsStr string
}

func (s *StmtAlterTable) parse() error {
	if err := s.Stmt.parseWithOpts(s.withOptsStr); err != nil {
		return err
	}

	// table class
	if _, ok := s.withOpts["CLASS"]; ok {
		tableClass := strings.ToUpper(s.withOpts["CLASS"].FirstString())
		if tableClasses[tableClass] == "" {
			return fmt.Errorf("invalid table class <%s>, accepts values are STANDARD, STANDARD_IA", s.withOpts["CLASS"].FirstString())
		}
		s.tableClass = &tableClass
	}

	// RCU
	if _, ok := s.withOpts["RCU"]; ok {
		rcu, err := strconv.ParseInt(s.withOpts["RCU"].FirstString(), 10, 64)
		if err != nil || rcu < 0 {
			return fmt.Errorf("invalid RCU value: %s", s.withOpts["RCU"])
		}
		s.rcu = &rcu
	}
	// WCU
	if _, ok := s.withOpts["WCU"]; ok {
		wcu, err := strconv.ParseInt(s.withOpts["WCU"].FirstString(), 10, 64)
		if err != nil || wcu < 0 {
			return fmt.Errorf("invalid WCU value: %s", s.withOpts["WCU"])
		}
		s.wcu = &wcu
	}

	return nil
}

func (s *StmtAlterTable) validate() error {
	if s.tableName == "" {
		return errors.New("table name is missing")
	}
	return nil
}

// Query implements driver.Stmt.Query.
// This function is not implemented, use Exec instead.
func (s *StmtAlterTable) Query(_ []driver.Value) (driver.Rows, error) {
	return nil, errors.New("this operation is not supported, please use Exec")
}

// Exec implements driver.Stmt.Exec.
func (s *StmtAlterTable) Exec(_ []driver.Value) (driver.Result, error) {
	input := &dynamodb.UpdateTableInput{
		TableName: &s.tableName,
	}
	if s.tableClass != nil {
		input.TableClass = tableClasses[*s.tableClass]
	}
	if s.rcu != nil || s.wcu != nil {
		if s.rcu != nil && *s.rcu == 0 && s.wcu != nil && *s.wcu == 0 {
			input.BillingMode = types.BillingModePayPerRequest
		} else {
			input.BillingMode = types.BillingModeProvisioned
			input.ProvisionedThroughput = &types.ProvisionedThroughput{
				ReadCapacityUnits:  s.rcu,
				WriteCapacityUnits: s.wcu,
			}
		}
	}
	_, err := s.conn.client.UpdateTable(context.Background(), input)
	result := &ResultAlterTable{Successful: err == nil}
	return result, err
}

// ResultAlterTable captures the result from CREATE TABLE operation.
type ResultAlterTable struct {
	// Successful flags if the operation was successful or not.
	Successful bool
}

// LastInsertId implements driver.Result.LastInsertId.
func (r *ResultAlterTable) LastInsertId() (int64, error) {
	return 0, fmt.Errorf("this operation is not supported.")
}

// RowsAffected implements driver.Result.RowsAffected.
func (r *ResultAlterTable) RowsAffected() (int64, error) {
	if r.Successful {
		return 1, nil
	}
	return 0, nil
}

/*----------------------------------------------------------------------*/

// StmtDropTable implements "DROP TABLE" operation.
//
// Syntax:
//
//	DROP TABLE [IF EXISTS] <table-name>
//
// If "IF EXISTS" is specified, Exec will silently swallow the error "ResourceNotFoundException".
type StmtDropTable struct {
	*Stmt
	tableName string
	ifExists  bool
}

func (s *StmtDropTable) validate() error {
	if s.tableName == "" {
		return errors.New("table name is missing")
	}
	return nil
}

// Query implements driver.Stmt.Query.
// This function is not implemented, use Exec instead.
func (s *StmtDropTable) Query(_ []driver.Value) (driver.Rows, error) {
	return nil, errors.New("this operation is not supported, please use Exec")
}

// Exec implements driver.Stmt.Exec.
func (s *StmtDropTable) Exec(_ []driver.Value) (driver.Result, error) {
	input := &dynamodb.DeleteTableInput{
		TableName: &s.tableName,
	}
	_, err := s.conn.client.DeleteTable(context.Background(), input)
	result := &ResultDropTable{Successful: err == nil}
	if s.ifExists && IsAwsError(err, "ResourceNotFoundException") {
		err = nil
	}
	return result, err
}

// ResultDropTable captures the result from DROP TABLE operation.
type ResultDropTable struct {
	// Successful flags if the operation was successful or not.
	Successful bool
}

// LastInsertId implements driver.Result.LastInsertId.
func (r *ResultDropTable) LastInsertId() (int64, error) {
	return 0, fmt.Errorf("this operation is not supported.")
}

// RowsAffected implements driver.Result.RowsAffected.
func (r *ResultDropTable) RowsAffected() (int64, error) {
	if r.Successful {
		return 1, nil
	}
	return 0, nil
}

/*----------------------------------------------------------------------*/

// StmtDescribeTable implements "DESCRIBE TABLE" operation.
//
// Syntax:
//
//	DESCRIBE TABLE <table-name>
type StmtDescribeTable struct {
	*Stmt
	tableName string
}

func (s *StmtDescribeTable) validate() error {
	if s.tableName == "" {
		return errors.New("table name is missing")
	}
	return nil
}

// Query implements driver.Stmt.Query.
func (s *StmtDescribeTable) Query(_ []driver.Value) (driver.Rows, error) {
	input := &dynamodb.DescribeTableInput{
		TableName: &s.tableName,
	}
	output, err := s.conn.client.DescribeTable(context.Background(), input)
	result := &RowsDescribeTable{count: 1}
	if err == nil {
		js, _ := json.Marshal(output.Table)
		json.Unmarshal(js, &result.tableInfo)
		for k := range result.tableInfo {
			result.columnList = append(result.columnList, k)
		}
		sort.Strings(result.columnList)
		result.columnTypeList = make([]reflect.Type, len(result.columnList))
		for i, col := range result.columnList {
			result.columnTypeList[i] = reflect.TypeOf(result.tableInfo[col])
		}
	}
	if IsAwsError(err, "ResourceNotFoundException") {
		err = nil
	}
	return result, err
}

// Exec implements driver.Stmt.Exec.
// This function is not implemented, use Query instead.
func (s *StmtDescribeTable) Exec(_ []driver.Value) (driver.Result, error) {
	return nil, errors.New("this operation is not supported, please use Query")
}

// RowsDescribeTable captures the result from DESCRIBE TABLE operation.
type RowsDescribeTable struct {
	count          int
	columnList     []string
	columnTypeList []reflect.Type
	tableInfo      map[string]interface{}
	cursorCount    int
}

// Columns implements driver.Rows.Columns.
func (r *RowsDescribeTable) Columns() []string {
	return r.columnList
}

// Close implements driver.Rows.Close.
func (r *RowsDescribeTable) Close() error {
	return nil
}

// Next implements driver.Rows.Next.
func (r *RowsDescribeTable) Next(dest []driver.Value) error {
	if r.cursorCount >= r.count {
		return io.EOF
	}
	for i, colName := range r.columnList {
		dest[i] = r.tableInfo[colName]
	}
	r.cursorCount++
	return nil
}

// ColumnTypeScanType implements driver.RowsColumnTypeScanType.ColumnTypeScanType
func (r *RowsDescribeTable) ColumnTypeScanType(index int) reflect.Type {
	return r.columnTypeList[index]
}

// ColumnTypeDatabaseTypeName implements driver.RowsColumnTypeDatabaseTypeName.ColumnTypeDatabaseTypeName
func (r *RowsDescribeTable) ColumnTypeDatabaseTypeName(index int) string {
	if r.columnTypeList[index] == nil {
		return ""
	}
	switch r.columnTypeList[index].Kind() {
	case reflect.Bool:
		return "BOOLEAN"
	case reflect.String:
		return "STRING"
	case reflect.Float32, reflect.Float64:
		return "NUMBER"
	case reflect.Array, reflect.Slice:
		return "ARRAY"
	case reflect.Map:
		return "MAP"
	}
	return ""
}

// // ColumnTypeLength implements driver.RowsColumnTypeLength.ColumnTypeLength
// func (r *RowsDescribeTable) ColumnTypeLength(index int) (length int64, ok bool) {
// 	return math.MaxInt64, true
// }

// // ColumnTypeNullable implements driver.RowsColumnTypeNullable.ColumnTypeNullable
// func (r *RowsDescribeTable) ColumnTypeNullable(index int) (nullable, ok bool) {
// 	return false, true
// }

// // ColumnTypePrecisionScale implements driver.RowsColumnTypePrecisionScale.ColumnTypePrecisionScale
// func (r *RowsDescribeTable) ColumnTypePrecisionScale(index int) (precision, scale int64, ok bool) {
// 	return 0, 0, false
// }
