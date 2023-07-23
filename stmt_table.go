package godynamo

import (
	"context"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"reflect"
	"sort"
	"strconv"
	"strings"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/btnguyen2k/consu/reddo"
)

type lsiDef struct {
	indexName, attrName, attrType string
	projectedAttrs                string
}

/*----------------------------------------------------------------------*/

// StmtCreateTable implements "CREATE TABLE" statement.
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
			lsiDef.attrName = strings.TrimSpace(lsiTokens[1])
		}
		if len(lsiTokens) > 2 {
			lsiDef.attrType = strings.TrimSpace(strings.ToUpper(lsiTokens[2]))
		}
		if len(lsiTokens) > 3 {
			lsiDef.projectedAttrs = strings.TrimSpace(lsiTokens[3])
		}
		if lsiDef.indexName != "" {
			if lsiDef.attrName == "" {
				return fmt.Errorf("invalid LSI definition <%s>: empty field name", lsiDef.indexName)
			}
			if _, ok := dataTypes[lsiDef.attrType]; !ok {
				return fmt.Errorf("invalid type <%s> of LSI <%s>, accepts values are BINARY, NUMBER and STRING", lsiDef.attrType, lsiDef.indexName)
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

// Query implements driver.Stmt/Query.
// This function is not implemented, use Exec instead.
func (s *StmtCreateTable) Query(_ []driver.Value) (driver.Rows, error) {
	return nil, errors.New("this operation is not supported, please use Exec")
}

// QueryContext implements driver.StmtQueryContext/QueryContext.
// This function is not implemented, use ExecContext instead.
func (s *StmtCreateTable) QueryContext(_ context.Context, _ []driver.NamedValue) (driver.Rows, error) {
	return nil, errors.New("this operation is not supported, please use ExecContext")
}

// Exec implements driver.Stmt/Exec.
func (s *StmtCreateTable) Exec(_ []driver.Value) (driver.Result, error) {
	return s.ExecContext(nil, nil)
}

// ExecContext implements driver.StmtExecContext/Exec.
//
// @Available since v0.2.0
func (s *StmtCreateTable) ExecContext(ctx context.Context, _ []driver.NamedValue) (driver.Result, error) {
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
		attrDefs = append(attrDefs, types.AttributeDefinition{AttributeName: &s.lsi[i].attrName, AttributeType: dataTypes[s.lsi[i].attrType]})
		lsi[i] = types.LocalSecondaryIndex{
			IndexName: &s.lsi[i].indexName,
			KeySchema: []types.KeySchemaElement{
				{AttributeName: &s.pkName, KeyType: keyTypes["HASH"]},
				{AttributeName: &s.lsi[i].attrName, KeyType: keyTypes["RANGE"]},
			},
			Projection: &types.Projection{ProjectionType: types.ProjectionTypeKeysOnly},
		}
		if s.lsi[i].projectedAttrs == "*" {
			lsi[i].Projection.ProjectionType = types.ProjectionTypeAll
		} else if s.lsi[i].projectedAttrs != "" {
			lsi[i].Projection.ProjectionType = types.ProjectionTypeInclude
			nonKeyAttrs := strings.Split(s.lsi[i].projectedAttrs, ",")
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
	_, err := s.conn.client.CreateTable(s.conn.ensureContext(ctx), input)
	affectedRows := int64(0)
	if err == nil {
		affectedRows = 1
	}
	if s.ifNotExists && IsAwsError(err, "ResourceInUseException") {
		err = nil
	}
	return &ResultNoResultSet{err: err, affectedRows: affectedRows}, err
}

/*----------------------------------------------------------------------*/

// StmtListTables implements "LIST TABLES" statement.
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

// Exec implements driver.Stmt/Exec.
// This function is not implemented, use Query instead.
func (s *StmtListTables) Exec(_ []driver.Value) (driver.Result, error) {
	return nil, errors.New("this operation is not supported, please use Query")
}

// ExecContext implements driver.StmtExecContext/Exec.
// This function is not implemented, use QueryContext instead.
func (s *StmtListTables) ExecContext(_ context.Context, _ []driver.NamedValue) (driver.Result, error) {
	return nil, errors.New("this operation is not supported, please use QueryContext")
}

// Query implements driver.Stmt/Query.
func (s *StmtListTables) Query(_ []driver.Value) (driver.Rows, error) {
	return s.QueryContext(nil, nil)
}

// QueryContext implements driver.StmtQueryContext/QueryContext.
//
// @Available since v0.2.0
func (s *StmtListTables) QueryContext(ctx context.Context, _ []driver.NamedValue) (driver.Rows, error) {
	output, err := s.conn.client.ListTables(s.conn.ensureContext(ctx), &dynamodb.ListTablesInput{})
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

// RowsListTables captures the result from LIST TABLES statement.
type RowsListTables struct {
	count       int
	tables      []string
	cursorCount int
}

// Columns implements driver.Rows/Columns.
func (r *RowsListTables) Columns() []string {
	return []string{"$1"}
}

// Close implements driver.Rows/Close.
func (r *RowsListTables) Close() error {
	return nil
}

// Next implements driver.Rows/Next.
func (r *RowsListTables) Next(dest []driver.Value) error {
	if r.cursorCount >= r.count {
		return io.EOF
	}
	rowData := r.tables[r.cursorCount]
	r.cursorCount++
	dest[0] = rowData
	return nil
}

// ColumnTypeScanType implements driver.RowsColumnTypeScanType/ColumnTypeScanType
func (r *RowsListTables) ColumnTypeScanType(_ int) reflect.Type {
	return reddo.TypeString
}

// ColumnTypeDatabaseTypeName implements driver.RowsColumnTypeDatabaseTypeName/ColumnTypeDatabaseTypeName
func (r *RowsListTables) ColumnTypeDatabaseTypeName(_ int) string {
	return "STRING"
}

/*----------------------------------------------------------------------*/

// StmtAlterTable implements "ALTER TABLE" statement.
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
//	- Note: if RCU and WRU are both 0, table's billing mode will be updated to PAY_PER_REQUEST; otherwise billing mode will be updated to PROVISIONED.
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

// Query implements driver.Stmt/Query.
// This function is not implemented, use Exec instead.
func (s *StmtAlterTable) Query(_ []driver.Value) (driver.Rows, error) {
	return nil, errors.New("this operation is not supported, please use Exec")
}

// QueryContext implements driver.StmtQueryContext/QueryContext.
// This function is not implemented, use ExecContext instead.
func (s *StmtAlterTable) QueryContext(_ []driver.NamedValue) (driver.Rows, error) {
	return nil, errors.New("this operation is not supported, please use ExecContext")
}

// Exec implements driver.Stmt/Exec.
func (s *StmtAlterTable) Exec(_ []driver.Value) (driver.Result, error) {
	return s.ExecContext(nil, nil)
}

// ExecContext implements driver.StmtExecContext/ExecContext.
//
// @Available since v0.2.0
func (s *StmtAlterTable) ExecContext(ctx context.Context, _ []driver.NamedValue) (driver.Result, error) {
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
	_, err := s.conn.client.UpdateTable(s.conn.ensureContext(ctx), input)
	affectedRows := int64(0)
	if err == nil {
		affectedRows = 1
	}
	return &ResultNoResultSet{err: err, affectedRows: affectedRows}, err
}

/*----------------------------------------------------------------------*/

// StmtDropTable implements "DROP TABLE" statement.
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

// Query implements driver.Stmt/Query.
// This function is not implemented, use Exec instead.
func (s *StmtDropTable) Query(_ []driver.Value) (driver.Rows, error) {
	return nil, errors.New("this operation is not supported, please use Exec")
}

// QueryContext implements driver.StmtQueryContext/QueryContext.
// This function is not implemented, use ExecContext instead.
func (s *StmtDropTable) QueryContext(_ context.Context, _ []driver.NamedValue) (driver.Rows, error) {
	return nil, errors.New("this operation is not supported, please use ExecContext")
}

// Exec implements driver.Stmt/Exec.
func (s *StmtDropTable) Exec(_ []driver.Value) (driver.Result, error) {
	return s.ExecContext(nil, nil)
}

// ExecContext implements driver.StmtExecContext/Exec.
//
// @Available since v0.2.0
func (s *StmtDropTable) ExecContext(ctx context.Context, _ []driver.NamedValue) (driver.Result, error) {
	input := &dynamodb.DeleteTableInput{
		TableName: &s.tableName,
	}
	_, err := s.conn.client.DeleteTable(s.conn.ensureContext(ctx), input)
	affectedRows := int64(0)
	if err == nil {
		affectedRows = 1
	}
	if s.ifExists && IsAwsError(err, "ResourceNotFoundException") {
		err = nil
	}
	return &ResultNoResultSet{err: err, affectedRows: affectedRows}, err
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

// Exec implements driver.Stmt/Exec.
// This function is not implemented, use Query instead.
func (s *StmtDescribeTable) Exec(_ []driver.Value) (driver.Result, error) {
	return nil, errors.New("this operation is not supported, please use Query")
}

// ExecContext implements driver.StmtExecContext/ExecContext.
// This function is not implemented, use QueryContext instead.
func (s *StmtDescribeTable) ExecContext(_ context.Context, _ []driver.NamedValue) (driver.Result, error) {
	return nil, errors.New("this operation is not supported, please use QueryContext")
}

// Query implements driver.Stmt/Query.
func (s *StmtDescribeTable) Query(_ []driver.Value) (driver.Rows, error) {
	return s.QueryContext(nil, nil)
}

// QueryContext implements driver.StmtQueryContext/Query.
//
// @Available since v0.2.0
func (s *StmtDescribeTable) QueryContext(ctx context.Context, _ []driver.NamedValue) (driver.Rows, error) {
	input := &dynamodb.DescribeTableInput{
		TableName: &s.tableName,
	}
	output, err := s.conn.client.DescribeTable(s.conn.ensureContext(ctx), input)
	result := &RowsDescribeTable{count: 0}
	if err == nil {
		result.count = 1
		js, _ := json.Marshal(output.Table)
		json.Unmarshal(js, &result.tableInfo)

		result.columnList = make([]string, 0)
		result.columnTypes = make(map[string]reflect.Type)
		result.columnSourceTypes = make(map[string]string)
		for col, spec := range dynamodbTableSpec {
			result.columnList = append(result.columnList, col)
			result.columnTypes[col] = spec.scanType
			result.columnSourceTypes[col] = spec.srcType
		}
		sort.Strings(result.columnList)
	}
	if IsAwsError(err, "ResourceNotFoundException") {
		err = nil
	}
	return result, err
}

var (
	dynamodbTableSpec = map[string]struct {
		scanType reflect.Type
		srcType  string
	}{
		"ArchivalSummary":           {srcType: "M", scanType: typeM},
		"AttributeDefinitions":      {srcType: "L", scanType: typeL},
		"BillingModeSummary":        {srcType: "M", scanType: typeM},
		"CreationDateTime":          {srcType: "S", scanType: typeTime},
		"DeletionProtectionEnabled": {srcType: "BOOL", scanType: typeBool},
		"GlobalSecondaryIndexes":    {srcType: "L", scanType: typeL},
		"GlobalTableVersion":        {srcType: "S", scanType: typeS},
		"ItemCount":                 {srcType: "N", scanType: typeN},
		"KeySchema":                 {srcType: "L", scanType: typeL},
		"LatestStreamArn":           {srcType: "S", scanType: typeS},
		"LatestStreamLabel":         {srcType: "S", scanType: typeS},
		"LocalSecondaryIndexes":     {srcType: "L", scanType: typeL},
		"ProvisionedThroughput":     {srcType: "M", scanType: typeM},
		"Replicas":                  {srcType: "L", scanType: typeL},
		"RestoreSummary":            {srcType: "M", scanType: typeM},
		"SSEDescription":            {srcType: "M", scanType: typeM},
		"StreamSpecification":       {srcType: "M", scanType: typeM},
		"TableArn":                  {srcType: "S", scanType: typeS},
		"TableClassSummary":         {srcType: "M", scanType: typeM},
		"TableId":                   {srcType: "S", scanType: typeS},
		"TableName":                 {srcType: "S", scanType: typeS},
		"TableSizeBytes":            {srcType: "N", scanType: typeN},
		"TableStatus":               {srcType: "S", scanType: typeS},
	}
)

// RowsDescribeTable captures the result from DESCRIBE TABLE statement.
type RowsDescribeTable struct {
	count             int
	columnList        []string
	columnTypes       map[string]reflect.Type
	columnSourceTypes map[string]string
	tableInfo         map[string]interface{}
	cursorCount       int
}

// Columns implements driver.Rows/Columns.
func (r *RowsDescribeTable) Columns() []string {
	return r.columnList
}

// Close implements driver.Rows/Close.
func (r *RowsDescribeTable) Close() error {
	return nil
}

// Next implements driver.Rows/Next.
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

// ColumnTypeScanType implements driver.RowsColumnTypeScanType/ColumnTypeScanType
func (r *RowsDescribeTable) ColumnTypeScanType(index int) reflect.Type {
	return r.columnTypes[r.columnList[index]]
}

// ColumnTypeDatabaseTypeName implements driver.RowsColumnTypeDatabaseTypeName/ColumnTypeDatabaseTypeName
//
// @since v0.3.0 ColumnTypeDatabaseTypeName returns DynamoDB's native data types (e.g. B, N, S, SS, NS, BS, BOOL, L, M, NULL).
func (r *RowsDescribeTable) ColumnTypeDatabaseTypeName(index int) string {
	return r.columnSourceTypes[r.columnList[index]]
}
