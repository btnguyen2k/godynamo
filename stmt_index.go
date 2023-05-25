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
)

// RowsDescribeIndex captures the result from DESCRIBE LSI or DESCRIBE GSI statement.
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

// StmtDescribeLSI implements "DESCRIBE LSI" statement.
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

/*----------------------------------------------------------------------*/

// StmtCreateGSI implements "CREATE GSI" statement.
//
// Syntax:
//
//		CREATE GSI [IF NOT EXISTS] <index-name> ON <table-name>
//		<WITH PK=pk-attr-name:data-type>
//		[[,] WITH SK=sk-attr-name:data-type]
//		[[,] WITH wcu=<number>[,] WITH rcu=<number>]
//		[[,] WITH projection=*|attr1,attr2,attr3,...]
//
//	- PK: GSI's partition key, format name:type (type is one of String, Number, Binary).
//	- SK: GSI's sort key, format name:type (type is one of String, Number, Binary).
//	- RCU: an integer specifying DynamoDB's read capacity.
//	- WCU: an integer specifying DynamoDB's write capacity.
//	- PROJECTION:
//	  - if not supplied, GSI will be created with projection setting KEYS_ONLY.
//	  - if equal to "*", GSI will be created with projection setting ALL.
//	  - if supplied with comma-separated attribute list, for example "attr1,attr2,attr3", GSI will be created with projection setting INCLUDE.
//	- If "IF NOT EXISTS" is specified, Exec will silently swallow the error "Attempting to create an index which already exists".
//	- Note: The provisioned throughput settings of a GSI are separate from those of its base table.
//	- Note: GSI inherit the RCU and WCU mode from the base table. That means if the base table is in on-demand mode, then DynamoDB also creates the GSI in on-demand mode.
//	- Note: there must be at least one space before the WITH keyword.
type StmtCreateGSI struct {
	*Stmt
	indexName, tableName string
	ifNotExists          bool
	pkName, pkType       string
	skName, skType       *string
	rcu, wcu             *int64
	projectedAttrs       string
	withOptsStr          string
}

func (s *StmtCreateGSI) parse() error {
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

	// projection
	s.projectedAttrs = s.withOpts["PROJECTION"].FirstString()

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

func (s *StmtCreateGSI) validate() error {
	if s.tableName == "" {
		return errors.New("table name is missing")
	}
	if s.indexName == "" {
		return errors.New("index name is missing")
	}
	return nil
}

// Query implements driver.Stmt.Query.
// This function is not implemented, use Exec instead.
func (s *StmtCreateGSI) Query(_ []driver.Value) (driver.Rows, error) {
	return nil, errors.New("this operation is not supported, please use Exec")
}

// Exec implements driver.Stmt.Exec.
func (s *StmtCreateGSI) Exec(_ []driver.Value) (driver.Result, error) {
	attrDefs := make([]types.AttributeDefinition, 0, 2)
	attrDefs = append(attrDefs, types.AttributeDefinition{AttributeName: &s.pkName, AttributeType: dataTypes[s.pkType]})
	keySchema := make([]types.KeySchemaElement, 0, 2)
	keySchema = append(keySchema, types.KeySchemaElement{AttributeName: &s.pkName, KeyType: keyTypes["HASH"]})
	if s.skName != nil {
		attrDefs = append(attrDefs, types.AttributeDefinition{AttributeName: s.skName, AttributeType: dataTypes[*s.skType]})
		keySchema = append(keySchema, types.KeySchemaElement{AttributeName: s.skName, KeyType: keyTypes["RANGE"]})
	}

	gsiInput := &types.CreateGlobalSecondaryIndexAction{
		IndexName: &s.indexName,
		KeySchema: keySchema,
		Projection: &types.Projection{
			ProjectionType: types.ProjectionTypeKeysOnly,
		},
	}
	if s.projectedAttrs == "*" {
		gsiInput.Projection.ProjectionType = types.ProjectionTypeAll
	} else if s.projectedAttrs != "" {
		gsiInput.Projection.ProjectionType = types.ProjectionTypeInclude
		nonKeyAttrs := strings.Split(s.projectedAttrs, ",")
		gsiInput.Projection.NonKeyAttributes = nonKeyAttrs
	}

	if s.rcu != nil && s.wcu != nil {
		gsiInput.ProvisionedThroughput = &types.ProvisionedThroughput{
			ReadCapacityUnits:  s.rcu,
			WriteCapacityUnits: s.wcu,
		}
	}

	input := &dynamodb.UpdateTableInput{
		TableName:                   &s.tableName,
		AttributeDefinitions:        attrDefs,
		GlobalSecondaryIndexUpdates: []types.GlobalSecondaryIndexUpdate{{Create: gsiInput}},
	}

	_, err := s.conn.client.UpdateTable(context.Background(), input)
	result := &ResultCreateGSI{Successful: err == nil}
	if s.ifNotExists && err != nil {
		if IsAwsError(err, "ResourceInUseException") || strings.Index(err.Error(), "already exist") >= 0 {
			err = nil
		}
	}
	return result, err
}

// ResultCreateGSI captures the result from CREATE GSI statement.
type ResultCreateGSI struct {
	// Successful flags if the operation was successful or not.
	Successful bool
}

// LastInsertId implements driver.Result.LastInsertId.
func (r *ResultCreateGSI) LastInsertId() (int64, error) {
	return 0, fmt.Errorf("this operation is not supported")
}

// RowsAffected implements driver.Result.RowsAffected.
func (r *ResultCreateGSI) RowsAffected() (int64, error) {
	if r.Successful {
		return 1, nil
	}
	return 0, nil
}

/*----------------------------------------------------------------------*/

// StmtDescribeGSI implements "DESCRIBE GSI" statement.
//
// Syntax:
//
//	DESCRIBE GSI <index-name> ON <table-name>
type StmtDescribeGSI struct {
	*Stmt
	tableName, indexName string
}

func (s *StmtDescribeGSI) validate() error {
	if s.tableName == "" {
		return errors.New("table name is missing")
	}
	if s.indexName == "" {
		return errors.New("index name is missing")
	}
	return nil
}

// Query implements driver.Stmt.Query.
func (s *StmtDescribeGSI) Query(_ []driver.Value) (driver.Rows, error) {
	input := &dynamodb.DescribeTableInput{
		TableName: &s.tableName,
	}
	output, err := s.conn.client.DescribeTable(context.Background(), input)
	result := &RowsDescribeIndex{count: 0}
	if err == nil {
		for _, gsi := range output.Table.GlobalSecondaryIndexes {
			if gsi.IndexName != nil && *gsi.IndexName == s.indexName {
				result.count = 1
				js, _ := json.Marshal(gsi)
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
func (s *StmtDescribeGSI) Exec(_ []driver.Value) (driver.Result, error) {
	return nil, errors.New("this operation is not supported, please use Query")
}

/*----------------------------------------------------------------------*/

// StmtAlterGSI implements "ALTER GSI" statement.
//
// Syntax:
//
//		ALTER GSI <index-name> ON <table-name>
//		WITH wcu=<number>[,] WITH rcu=<number>
//
//	- RCU: an integer specifying DynamoDB's read capacity.
//	- WCU: an integer specifying DynamoDB's write capacity.
//	- Note: The provisioned throughput settings of a GSI are separate from those of its base table.
//	- Note: GSI inherit the RCU and WCU mode from the base table. That means if the base table is in on-demand mode, then DynamoDB also creates the GSI in on-demand mode.
//	- Note: there must be at least one space before the WITH keyword.
type StmtAlterGSI struct {
	*Stmt
	indexName, tableName string
	rcu, wcu             *int64
	withOptsStr          string
}

func (s *StmtAlterGSI) parse() error {
	if err := s.Stmt.parseWithOpts(s.withOptsStr); err != nil {
		return err
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

func (s *StmtAlterGSI) validate() error {
	if s.tableName == "" {
		return errors.New("table name is missing")
	}
	if s.indexName == "" {
		return errors.New("index name is missing")
	}
	return nil
}

// Query implements driver.Stmt.Query.
// This function is not implemented, use Exec instead.
func (s *StmtAlterGSI) Query(_ []driver.Value) (driver.Rows, error) {
	return nil, errors.New("this operation is not supported, please use Exec")
}

// Exec implements driver.Stmt.Exec.
func (s *StmtAlterGSI) Exec(_ []driver.Value) (driver.Result, error) {
	gsiInput := &types.UpdateGlobalSecondaryIndexAction{
		IndexName: &s.indexName,
		ProvisionedThroughput: &types.ProvisionedThroughput{
			ReadCapacityUnits:  s.rcu,
			WriteCapacityUnits: s.wcu,
		},
	}
	input := &dynamodb.UpdateTableInput{
		TableName:                   &s.tableName,
		GlobalSecondaryIndexUpdates: []types.GlobalSecondaryIndexUpdate{{Update: gsiInput}},
	}

	_, err := s.conn.client.UpdateTable(context.Background(), input)
	result := &ResultAlterGSI{Successful: err == nil}
	return result, err
}

// ResultAlterGSI captures the result from ALTER GSI statement.
type ResultAlterGSI struct {
	// Successful flags if the operation was successful or not.
	Successful bool
}

// LastInsertId implements driver.Result.LastInsertId.
func (r *ResultAlterGSI) LastInsertId() (int64, error) {
	return 0, fmt.Errorf("this operation is not supported")
}

// RowsAffected implements driver.Result.RowsAffected.
func (r *ResultAlterGSI) RowsAffected() (int64, error) {
	if r.Successful {
		return 1, nil
	}
	return 0, nil
}

/*----------------------------------------------------------------------*/

// StmtDropGSI implements "DROP GSI" statement.
//
// Syntax:
//
//	DROP GSI [IF EXISTS] <index-name> ON <table-name>
//
// If "IF EXISTS" is specified, Exec will silently swallow the error "ResourceNotFoundException".
type StmtDropGSI struct {
	*Stmt
	tableName string
	indexName string
	ifExists  bool
}

func (s *StmtDropGSI) validate() error {
	if s.tableName == "" {
		return errors.New("table name is missing")
	}
	if s.indexName == "" {
		return errors.New("index name is missing")
	}
	return nil
}

// Query implements driver.Stmt.Query.
// This function is not implemented, use Exec instead.
func (s *StmtDropGSI) Query(_ []driver.Value) (driver.Rows, error) {
	return nil, errors.New("this operation is not supported, please use Exec")
}

// Exec implements driver.Stmt.Exec.
func (s *StmtDropGSI) Exec(_ []driver.Value) (driver.Result, error) {
	gsiInput := &types.DeleteGlobalSecondaryIndexAction{IndexName: &s.indexName}
	input := &dynamodb.UpdateTableInput{
		TableName:                   &s.tableName,
		GlobalSecondaryIndexUpdates: []types.GlobalSecondaryIndexUpdate{{Delete: gsiInput}},
	}
	_, err := s.conn.client.UpdateTable(context.Background(), input)
	result := &ResultDropGSI{Successful: err == nil}
	if s.ifExists && IsAwsError(err, "ResourceNotFoundException") {
		err = nil
	}
	return result, err
}

// ResultDropGSI captures the result from DROP GSI statement.
type ResultDropGSI struct {
	// Successful flags if the operation was successful or not.
	Successful bool
}

// LastInsertId implements driver.Result.LastInsertId.
func (r *ResultDropGSI) LastInsertId() (int64, error) {
	return 0, fmt.Errorf("this operation is not supported")
}

// RowsAffected implements driver.Result.RowsAffected.
func (r *ResultDropGSI) RowsAffected() (int64, error) {
	if r.Successful {
		return 1, nil
	}
	return 0, nil
}
