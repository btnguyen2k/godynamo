package godynamo_test

import (
	"context"
	"fmt"
	"github.com/miyamo2/godynamo"
	"strings"
	"testing"
	"time"
)

func Test_Query_CreateTable(t *testing.T) {
	testName := "Test_Query_CreateTable"
	db := _openDb(t, testName)
	defer func() { _ = db.Close() }()

	_, err := db.Query(fmt.Sprintf("CREATE TABLE %s WITH pk=id:string", tblTestTemp))
	if err == nil || strings.Index(err.Error(), "not supported") < 0 {
		t.Fatalf("%s failed: expected 'not support' error, but received %#v", testName, err)
	}
}

func Test_Exec_CreateTable_Query_DescribeTable(t *testing.T) {
	testName := "Test_Exec_CreateTable_Query_DescribeTable"
	db := _openDb(t, testName)
	defer func() { _ = db.Close() }()
	_initTest(db)

	testData := []struct {
		name         string
		sql          string
		tableInfo    *tableInfo
		affectedRows int64
	}{
		{name: "basic", sql: fmt.Sprintf(`CREATE TABLE %s%d WITH PK=id:string`, tblTestTemp, 1), affectedRows: 1,
			tableInfo: &tableInfo{tableName: fmt.Sprintf(`%s%d`, tblTestTemp, 1), billingMode: "PAY_PER_REQUEST", wcu: 0, rcu: 0, pkAttr: "id", pkType: "S"}},
		{name: "if_not_exists", sql: fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s%d WITH PK=id:NUMBER`, tblTestTemp, 1), affectedRows: 0,
			tableInfo: &tableInfo{tableName: fmt.Sprintf(`%s%d`, tblTestTemp, 1), billingMode: "PAY_PER_REQUEST", wcu: 0, rcu: 0, pkAttr: "id", pkType: "S"}},
		{name: "with_sk", sql: fmt.Sprintf(`CREATE TABLE %s%d WITH PK=id:binary WITH sk=grade:number, WITH class=standard`, tblTestTemp, 2), affectedRows: 1,
			tableInfo: &tableInfo{tableName: fmt.Sprintf(`%s%d`, tblTestTemp, 2), billingMode: "PAY_PER_REQUEST", wcu: 0, rcu: 0, pkAttr: "id", pkType: "B", skAttr: "grade", skType: "N"}},
		{name: "with_rcu_wcu", sql: fmt.Sprintf(`CREATE TABLE %s%d WITH PK=id:number WITH rcu=1 WITH wcu=2 WITH class=standard_ia`, tblTestTemp, 3), affectedRows: 1,
			tableInfo: &tableInfo{tableName: fmt.Sprintf(`%s%d`, tblTestTemp, 3), billingMode: "", wcu: 2, rcu: 1, pkAttr: "id", pkType: "N"}},
		{name: "with_lsi", sql: fmt.Sprintf(`CREATE TABLE %s%d WITH PK=id:binary WITH SK=username:string WITH LSI=index1:grade:number, WITH LSI=index2:dob:string:*, WITH LSI=index3:yob:binary:a,b,c`, tblTestTemp, 4), affectedRows: 1,
			tableInfo: &tableInfo{tableName: fmt.Sprintf(`%s%d`, tblTestTemp, 4), billingMode: "PAY_PER_REQUEST", wcu: 0, rcu: 0, pkAttr: "id", pkType: "B", skAttr: "username", skType: "S",
				lsi: map[string]lsiInfo{
					"index1": {projType: "KEYS_ONLY", lsiDef: lsiDef{indexName: "index1", attrName: "grade", attrType: "N"}},
					"index2": {projType: "ALL", lsiDef: lsiDef{indexName: "index2", attrName: "dob", attrType: "S"}},
					"index3": {projType: "INCLUDE", lsiDef: lsiDef{indexName: "index3", attrName: "yob", attrType: "B", projectedAttrs: "a,b,c"}},
				},
			}},
	}

	for _, testCase := range testData {
		t.Run(testCase.name, func(t *testing.T) {
			execResult, err := db.Exec(testCase.sql)
			if err != nil {
				t.Fatalf("%s failed: %s", testName+"/"+testCase.name+"/create_table", err)
			}
			affectedRows, err := execResult.RowsAffected()
			if err != nil {
				t.Fatalf("%s failed: %s", testName+"/"+testCase.name+"/rows_affected", err)
			}
			if affectedRows != testCase.affectedRows {
				t.Fatalf("%s failed: expected %#v affected-rows but received %#v", testName+"/"+testCase.name, testCase.affectedRows, affectedRows)
			}

			if testCase.tableInfo == nil {
				return
			}

			ctx, cancelF := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancelF()
			err = godynamo.WaitForTableStatus(ctx, db, testCase.tableInfo.tableName, []string{"ACTIVE"}, 500*time.Millisecond)
			if err != nil {
				t.Fatalf("%s failed: %s", testName+"/"+testCase.name+"/WaitForTableStatus", err)
			}

			dbresult, err := db.Query(fmt.Sprintf(`DESCRIBE TABLE %s`, testCase.tableInfo.tableName))
			if err != nil {
				t.Fatalf("%s failed: %s", testName+"/"+testCase.name+"/describe_table", err)
			}
			rows, err := _fetchAllRows(dbresult)
			if err != nil {
				t.Fatalf("%s failed: %s", testName+"/"+testCase.name+"/fetch_rows", err)
			}
			_verifyTableInfo(t, testName+"/"+testCase.name, rows[0], testCase.tableInfo)
		})
	}
}

//func Test_Exec_ListTables(t *testing.T) {
//	testName := "Test_Exec_ListTables"
//	db := _openDb(t, testName)
//	defer db.Close()
//
//	_, err := db.Exec("LIST TABLES")
//	if err == nil || strings.Index(err.Error(), "not supported") < 0 {
//		t.Fatalf("%s failed: expected 'not support' error, but received %#v", testName, err)
//	}
//}
//
//func Test_Query_ListTables(t *testing.T) {
//	testName := "Test_Query_ListTables"
//	db := _openDb(t, testName)
//	_initTest(db)
//	defer db.Close()
//
//	tableList := []string{"tbltest2", "tbltest1", "tbltest3", "tbltest0"}
//	for _, tbl := range tableList {
//		db.Exec("CREATE TABLE " + tbl + " WITH PK=id:string")
//	}
//
//	dbresult, err := db.Query(`LIST TABLES`)
//	if err != nil {
//		t.Fatalf("%s failed: %s", testName+"/query", err)
//	}
//	rows, err := _fetchAllRows(dbresult)
//	if err != nil {
//		t.Fatalf("%s failed: %s", testName+"/fetch_rows", err)
//	}
//	if len(rows) != 4 {
//		t.Fatalf("%s failed: expected 4 rows but received %d", testName+"/fetch_rows", len(rows))
//	}
//	for i := 0; i < 4; i++ {
//		tblname := rows[i]["$1"].(string)
//		if tblname != "tbltest"+strconv.Itoa(i) {
//			t.Fatalf("%s failed: expected row #%d to be %#v but received %#v", testName+"/fetch_rows", i, "tbltemp"+strconv.Itoa(i), tblname)
//		}
//	}
//}
//
//func Test_Query_AlterTable(t *testing.T) {
//	testName := "Test_Query_AlterTable"
//	db := _openDb(t, testName)
//	defer db.Close()
//
//	_, err := db.Query("ALTER TABLE tbltemp WITH wcu=0 WITH rcu=0")
//	if err == nil || strings.Index(err.Error(), "not supported") < 0 {
//		t.Fatalf("%s failed: expected 'not support' error, but received %#v", testName, err)
//	}
//}
//
//func Test_Exec_AlterTable(t *testing.T) {
//	testName := "Test_Exec_AlterTable"
//	db := _openDb(t, testName)
//	_initTest(db)
//	defer db.Close()
//
//	db.Exec(`CREATE TABLE tbltest WITH PK=id:string`)
//	testData := []struct {
//		name         string
//		sql          string
//		tableInfo    *tableInfo
//		affectedRows int64
//	}{
//		{name: "change_wcu_rcu_provisioned", sql: `ALTER TABLE tbltest WITH wcu=3 WITH rcu=5`, affectedRows: 1, tableInfo: &tableInfo{tableName: "tbltest",
//			billingMode: "PROVISIONED", wcu: 3, rcu: 5, pkAttr: "id", pkType: "S"}},
//		{name: "change_wcu_rcu_on_demand", sql: `ALTER TABLE tbltest WITH wcu=0 WITH rcu=0`, affectedRows: 1, tableInfo: &tableInfo{tableName: "tbltest",
//			billingMode: "PAY_PER_REQUEST", wcu: 0, rcu: 0, pkAttr: "id", pkType: "S"}},
//		// DynamoDB Docker version does not support changing table class
//	}
//
//	for _, testCase := range testData {
//		t.Run(testCase.name, func(t *testing.T) {
//			execResult, err := db.Exec(testCase.sql)
//			if err != nil {
//				t.Fatalf("%s failed: %s", testName+"/"+testCase.name, err)
//			}
//			affectedRows, err := execResult.RowsAffected()
//			if err != nil {
//				t.Fatalf("%s failed: %s", testName+"/"+testCase.name+"/rows_affected", err)
//			}
//			if affectedRows != testCase.affectedRows {
//				t.Fatalf("%s failed: expected %#v affected-rows but received %#v", testName+"/"+testCase.name, testCase.affectedRows, affectedRows)
//			}
//
//			if testCase.tableInfo == nil {
//				return
//			}
//			dbresult, err := db.Query(`DESCRIBE TABLE ` + testCase.tableInfo.tableName)
//			if err != nil {
//				t.Fatalf("%s failed: %s", testName+"/"+testCase.name+"/describe_table", err)
//			}
//			rows, err := _fetchAllRows(dbresult)
//			if err != nil {
//				t.Fatalf("%s failed: %s", testName+"/"+testCase.name+"/fetch_rows", err)
//			}
//			_verifyTableInfo(t, testName+"/"+testCase.name, rows[0], testCase.tableInfo)
//		})
//	}
//}
//
//func Test_Query_DropTable(t *testing.T) {
//	testName := "Test_Query_DropTable"
//	db := _openDb(t, testName)
//	defer db.Close()
//
//	_, err := db.Query("DROP TABLE tbltemp")
//	if err == nil || strings.Index(err.Error(), "not supported") < 0 {
//		t.Fatalf("%s failed: expected 'not support' error, but received %#v", testName, err)
//	}
//}
//
//func Test_Exec_DropTable(t *testing.T) {
//	testName := "Test_Exec_DropTable"
//	db := _openDb(t, testName)
//	_initTest(db)
//	defer db.Close()
//
//	db.Exec(`CREATE TABLE tbltest WITH PK=id:string`)
//	testData := []struct {
//		name         string
//		sql          string
//		affectedRows int64
//	}{
//		{name: "basic", sql: `DROP TABLE tbltest`, affectedRows: 1},
//		{name: "if_exists", sql: `DROP TABLE IF EXISTS tbltest`, affectedRows: 0},
//	}
//
//	for _, testCase := range testData {
//		t.Run(testCase.name, func(t *testing.T) {
//			execResult, err := db.Exec(testCase.sql)
//			if err != nil {
//				t.Fatalf("%s failed: %s", testName+"/"+testCase.name, err)
//			}
//			affectedRows, err := execResult.RowsAffected()
//			if err != nil {
//				t.Fatalf("%s failed: %s", testName+"/"+testCase.name+"/rows_affected", err)
//			}
//			if affectedRows != testCase.affectedRows {
//				t.Fatalf("%s failed: expected %#v affected-rows but received %#v", testName+"/"+testCase.name, testCase.affectedRows, affectedRows)
//			}
//		})
//	}
//}
//
//func Test_Exec_DescribeTable(t *testing.T) {
//	testName := "Test_Exec_DescribeTable"
//	db := _openDb(t, testName)
//	defer db.Close()
//
//	_, err := db.Exec("DESCRIBE TABLE tbltemp")
//	if err == nil || strings.Index(err.Error(), "not supported") < 0 {
//		t.Fatalf("%s failed: expected 'not support' error, but received %#v", testName, err)
//	}
//}
//
//func TestRowsDescribeTable_ColumnTypeDatabaseTypeName(t *testing.T) {
//	testName := "TestRowsDescribeTable_ColumnTypeDatabaseTypeName"
//	db := _openDb(t, testName)
//	defer db.Close()
//	_initTest(db)
//
//	expected := map[string]struct {
//		scanType reflect.Type
//		srcType  string
//	}{
//		"ArchivalSummary":           {srcType: "M", scanType: typeM},
//		"AttributeDefinitions":      {srcType: "L", scanType: typeL},
//		"BillingModeSummary":        {srcType: "M", scanType: typeM},
//		"CreationDateTime":          {srcType: "S", scanType: typeTime},
//		"DeletionProtectionEnabled": {srcType: "BOOL", scanType: typeBool},
//		"GlobalSecondaryIndexes":    {srcType: "L", scanType: typeL},
//		"GlobalTableVersion":        {srcType: "S", scanType: typeS},
//		"ItemCount":                 {srcType: "N", scanType: typeN},
//		"KeySchema":                 {srcType: "L", scanType: typeL},
//		"LatestStreamArn":           {srcType: "S", scanType: typeS},
//		"LatestStreamLabel":         {srcType: "S", scanType: typeS},
//		"LocalSecondaryIndexes":     {srcType: "L", scanType: typeL},
//		"ProvisionedThroughput":     {srcType: "M", scanType: typeM},
//		"Replicas":                  {srcType: "L", scanType: typeL},
//		"RestoreSummary":            {srcType: "M", scanType: typeM},
//		"SSEDescription":            {srcType: "M", scanType: typeM},
//		"StreamSpecification":       {srcType: "M", scanType: typeM},
//		"TableArn":                  {srcType: "S", scanType: typeS},
//		"TableClassSummary":         {srcType: "M", scanType: typeM},
//		"TableId":                   {srcType: "S", scanType: typeS},
//		"TableName":                 {srcType: "S", scanType: typeS},
//		"TableSizeBytes":            {srcType: "N", scanType: typeN},
//		"TableStatus":               {srcType: "S", scanType: typeS},
//	}
//
//	_, err := db.Exec(`CREATE TABLE tbltest WITH PK=id:string`)
//	if err != nil {
//		t.Fatalf("%s failed: %s", testName+"/createTable", err)
//	}
//	dbresult, err := db.Query(`DESCRIBE TABLE tbltest`)
//	if err != nil {
//		t.Fatalf("%s failed: %s", testName+"/describeTable", err)
//	}
//	cols, _ := dbresult.Columns()
//	colTypes, _ := dbresult.ColumnTypes()
//	for i, col := range cols {
//		srcType := colTypes[i].DatabaseTypeName()
//		if expected[col].srcType != srcType {
//			t.Fatalf("%s failed: expected column <%s> to be %s but received %s", testName, col, expected[col].srcType, srcType)
//		}
//		scanType := colTypes[i].ScanType()
//		if expected[col].scanType != scanType {
//			t.Fatalf("%s failed: expected column <%s> to be %s but received %s", testName, col, expected[col].scanType, scanType)
//		}
//	}
//}
