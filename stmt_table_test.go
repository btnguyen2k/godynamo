package godynamo

import (
	"strconv"
	"strings"
	"testing"
)

func Test_Query_CreateTable(t *testing.T) {
	testName := "Test_Query_CreateTable"
	db := _openDb(t, testName)
	defer db.Close()

	_, err := db.Query("CREATE TABLE tbltemp WITH pk=id:string")
	if err == nil || strings.Index(err.Error(), "not supported") < 0 {
		t.Fatalf("%s failed: expected 'not support' error, but received %#v", testName, err)
	}
}

func Test_Exec_CreateTable(t *testing.T) {
	testName := "Test_Exec_CreateTable"
	db := _openDb(t, testName)
	defer db.Close()
	_initTest(db)

	testData := []struct {
		name      string
		sql       string
		tableInfo *tableInfo
	}{
		{name: "basic", sql: `CREATE TABLE tbltest1 WITH PK=id:string`, tableInfo: &tableInfo{tableName: "tbltest1",
			billingMode: "PAY_PER_REQUEST", wcu: 0, rcu: 0, pkAttr: "id", pkType: "S"}},
		{name: "if_not_exists", sql: `CREATE TABLE IF NOT EXISTS tbltest1 WITH PK=id:NUMBER`, tableInfo: &tableInfo{tableName: "tbltest1",
			billingMode: "PAY_PER_REQUEST", wcu: 0, rcu: 0, pkAttr: "id", pkType: "S"}},
		{name: "with_sk", sql: `CREATE TABLE tbltest2 WITH PK=id:binary WITH sk=grade:number, WITH class=standard`, tableInfo: &tableInfo{tableName: "tbltest2",
			billingMode: "PAY_PER_REQUEST", wcu: 0, rcu: 0, pkAttr: "id", pkType: "B", skAttr: "grade", skType: "N"}},
		{name: "with_rcu_wcu", sql: `CREATE TABLE tbltest3 WITH PK=id:number WITH rcu=1 WITH wcu=2 WITH class=standard_ia`, tableInfo: &tableInfo{tableName: "tbltest3",
			billingMode: "", wcu: 2, rcu: 1, pkAttr: "id", pkType: "N"}},
		{name: "with_lsi", sql: `CREATE TABLE tbltest4 WITH PK=id:binary WITH SK=username:string WITH LSI=index1:grade:number, WITH LSI=index2:dob:string:*, WITH LSI=index3:yob:binary:a,b,c`, tableInfo: &tableInfo{tableName: "tbltest4",
			billingMode: "PAY_PER_REQUEST", wcu: 0, rcu: 0, pkAttr: "id", pkType: "B", skAttr: "username", skType: "S",
			lsi: map[string]lsiInfo{
				"index1": {projType: "KEYS_ONLY", lsiDef: lsiDef{indexName: "index1", attrName: "grade", attrType: "N"}},
				"index2": {projType: "ALL", lsiDef: lsiDef{indexName: "index2", attrName: "dob", attrType: "S"}},
				"index3": {projType: "INCLUDE", lsiDef: lsiDef{indexName: "index3", attrName: "yob", attrType: "B", projectedAttrs: "a,b,c"}},
			},
		}},
	}

	for _, testCase := range testData {
		t.Run(testCase.name, func(t *testing.T) {
			_, err := db.Exec(testCase.sql)
			if err != nil {
				t.Fatalf("%s failed: %s", testName+"/"+testCase.name+"/create_table", err)
			}

			if testCase.tableInfo == nil {
				return
			}
			dbresult, err := db.Query(`DESCRIBE TABLE ` + testCase.tableInfo.tableName)
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

func Test_Exec_ListTables(t *testing.T) {
	testName := "Test_Exec_ListTables"
	db := _openDb(t, testName)
	defer db.Close()

	_, err := db.Exec("LIST TABLES")
	if err == nil || strings.Index(err.Error(), "not supported") < 0 {
		t.Fatalf("%s failed: expected 'not support' error, but received %#v", testName, err)
	}
}

func Test_Query_ListTables(t *testing.T) {
	testName := "Test_Query_ListTables"
	db := _openDb(t, testName)
	_initTest(db)
	defer db.Close()

	tableList := []string{"tbltest2", "tbltest1", "tbltest3", "tbltest0"}
	for _, tbl := range tableList {
		db.Exec("CREATE TABLE " + tbl + " WITH PK=id:string")
	}

	dbresult, err := db.Query(`LIST TABLES`)
	if err != nil {
		t.Fatalf("%s failed: %s", testName+"/query", err)
	}
	rows, err := _fetchAllRows(dbresult)
	if err != nil {
		t.Fatalf("%s failed: %s", testName+"/fetch_rows", err)
	}
	if len(rows) != 4 {
		t.Fatalf("%s failed: expected 4 rows but received %d", testName+"/fetch_rows", len(rows))
	}
	for i := 0; i < 4; i++ {
		tblname := rows[i]["$1"].(string)
		if tblname != "tbltest"+strconv.Itoa(i) {
			t.Fatalf("%s failed: expected row #%d to be %#v but received %#v", testName+"/fetch_rows", i, "tbltemp"+strconv.Itoa(i), tblname)
		}
	}
}

func Test_Query_AlterTable(t *testing.T) {
	testName := "Test_Query_AlterTable"
	db := _openDb(t, testName)
	defer db.Close()

	_, err := db.Query("ALTER TABLE tbltemp WITH wcu=0 WITH rcu=0")
	if err == nil || strings.Index(err.Error(), "not supported") < 0 {
		t.Fatalf("%s failed: expected 'not support' error, but received %#v", testName, err)
	}
}

func Test_Exec_AlterTable(t *testing.T) {
	testName := "Test_Exec_AlterTable"
	db := _openDb(t, testName)
	_initTest(db)
	defer db.Close()

	db.Exec(`CREATE TABLE tbltest WITH PK=id:string`)
	testData := []struct {
		name      string
		sql       string
		tableInfo *tableInfo
	}{
		{name: "change_wcu_rcu", sql: `ALTER TABLE tbltest WITH wcu=3 WITH rcu=5`, tableInfo: &tableInfo{tableName: "tbltest",
			billingMode: "PROVISIONED", wcu: 3, rcu: 5, pkAttr: "id", pkType: "S"}},
		// DynamoDB Docker version does not support changing table class
	}

	for _, testCase := range testData {
		t.Run(testCase.name, func(t *testing.T) {
			_, err := db.Exec(testCase.sql)
			if err != nil {
				t.Fatalf("%s failed: %s", testName+"/"+testCase.name, err)
			}

			if testCase.tableInfo == nil {
				return
			}
			dbresult, err := db.Query(`DESCRIBE TABLE ` + testCase.tableInfo.tableName)
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
