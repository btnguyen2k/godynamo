package godynamo

import (
	"reflect"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
)

func TestStmtCreateTable_parse(t *testing.T) {
	testName := "TestStmtCreateTable_parse"
	testData := []struct {
		name      string
		sql       string
		expected  *StmtCreateTable
		mustError bool
	}{
		{
			name:      "no_table",
			sql:       "CREATE TABLE  WITH pk=id:string",
			mustError: true,
		},
		{
			name:      "no_pk",
			sql:       "CREATE TABLE demo",
			mustError: true,
		},
		{
			name:      "invalid_rcu",
			sql:       "CREATE TABLE demo WITH pk=id:string WITH RCU=-1",
			mustError: true,
		},
		{
			name:      "invalid_wcu",
			sql:       "CREATE TABLE demo WITH pk=id:string WITH wcu=-1",
			mustError: true,
		},
		{
			name:      "invalid_pk_type",
			sql:       "CREATE TABLE demo WITH pk=id:int",
			mustError: true,
		},
		{
			name:      "invalid_sk_type",
			sql:       "CREATE TABLE demo WITH pk=id:string WITH sk=grade:int",
			mustError: true,
		},
		{
			name:      "invalid_table_class",
			sql:       "CREATE TABLE demo WITH pk=id:string WITH class=table_class",
			mustError: true,
		},
		{
			name:      "invalid_lsi_type",
			sql:       "CREATE TABLE demo WITH pk=id:string WITH LSI=idxname:attrname:float",
			mustError: true,
		},

		{
			name:     "basic",
			sql:      "CREATE TABLE demo WITH pk=id:string",
			expected: &StmtCreateTable{tableName: "demo", pkName: "id", pkType: "STRING"},
		},
		{
			name:     "with_sk",
			sql:      "CREATE TABLE demo WITH pk=id:string with SK=grade:binary",
			expected: &StmtCreateTable{tableName: "demo", pkName: "id", pkType: "STRING", skName: aws.String("grade"), skType: aws.String("BINARY")},
		},
		{
			name:     "with_rcu_wcu",
			sql:      "CREATE TABLE IF NOT EXISTS demo WITH pk=id:number, with WCU=1 WITH rcu=3",
			expected: &StmtCreateTable{tableName: "demo", ifNotExists: true, pkName: "id", pkType: "NUMBER", wcu: aws.Int64(1), rcu: aws.Int64(3)},
		},
		{
			name:     "with_table_class",
			sql:      "CREATE TABLE demo WITH pk=id:number, with WCU=1 WITH rcu=3, WITH class=STANDARD_ia",
			expected: &StmtCreateTable{tableName: "demo", pkName: "id", pkType: "NUMBER", wcu: aws.Int64(1), rcu: aws.Int64(3), tableClass: aws.String("STANDARD_IA")},
		},
		{
			name: "with_lsi",
			sql:  "CREATE TABLE IF NOT EXISTS demo WITH pk=id:number, with LSI=i1:f1:string, with LSI=i2:f2:number:*, , with LSI=i3:f3:binary:a,b,c",
			expected: &StmtCreateTable{tableName: "demo", ifNotExists: true, pkName: "id", pkType: "NUMBER", lsi: []lsiDef{
				{indexName: "i1", attrName: "f1", attrType: "STRING"},
				{indexName: "i2", attrName: "f2", attrType: "NUMBER", projectedAttrs: "*"},
				{indexName: "i3", attrName: "f3", attrType: "BINARY", projectedAttrs: "a,b,c"},
			}},
		},
	}
	for _, testCase := range testData {
		t.Run(testCase.name, func(t *testing.T) {
			stmt, err := parseQuery(nil, testCase.sql)
			if testCase.mustError && err == nil {
				t.Fatalf("%s failed: parsing must fail", testName+"/"+testCase.name)
			}
			if testCase.mustError {
				return
			}
			if err != nil {
				t.Fatalf("%s failed: %s", testName+"/"+testCase.name, err)
			}
			stmtCreateTable, ok := stmt.(*StmtCreateTable)
			if !ok {
				t.Fatalf("%s failed: expected StmtCreateTable but received %T", testName+"/"+testCase.name, stmt)
			}
			stmtCreateTable.Stmt = nil
			stmtCreateTable.withOptsStr = ""
			if !reflect.DeepEqual(stmtCreateTable, testCase.expected) {
				t.Fatalf("%s failed:\nexpected %#v\nreceived %#v", testName+"/"+testCase.name, testCase.expected, stmtCreateTable)
			}
		})
	}
}

func TestStmtListTables_parse(t *testing.T) {
	testName := "TestStmtListTables_parse"
	testData := []struct {
		name     string
		sql      string
		expected *StmtListTables
	}{
		{
			name:     "basic",
			sql:      "LIST TABLES",
			expected: &StmtListTables{},
		},
	}
	for _, testCase := range testData {
		t.Run(testCase.name, func(t *testing.T) {
			stmt, err := parseQuery(nil, testCase.sql)
			if err != nil {
				t.Fatalf("%s failed: %s", testName+"/"+testCase.name, err)
			}
			stmtListTables, ok := stmt.(*StmtListTables)
			if !ok {
				t.Fatalf("%s failed: expected StmtListTables but received %T", testName+"/"+testCase.name, stmt)
			}
			stmtListTables.Stmt = nil
			if !reflect.DeepEqual(stmtListTables, testCase.expected) {
				t.Fatalf("%s failed:\nexpected %#v\nreceived %#v", testName+"/"+testCase.name, testCase.expected, stmtListTables)
			}
		})
	}
}

func TestStmtAlterTable_parse(t *testing.T) {
	testName := "TestStmtAlterTable_parse"
	testData := []struct {
		name      string
		sql       string
		expected  *StmtAlterTable
		mustError bool
	}{
		{
			name:      "no_table",
			sql:       "ALTER TABLE  WITH wcu=1 WITH rcu=3",
			mustError: true,
		},
		{
			name:      "invalid_rcu",
			sql:       "ALTER TABLE demo WITH wcu=1 WITH rcu=-3",
			mustError: true,
		},
		{
			name:      "invalid_wcu",
			sql:       "ALTER TABLE demo WITH wcu=-1 WITH rcu=3",
			mustError: true,
		},
		{
			name:      "invalid_table_class",
			sql:       "ALTER TABLE demo WITH class=invalid",
			mustError: true,
		},

		{
			name:     "with_rcu_wcu",
			sql:      "ALTER TABLE demo WITH wcu=1 WITH rcu=3",
			expected: &StmtAlterTable{tableName: "demo", wcu: aws.Int64(1), rcu: aws.Int64(3)},
		},
		{
			name:     "with_table_class",
			sql:      "ALTER TABLE demo WITH CLASS=standard_IA",
			expected: &StmtAlterTable{tableName: "demo", tableClass: aws.String("STANDARD_IA")},
		},
	}
	for _, testCase := range testData {
		t.Run(testCase.name, func(t *testing.T) {
			stmt, err := parseQuery(nil, testCase.sql)
			if testCase.mustError && err == nil {
				t.Fatalf("%s failed: parsing must fail", testName+"/"+testCase.name)
			}
			if testCase.mustError {
				return
			}
			if err != nil {
				t.Fatalf("%s failed: %s", testName+"/"+testCase.name, err)
			}
			stmtAlterTable, ok := stmt.(*StmtAlterTable)
			if !ok {
				t.Fatalf("%s failed: expected StmtAlterTable but received %T", testName+"/"+testCase.name, stmt)
			}
			stmtAlterTable.Stmt = nil
			stmtAlterTable.withOptsStr = ""
			if !reflect.DeepEqual(stmtAlterTable, testCase.expected) {
				t.Fatalf("%s failed:\nexpected %#v\nreceived %#v", testName+"/"+testCase.name, testCase.expected, stmtAlterTable)
			}
		})
	}
}

func TestStmtDropTable_parse(t *testing.T) {
	testName := "TestStmtDropTable_parse"
	testData := []struct {
		name     string
		sql      string
		expected *StmtDropTable
	}{
		{
			name:     "basic",
			sql:      "DROP TABLE demo",
			expected: &StmtDropTable{tableName: "demo"},
		},
		{
			name:     "if_exists",
			sql:      "DROP TABLE IF EXISTS demo",
			expected: &StmtDropTable{tableName: "demo", ifExists: true},
		},
	}
	for _, testCase := range testData {
		t.Run(testCase.name, func(t *testing.T) {
			stmt, err := parseQuery(nil, testCase.sql)
			if err != nil {
				t.Fatalf("%s failed: %s", testName+"/"+testCase.name, err)
			}
			stmtDropTable, ok := stmt.(*StmtDropTable)
			if !ok {
				t.Fatalf("%s failed: expected StmtDropTable but received %T", testName+"/"+testCase.name, stmt)
			}
			stmtDropTable.Stmt = nil
			if !reflect.DeepEqual(stmtDropTable, testCase.expected) {
				t.Fatalf("%s failed:\nexpected %#v\nreceived %#v", testName+"/"+testCase.name, testCase.expected, stmtDropTable)
			}
		})
	}
}

func TestStmtDescribeTable_parse(t *testing.T) {
	testName := "TestStmtDescribeTable_parse"
	testData := []struct {
		name     string
		sql      string
		expected *StmtDescribeTable
	}{
		{
			name:     "basic",
			sql:      "DESCRIBE TABLE demo",
			expected: &StmtDescribeTable{tableName: "demo"},
		},
	}
	for _, testCase := range testData {
		t.Run(testCase.name, func(t *testing.T) {
			stmt, err := parseQuery(nil, testCase.sql)
			if err != nil {
				t.Fatalf("%s failed: %s", testName+"/"+testCase.name, err)
			}
			stmtDescribeTable, ok := stmt.(*StmtDescribeTable)
			if !ok {
				t.Fatalf("%s failed: expected StmtDescribeTable but received %T", testName+"/"+testCase.name, stmt)
			}
			stmtDescribeTable.Stmt = nil
			if !reflect.DeepEqual(stmtDescribeTable, testCase.expected) {
				t.Fatalf("%s failed:\nexpected %#v\nreceived %#v", testName+"/"+testCase.name, testCase.expected, stmtDescribeTable)
			}
		})
	}
}
