package godynamo

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
)

func TestStmtDescribeLSI_parse(t *testing.T) {
	testName := "TestStmtDescribeLSI_parse"
	testData := []struct {
		name      string
		sql       string
		expected  *StmtDescribeLSI
		mustError bool
	}{
		{
			name:     "basic",
			sql:      "DESCRIBE LSI idxname ON tblname",
			expected: &StmtDescribeLSI{tableName: "tblname", indexName: "idxname"},
		},
		{
			name:      "no_table",
			sql:       "DESCRIBE LSI idxname",
			mustError: true,
		},
	}
	for _, testCase := range testData {
		t.Run(testCase.name, func(t *testing.T) {
			s, err := parseQuery(nil, testCase.sql)
			if testCase.mustError && err == nil {
				t.Fatalf("%s failed: parsing must fail", testName+"/"+testCase.name)
			}
			if testCase.mustError {
				return
			}
			if err != nil {
				t.Fatalf("%s failed: %s", testName+"/"+testCase.name, err)
			}
			stmt, ok := s.(*StmtDescribeLSI)
			if !ok {
				t.Fatalf("%s failed: expected StmtDescribeLSI but received %T", testName+"/"+testCase.name, stmt)
			}
			stmt.Stmt = nil
			if !reflect.DeepEqual(stmt, testCase.expected) {
				t.Fatalf("%s failed:\nexpected %#v\nreceived %#v", testName+"/"+testCase.name, testCase.expected, stmt)
			}
		})
	}
}

func TestStmtCreateGSI_parse(t *testing.T) {
	testName := "TestStmtCreateGSI_parse"
	testData := []struct {
		name      string
		sql       string
		expected  *StmtCreateGSI
		mustError bool
	}{
		{
			name:      "no_table",
			sql:       "CREATE GSI abc ON WITH pk=id:string",
			mustError: true,
		},
		{
			name:      "no_index_name",
			sql:       "CREATE GSI  ON table WITH pk=id:string",
			mustError: true,
		},
		{
			name:      "no_pk",
			sql:       "CREATE GSI index ON table",
			mustError: true,
		},
		{
			name:      "invalid_rcu",
			sql:       "CREATE GSI index ON table WITH pk=id:string WITH RCU=-1",
			mustError: true,
		},
		{
			name:      "invalid_wcu",
			sql:       "CREATE GSI index ON table WITH pk=id:string WITH wcu=-1",
			mustError: true,
		},
		{
			name:      "invalid_pk_type",
			sql:       "CREATE GSI index ON table WITH pk=id:int",
			mustError: true,
		},
		{
			name:      "invalid_sk_type",
			sql:       "CREATE GSI index ON table WITH pk=id:string WITH sk=grade:int",
			mustError: true,
		},

		{
			name:     "basic",
			sql:      "CREATE GSI index ON table WITH pk=id:string",
			expected: &StmtCreateGSI{tableName: "table", indexName: "index", pkName: "id", pkType: "STRING"},
		},
		{
			name:     "with_sk",
			sql:      "CREATE GSI index ON table WITH pk=id:string with SK=grade:binary WITH projection=*",
			expected: &StmtCreateGSI{tableName: "table", indexName: "index", pkName: "id", pkType: "STRING", skName: aws.String("grade"), skType: aws.String("BINARY"), projectedAttrs: "*"},
		},
		{
			name:     "with_rcu_wcu",
			sql:      "CREATE GSI IF NOT EXISTS index ON table WITH pk=id:number, with WCU=1 WITH rcu=0 WITH projection=a,b,c",
			expected: &StmtCreateGSI{tableName: "table", indexName: "index", ifNotExists: true, pkName: "id", pkType: "NUMBER", wcu: aws.Int64(1), rcu: aws.Int64(0), projectedAttrs: "a,b,c"},
		},
	}
	for _, testCase := range testData {
		t.Run(testCase.name, func(t *testing.T) {
			s, err := parseQuery(nil, testCase.sql)
			if testCase.mustError && err == nil {
				t.Fatalf("%s failed: parsing must fail", testName+"/"+testCase.name)
			}
			if testCase.mustError {
				return
			}
			if err != nil {
				fmt.Printf("[DEBUG] %s\n", testCase.sql)
				t.Fatalf("%s failed: %s", testName+"/"+testCase.name, err)
			}
			stmt, ok := s.(*StmtCreateGSI)
			if !ok {
				t.Fatalf("%s failed: expected StmtCreateGSI but received %T", testName+"/"+testCase.name, s)
			}
			stmt.Stmt = nil
			stmt.withOptsStr = ""
			if !reflect.DeepEqual(stmt, testCase.expected) {
				t.Fatalf("%s failed:\nexpected %#v\nreceived %#v", testName+"/"+testCase.name, testCase.expected, stmt)
			}
		})
	}
}

func TestStmtDescribeGSI_parse(t *testing.T) {
	testName := "TestStmtDescribeGSI_parse"
	testData := []struct {
		name      string
		sql       string
		expected  *StmtDescribeGSI
		mustError bool
	}{
		{
			name:     "basic",
			sql:      "DESCRIBE GSI idxname ON tblname",
			expected: &StmtDescribeGSI{tableName: "tblname", indexName: "idxname"},
		},
		{
			name:      "no_table",
			sql:       "DESCRIBE LSI idxname",
			mustError: true,
		},
	}
	for _, testCase := range testData {
		t.Run(testCase.name, func(t *testing.T) {
			s, err := parseQuery(nil, testCase.sql)
			if testCase.mustError && err == nil {
				t.Fatalf("%s failed: parsing must fail", testName+"/"+testCase.name)
			}
			if testCase.mustError {
				return
			}
			if err != nil {
				t.Fatalf("%s failed: %s", testName+"/"+testCase.name, err)
			}
			stmt, ok := s.(*StmtDescribeGSI)
			if !ok {
				t.Fatalf("%s failed: expected StmtDescribeGSI but received %T", testName+"/"+testCase.name, stmt)
			}
			stmt.Stmt = nil
			if !reflect.DeepEqual(stmt, testCase.expected) {
				t.Fatalf("%s failed:\nexpected %#v\nreceived %#v", testName+"/"+testCase.name, testCase.expected, stmt)
			}
		})
	}
}

func TestStmtAlterGSI_parse(t *testing.T) {
	testName := "TestStmtAlterGSI_parse"
	testData := []struct {
		name      string
		sql       string
		expected  *StmtAlterGSI
		mustError bool
	}{
		{
			name:      "no_table",
			sql:       "ALTER GSI abc ON  WITH wcu=1 WITH rcu=2",
			mustError: true,
		},
		{
			name:      "no_index_name",
			sql:       "ALTER GSI  ON table WITH wcu=1 WITH rcu=2",
			mustError: true,
		},
		{
			name:      "invalid_rcu",
			sql:       "ALTER GSI index ON table WITH RCU=-1",
			mustError: true,
		},
		{
			name:      "invalid_wcu",
			sql:       "ALTER GSI index ON table WITH wcu=-1",
			mustError: true,
		},

		{
			name:     "basic",
			sql:      "ALTER GSI index ON table WITH wcu=1 WITH rcu=2",
			expected: &StmtAlterGSI{tableName: "table", indexName: "index", wcu: aws.Int64(1), rcu: aws.Int64(2)},
		},
	}
	for _, testCase := range testData {
		t.Run(testCase.name, func(t *testing.T) {
			s, err := parseQuery(nil, testCase.sql)
			if testCase.mustError && err == nil {
				t.Fatalf("%s failed: parsing must fail", testName+"/"+testCase.name)
			}
			if testCase.mustError {
				return
			}
			if err != nil {
				t.Fatalf("%s failed: %s", testName+"/"+testCase.name, err)
			}
			stmt, ok := s.(*StmtAlterGSI)
			if !ok {
				t.Fatalf("%s failed: expected StmtAlterGSI but received %T", testName+"/"+testCase.name, s)
			}
			stmt.Stmt = nil
			stmt.withOptsStr = ""
			if !reflect.DeepEqual(stmt, testCase.expected) {
				t.Fatalf("%s failed:\nexpected %#v\nreceived %#v", testName+"/"+testCase.name, testCase.expected, stmt)
			}
		})
	}
}

func TestStmtDropGSI_parse(t *testing.T) {
	testName := "TestStmtDropGSI_parse"
	testData := []struct {
		name     string
		sql      string
		expected *StmtDropGSI
	}{
		{
			name:     "basic",
			sql:      "DROP GSI index ON table",
			expected: &StmtDropGSI{tableName: "table", indexName: "index"},
		},
		{
			name:     "if_exists",
			sql:      "DROP GSI IF EXISTS index ON table",
			expected: &StmtDropGSI{tableName: "table", indexName: "index", ifExists: true},
		},
	}
	for _, testCase := range testData {
		t.Run(testCase.name, func(t *testing.T) {
			s, err := parseQuery(nil, testCase.sql)
			if err != nil {
				t.Fatalf("%s failed: %s", testName+"/"+testCase.name, err)
			}
			stmt, ok := s.(*StmtDropGSI)
			if !ok {
				t.Fatalf("%s failed: expected StmtDropGSI but received %T", testName+"/"+testCase.name, s)
			}
			stmt.Stmt = nil
			if !reflect.DeepEqual(stmt, testCase.expected) {
				t.Fatalf("%s failed:\nexpected %#v\nreceived %#v", testName+"/"+testCase.name, testCase.expected, stmt)
			}
		})
	}
}
