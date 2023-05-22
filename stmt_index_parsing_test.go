package godynamo

import (
	"reflect"
	"testing"
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
