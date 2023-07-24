package godynamo

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
)

func Test_Stmt_Select_parse(t *testing.T) {
	testName := "Test_Stmt_Select_parse"
	testData := []struct {
		name      string
		sql       string
		afterSql  string
		numInput  int
		limit     *int32
		mustError bool
	}{
		{name: "basic", sql: `SELECT * FROM "table"`, numInput: 0, afterSql: `SELECT * FROM "table"`},
		{name: "limit", sql: `SELECT * FROM "table" LIMIT 10`, numInput: 0, limit: aws.Int32(10), afterSql: `SELECT * FROM "table"`},
		{name: "limit with space", sql: `SELECT * FROM "table" LIMIT  10`, numInput: 0, limit: aws.Int32(10), afterSql: `SELECT * FROM "table"`},
		{name: "limit with space and new line", sql: `SELECT * FROM "table" LIMIT  10
`, numInput: 0, limit: aws.Int32(10), afterSql: `SELECT * FROM "table"`},
		{name: "parameterized", sql: `SELECT * FROM "table" WHERE id=?`, numInput: 1, afterSql: `SELECT * FROM "table" WHERE id=?`},
		{name: "parameterized with space", sql: `SELECT * FROM "table" WHERE id = ?`, numInput: 1, afterSql: `SELECT * FROM "table" WHERE id = ?`},
		{name: "parameterized with space and new line", sql: `SELECT * FROM "table" WHERE id = ?
`, numInput: 1, afterSql: `SELECT * FROM "table" WHERE id = ?`},

		{name: "invalid limit", sql: `SELECT * FROM "table" LIMIT a`, mustError: true},
		{name: "invalid limit value", sql: `SELECT * FROM "table" LIMIT -2`, mustError: true},
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
			stmt, ok := s.(*StmtSelect)
			if !ok {
				t.Fatalf("%s failed: expected StmtSelect but received %T", testName+"/"+testCase.name, stmt)
			}

			if stmt.numInput != testCase.numInput {
				t.Fatalf("%s failed: expected %#v input parameters but received %#v", testName+"/"+testCase.name, testCase.numInput, stmt.numInput)
			}
			if (testCase.limit == nil && stmt.limit != nil) || (testCase.limit != nil && (stmt.limit == nil || *testCase.limit != *stmt.limit)) {
				t.Fatalf("%s failed: expected %#v limit but received %#v", testName+"/"+testCase.name, testCase.limit, stmt.limit)
			}
			if stmt.query != testCase.afterSql {
				t.Fatalf("%s failed: expected %#v afterSql but received %#v", testName+"/"+testCase.name, testCase.afterSql, stmt.query)
			}
		})
	}
}
