package godynamo

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
)

func TestSelectWithOptParsing(t *testing.T) {
	testCases := []struct {
		name string
		s    string
		e    map[string]OptStrings
	}{{
		name: "basic",
		s:    `SELECT * FROM "table"`,
		e:    map[string]OptStrings{},
	}, {
		name: "with read consistency",
		s:    `SELECT * FROM "table" WITH CONSISTENTREAD=strong`,
		e:    map[string]OptStrings{"CONSISTENTREAD": {"strong"}},
	},
		{
			name: "with read consistency and projection",
			s:    `SELECT * FROM "table" WITH CONSISTENTREAD=strong WITH PROJECTION=ALL`,
			e:    map[string]OptStrings{"CONSISTENTREAD": {"strong"}, "PROJECTION": {"ALL"}},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			s := &StmtExecutable{Stmt: &Stmt{query: tc.s}}
			err := s.parse()
			if err != nil {
				t.Fatalf("failed to parse: %s", err)
			}
			err = s.parseWithOpts(s.withOptString)
			if err != nil {
				t.Fatalf("failed to parse with options: %s", err)
			}
			if !reflect.DeepEqual(s.withOpts, tc.e) {
				t.Fatalf("expected %#v but received %#v", tc.e, s.withOpts)
			}
		})
	}

}

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
		{name: "limit value with opt", sql: `SELECT * FROM "table" LIMIT 1 WITH CONSTENCY=strong`, mustError: false, limit: aws.Int32(1), afterSql: `SELECT * FROM "table"`},
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

func Test_Stmt_Select_parse_placeholders(t *testing.T) {
	testName := "Test_Stmt_Select_parse_placeholders"
	testData := []struct {
		name            string
		sql             string
		numPlaceholders int
	}{
		{name: "basic", sql: `SELECT * FROM "table"`, numPlaceholders: 0},
		{name: "parameterized", sql: `SELECT * FROM "table" WHERE id=?`, numPlaceholders: 1},
		{name: "parameterized with space", sql: `SELECT * FROM "table" WHERE id = ?`, numPlaceholders: 1},
		{name: "parameterized with space and new line", sql: `SELECT * FROM "table" WHERE id = ?
		`, numPlaceholders: 1},
		{name: "multiple placeholders", sql: `SELECT "Category", "Name" FROM "Forum" WHERE ("Category" IS NULL OR "Category" = ? OR trim("Category") = ?)`, numPlaceholders: 2},
		{name: "not in string", sql: `SELECT * FROM "table" WHERE id = 'ab'+?+'cd'`, numPlaceholders: 1},
		{name: "not in string with prefix", sql: `SELECT * FROM "table" WHERE id = prefix+'ab'+?+"cd"`, numPlaceholders: 1},
		{name: "not in string with suffix", sql: `SELECT * FROM "table" WHERE id = "ab"+?+"cd"+suffix`, numPlaceholders: 1},
		{name: "in string - single quote", sql: `SELECT * FROM "table" WHERE id = 'ab?cd'`, numPlaceholders: 0},
		{name: "in string - double quote", sql: `SELECT * FROM "table" WHERE id = "ab?cd"`, numPlaceholders: 0},
		{name: "in string - double quote inside single quote", sql: `SELECT * FROM "table" WHERE id = 'ab"?"cd'`, numPlaceholders: 0},
		{name: "in string - single quote inside double quote", sql: `SELECT * FROM "table" WHERE id = "ab'?'cd"`, numPlaceholders: 0},
		{name: "in string with space - single quote", sql: `SELECT * FROM "table" WHERE id = 'ab? cd'`, numPlaceholders: 0},
		{name: "in string with space - double quote", sql: `SELECT * FROM "table" WHERE id = "ab? cd"`, numPlaceholders: 0},
		{name: "in string with space - double quote inside single quote", sql: `SELECT * FROM "table" WHERE id = 'ab"? "cd'`, numPlaceholders: 0},
		{name: "in string with space - single quote inside double quote", sql: `SELECT * FROM "table" WHERE id = "ab'? 'cd"`, numPlaceholders: 0},
		{name: "large number of placeholders", sql: `SELECT * FROM "table" WHERE id = ? AND name = ? AND age = ? AND active = ? AND grade = ? AND list = ? AND map = ?`, numPlaceholders: 7},
		{name: "placeholder inside sql functions", sql: `SELECT * FROM "table" WHERE id = trim(?)`, numPlaceholders: 1},
		{name: "placeholder inside sql functions with space", sql: `SELECT * FROM "table" WHERE id = trim( ? )`, numPlaceholders: 1},
	}

	for _, testCase := range testData {
		t.Run(testCase.name, func(t *testing.T) {
			stmt, err := parseQuery(nil, testCase.sql)
			if err != nil {
				t.Fatalf("%s failed: %s", testCase.name, err)
			}
			if stmt.NumInput() != testCase.numPlaceholders {
				fmt.Printf("[DEBUG] %s\n", testCase.sql)
				t.Fatalf("%s failed: expected %#v placeholders but received %#v", testName+"/"+testCase.name, testCase.numPlaceholders, stmt.NumInput())
			}
		})
	}
}
