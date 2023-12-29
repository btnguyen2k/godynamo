package godynamo_test

import (
	"github.com/btnguyen2k/godynamo"
	"testing"
)

func TestTransformInsertStmToPartiQL(t *testing.T) {
	testName := "TestTransformInsertStmToPartiQL"
	testData := []struct {
		name      string
		sql       string
		expected  string
		mustError bool
	}{
		{
			name:      "not_insert_stm",
			sql:       "SELECT * FROM table_name",
			mustError: true,
		},
		{
			name:      "fields_and_values_mismatch",
			sql:       "INSERT INTO table_name (f1,f2) VALUES (v1)",
			mustError: true,
		},
		{
			name:      "fields_and_values_mismatch_2",
			sql:       "INSERT INTO table_name (f1,f2) VALUES (?,1,true)",
			mustError: true,
		},
		{
			name:     "simple",
			sql:      `INSERT INTO table_name (f1, f2) VALUES (?, ?)`,
			expected: `INSERT INTO "table_name" VALUE {'f1': ?, 'f2': ?}`,
		},
		{
			name: "string_double_quoted",
			sql: `INSERT	INTO  table_name
		(f1	, f2   ,f3,		f4	)	  VALUES  	  ("str\ti\nng1" , "str''i''ng2"	, "str\"i\"ng3"    ,""  )`,
			expected: `INSERT INTO "table_name" VALUE {'f1': 'str\ti\nng1', 'f2': 'str''i''ng2', 'f3': 'str"i"ng3', 'f4': ''}`, // it's caller's responsibility to supply valid values
		},
		{
			name: "string_single_quoted",
			sql: `INSERT	INTO  table_name
		(
		f1	,
		 f2   ,f3
		,f4
		)	  VALUES  	  ('str\ti\nng1'
		,
			'str''i''ng2'
		, 'str"i"ng3',	''
		)`,
			expected: `INSERT INTO "table_name" VALUE {'f1': 'str\ti\nng1', 'f2': 'str''i''ng2', 'f3': 'str"i"ng3', 'f4': ''}`, // it's caller's responsibility to supply valid values
		},
		{
			name:     "number",
			sql:      `INSERT INTO table_name (f1, f2, f3, f4, f5) VALUES (1, -2.3, 4.5e-6, 7.8e+19, 1.2e+123)`,
			expected: `INSERT INTO "table_name" VALUE {'f1': 1, 'f2': -2.3, 'f3': 4.5e-06, 'f4': 7.8e+19, 'f5': 1.2e+123}`,
		},
		{
			name:     "boolean",
			sql:      `INSERT INTO table_name (f1, f2) VALUES (false, true)`,
			expected: `INSERT INTO "table_name" VALUE {'f1': false, 'f2': true}`,
		},
		{
			name:     "null",
			sql:      `INSERT INTO table_name (f0) VALUES (NuLL)`,
			expected: `INSERT INTO "table_name" VALUE {'f0': NULL}`,
		},
		{
			name:     "raw",
			sql:      `INSERT INTO table_name (f1,f2,f3) VALUES (val1,val2,val3)`,
			expected: `INSERT INTO "table_name" VALUE {'f1': val1, 'f2': val2, 'f3': val3}`,
		},
		{
			name:     "mixed",
			sql:      `INSERT INTO table_name (f1,f2,f3,f4,f5,f6,f7,f8,f9) VALUES (1,2.3e+40,-5.6,true,"false",NULL,'str''i''ng',?,val9)`,
			expected: `INSERT INTO "table_name" VALUE {'f1': 1, 'f2': 2.3e+40, 'f3': -5.6, 'f4': true, 'f5': 'false', 'f6': NULL, 'f7': 'str''i''ng', 'f8': ?, 'f9': val9}`,
		},
	}
	for _, testCase := range testData {
		t.Run(testCase.name, func(t *testing.T) {
			stmt, err := godynamo.TransformInsertStmToPartiQL(testCase.sql)
			if testCase.mustError && err == nil {
				t.Fatalf("%s failed: parsing must fail", testName+"/"+testCase.name)
			}
			if testCase.mustError {
				return
			}
			if err != nil {
				t.Fatalf("%s failed: %s", testName+"/"+testCase.name, err)
			}
			if testCase.expected != stmt {
				t.Fatalf("%s failed:\nexpected %s\nreceived %s", testName+"/"+testCase.name, testCase.expected, stmt)
			}
		})
	}
}
