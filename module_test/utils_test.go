package godynamo_test

import (
	"context"
	"fmt"
	"github.com/miyamo2/godynamo"
	"testing"
	"time"
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

func TestWaitForGSIStatus(t *testing.T) {
	testName := "TestWaitForGSIStatus"
	db := _openDb(t, testName)
	defer func() { _ = db.Close() }()
	_initTest(db)

	_, _ = db.Exec(fmt.Sprintf(`CREATE TABLE %s WITH pk=id:string WITH rcu=1 WITH wcu=1`, tblTestTemp))
	testData := []struct {
		name               string
		sql                string
		tableName, gsiName string
		statusList         []string
		mustError          bool
	}{
		{name: "create_gsi", sql: fmt.Sprintf(`CREATE GSI index1 ON %s WITH PK=grade:number WITH wcu=1 WITH rcu=2`, tblTestTemp),
			tableName: tblTestTemp, gsiName: "index1", statusList: []string{"ACTIVE"}},
		{name: "drop_gsi", sql: fmt.Sprintf(`DROP GSI index1 ON %s`, tblTestTemp),
			tableName: tblTestTemp, gsiName: "index1", statusList: []string{""}},
		{name: "not_exists", mustError: true, tableName: tblTestTemp, gsiName: "idxnotexist", statusList: []string{"ACTIVE"}},
	}
	for _, testCase := range testData {
		t.Run(testCase.name, func(t *testing.T) {
			if testCase.sql != "" {
				_, err := db.Exec(testCase.sql)
				if err != nil {
					t.Fatalf("%s failed: %s", testName+"/"+testCase.name+"/exec", err)
				}
			}
			ctx, cancelF := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancelF()
			err := godynamo.WaitForGSIStatus(ctx, db, testCase.tableName, testCase.gsiName, testCase.statusList, 100*time.Millisecond)
			if testCase.mustError && err == nil {
				t.Fatalf("%s failed: WaitForGSIStatus must fail", testName+"/"+testCase.name)
			}
			if testCase.mustError {
				fmt.Printf("[DEBUG] %T - %s\n", err, err)
				return
			}
			if err != nil {
				t.Fatalf("%s failed: %s", testName+"/"+testCase.name, err)
			}
		})
	}
}

func TestWaitForTableStatus(t *testing.T) {
	testName := "TestWaitForTableStatus"
	db := _openDb(t, testName)
	defer func() { _ = db.Close() }()
	_initTest(db)

	testData := []struct {
		name       string
		sql        string
		tableName  string
		statusList []string
		mustError  bool
	}{
		{name: "create_table", sql: fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s WITH pk=id:string WITH rcu=1 WITH wcu=1`, tblTestTemp),
			tableName: tblTestTemp, statusList: []string{"ACTIVE"}},
		{name: "drop_table", sql: fmt.Sprintf(`DROP TABLE IF EXISTS %s`, tblTestTemp),
			tableName: tblTestTemp, statusList: []string{""}},
		{name: "not_exists", mustError: true, tableName: tblTestTemp, statusList: []string{"ACTIVE"}},
	}
	for _, testCase := range testData {
		t.Run(testCase.name, func(t *testing.T) {
			if testCase.sql != "" {
				_, err := db.Exec(testCase.sql)
				if err != nil {
					t.Fatalf("%s failed: %s", testName+"/"+testCase.name+"/exec", err)
				}
			}
			ctx, cancelF := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancelF()
			err := godynamo.WaitForTableStatus(ctx, db, testCase.tableName, testCase.statusList, 100*time.Millisecond)
			if testCase.mustError && err == nil {
				t.Fatalf("%s failed: WaitForGSIStatus must fail", testName+"/"+testCase.name)
			}
			if testCase.mustError {
				fmt.Printf("[DEBUG] %T - %s\n", err, err)
				return
			}
			if err != nil {
				t.Fatalf("%s failed: %s", testName+"/"+testCase.name, err)
			}
		})
	}
}
