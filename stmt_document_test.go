package godynamo

import (
	"reflect"
	"strings"
	"testing"
)

func Test_Query_Insert(t *testing.T) {
	testName := "Test_Query_Insert"
	db := _openDb(t, testName)
	defer db.Close()

	_, err := db.Query("INSERT INTO table VALUE {}")
	if err == nil || strings.Index(err.Error(), "not supported") < 0 {
		t.Fatalf("%s failed: expected 'not support' error, but received %#v", testName, err)
	}
}

func Test_Exec_Insert(t *testing.T) {
	testName := "Test_Exec_Insert"
	db := _openDb(t, testName)
	defer db.Close()
	_initTest(db)

	db.Exec(`CREATE TABLE tbltest WITH pk=id:string WITH rcu=1 WITH wcu=1`)

	testData := []struct {
		name         string
		sql          string
		params       []interface{}
		affectedRows int64
	}{
		{name: "basic", sql: `INSERT INTO "tbltest" VALUE {'id': '1', 'name': 'User 1'}`, affectedRows: 1},
		{name: "parameterized", sql: `INSERT INTO "tbltest" VALUE {'id': ?, 'name': ?, 'active': ?, 'grade': ?, 'list': ?, 'map': ?}`, affectedRows: 1,
			params: []interface{}{"2", "User 2", true, 10, []interface{}{1.2, false, "3"}, map[string]interface{}{"N": -3.4, "B": false, "S": "3"}}},
	}

	for _, testCase := range testData {
		t.Run(testCase.name, func(t *testing.T) {
			execResult, err := db.Exec(testCase.sql, testCase.params...)
			if err != nil {
				t.Fatalf("%s failed: %s", testName+"/"+testCase.name+"/exec", err)
			}
			affectedRows, err := execResult.RowsAffected()
			if err != nil {
				t.Fatalf("%s failed: %s", testName+"/"+testCase.name+"/rows_affected", err)
			}
			if affectedRows != testCase.affectedRows {
				t.Fatalf("%s failed: expected %#v affected-rows but received %#v", testName+"/"+testCase.name, testCase.affectedRows, affectedRows)
			}
		})
	}
}

func Test_Exec_Select(t *testing.T) {
	testName := "Test_Exec_Select"
	db := _openDb(t, testName)
	defer db.Close()

	_, err := db.Exec(`SELECT * FROM "table" WHERE id='a'`)
	if err == nil || strings.Index(err.Error(), "not supported") < 0 {
		t.Fatalf("%s failed: expected 'not support' error, but received %#v", testName, err)
	}
}

func Test_Query_Select(t *testing.T) {
	testName := "Test_Query_Select"
	db := _openDb(t, testName)
	defer db.Close()
	_initTest(db)

	_, err := db.Exec(`CREATE TABLE tbltest WITH PK=app:string WITH SK=user:string WITH rcu=100 WITH wcu=100`)
	if err != nil {
		t.Fatalf("%s failed: %s", testName, err)
	}
	_, err = db.Exec(`INSERT INTO "tbltest" VALUE {'app': ?, 'user': ?, 'os': ?, 'active': ?, 'duration': ?}`, "app0", "user1", "Linux", true, 12.34)
	if err != nil {
		t.Fatalf("%s failed: %s", testName, err)
	}

	dbresult, err := db.Query(`SELECT * FROM "tbltest" WHERE app=?`, "app0")
	if err != nil {
		t.Fatalf("%s failed: %s", testName, err)
	}
	rows, err := _fetchAllRows(dbresult)
	if err != nil {
		t.Fatalf("%s failed: %s", testName, err)
	}
	if len(rows) != 1 {
		t.Fatalf("%s failed: expected 1 row but received %#v", testName, len(rows))
	}
	expectedRow := map[string]interface{}{
		"app":      "app0",
		"user":     "user1",
		"os":       "Linux",
		"active":   true,
		"duration": 12.34,
	}
	if !reflect.DeepEqual(rows[0], expectedRow) {
		t.Fatalf("%s failed:\nexpected %#v\nreceived %#v", testName, expectedRow, rows)
	}
}
