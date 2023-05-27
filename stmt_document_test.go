package godynamo

import (
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/smithy-go"
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

func Test_Exec_Delete(t *testing.T) {
	testName := "Test_Exec_Delete"
	db := _openDb(t, testName)
	defer db.Close()
	_initTest(db)

	// setup table
	db.Exec(`DROP TABLE IF EXISTS tbltest`)
	db.Exec(`CREATE TABLE tbltest WITH PK=app:string WITH SK=user:string WITH rcu=100 WITH wcu=100`)
	_, err := db.Exec(`INSERT INTO "tbltest" VALUE {'app': ?, 'user': ?, 'os': ?, 'active': ?, 'duration': ?}`, "app0", "user1", "Ubuntu", true, 12.34)
	if err != nil {
		t.Fatalf("%s failed: %s", testName+"/insert", err)
	}
	_, err = db.Exec(`INSERT INTO "tbltest" VALUE {'app': ?, 'user': ?, 'platform': ?, 'location': ?}`, "app0", "user2", "Windows", "AU")
	if err != nil {
		t.Fatalf("%s failed: %s", testName+"/insert", err)
	}

	// make sure table has 2 rows
	dbRows1, _ := db.Query(`SELECT * FROM "tbltest"`)
	rows1, _ := _fetchAllRows(dbRows1)
	if len(rows1) != 2 {
		t.Fatalf("%s failed: expected 2 rows in table, but there is %#v", testName, len(rows1))
	}

	// delete 1 row
	sql := `DELETE FROM "tbltest" WHERE "app"=? AND "user"=?`
	result1, err := db.Exec(sql, "app0", "user1")
	if err != nil {
		t.Fatalf("%s failed: %s", testName+"/delete", err)
	}
	rowsAffected1, err := result1.RowsAffected()
	if err != nil {
		t.Fatalf("%s failed: %s", testName+"/rows_affected", err)
	}
	if rowsAffected1 != 1 {
		t.Fatalf("%s failed: expected 1 row affected but received %#v", testName+"/rows_affected", rowsAffected1)
	}

	// make sure table has 1 row
	dbRows2, _ := db.Query(`SELECT * FROM "tbltest"`)
	rows2, _ := _fetchAllRows(dbRows2)
	if len(rows2) != 1 {
		t.Fatalf("%s failed: expected 1 rows in table, but there is %#v", testName, len(rows1))
	}

	// delete 0 row
	result0, err := db.Exec(sql, "app0", "user0")
	if err != nil {
		t.Fatalf("%s failed: %s", testName+"/delete", err)
	}
	rowsAffected0, err := result0.RowsAffected()
	if err != nil {
		t.Fatalf("%s failed: %s", testName+"/rows_affected", err)
	}
	if rowsAffected0 != 0 {
		t.Fatalf("%s failed: expected 0 row affected but received %#v", testName+"/rows_affected", rowsAffected0)
	}
}

func Test_Query_Delete(t *testing.T) {
	testName := "Test_Query_Delete"
	db := _openDb(t, testName)
	defer db.Close()
	_initTest(db)

	// setup table
	db.Exec(`DROP TABLE IF EXISTS tbltest`)
	db.Exec(`CREATE TABLE tbltest WITH PK=app:string WITH SK=user:string WITH rcu=100 WITH wcu=100`)
	_, err := db.Exec(`INSERT INTO "tbltest" VALUE {'app': ?, 'user': ?, 'os': ?, 'active': ?, 'duration': ?}`, "app0", "user1", "Ubuntu", true, 12.34)
	if err != nil {
		t.Fatalf("%s failed: %s", testName+"/insert", err)
	}
	_, err = db.Exec(`INSERT INTO "tbltest" VALUE {'app': ?, 'user': ?, 'platform': ?, 'location': ?}`, "app0", "user2", "Windows", "AU")
	if err != nil {
		t.Fatalf("%s failed: %s", testName+"/insert", err)
	}

	// make sure table has 2 rows
	dbRows1, _ := db.Query(`SELECT * FROM "tbltest"`)
	rows1, _ := _fetchAllRows(dbRows1)
	if len(rows1) != 2 {
		t.Fatalf("%s failed: expected 2 rows in table, but there is %#v", testName, len(rows1))
	}

	// delete 1 row
	sql := `DELETE FROM "tbltest" WHERE "app"=? AND "user"=? RETURNING ALL OLD *`
	result1, err := db.Query(sql, "app0", "user1")
	if err != nil {
		t.Fatalf("%s failed: %s", testName+"/delete", err)
	}
	// the old row should be returned
	rows, err := _fetchAllRows(result1)
	if err != nil {
		t.Fatalf("%s failed: %s", testName+"/delete", err)
	}
	if len(rows) != 1 {
		t.Fatalf("%s failed: expected 1 row returned, but received %#v", testName+"/delete", len(rows))
	}
	expected := map[string]interface{}{"app": "app0", "user": "user1", "os": "Ubuntu", "active": true, "duration": float64(12.34)}
	if !reflect.DeepEqual(rows[0], expected) {
		t.Fatalf("%s failed:\nexpected     %#v\nbut received %#v", testName+"/delete", expected, rows[0])
	}

	// make sure table has 1 row
	dbRows2, _ := db.Query(`SELECT * FROM "tbltest"`)
	rows2, _ := _fetchAllRows(dbRows2)
	if len(rows2) != 1 {
		t.Fatalf("%s failed: expected 1 rows in table, but there is %#v", testName, len(rows1))
	}

	// delete 0 row
	result0, err := db.Query(sql, "app0", "user0")
	if err != nil {
		t.Fatalf("%s failed: %s", testName+"/delete", err)
	}
	rows, err = _fetchAllRows(result0)
	if err != nil {
		t.Fatalf("%s failed: %s", testName+"/delete", err)
	}
	if len(rows) != 0 {
		t.Fatalf("%s failed: expected 0 row returned, but received %#v", testName+"/delete", len(rows))
	}
}

func Test_Exec_Update(t *testing.T) {
	testName := "Test_Exec_Update"
	db := _openDb(t, testName)
	defer db.Close()
	_initTest(db)

	// setup table
	db.Exec(`DROP TABLE IF EXISTS tbltest`)
	db.Exec(`CREATE TABLE tbltest WITH PK=app:string WITH SK=user:string WITH rcu=100 WITH wcu=100`)
	_, err := db.Exec(`INSERT INTO "tbltest" VALUE {'app': ?, 'user': ?, 'platform': ?, 'location': ?}`, "app0", "user0", "Linux", "AU")
	if err != nil {
		t.Fatalf("%s failed: %s", testName+"/insert", err)
	}

	// update 1 row
	sql := `UPDATE "tbltest" SET location=? SET os=? WHERE "app"=? AND "user"=?`
	result1, err := db.Exec(sql, "VN", "Ubuntu", "app0", "user0")
	if err != nil {
		t.Fatalf("%s failed: %s", testName+"/update", err)
	}
	rowsAffected1, err := result1.RowsAffected()
	if err != nil {
		t.Fatalf("%s failed: %s", testName+"/rows_affected", err)
	}
	if rowsAffected1 != 1 {
		t.Fatalf("%s failed: expected 1 row affected but received %#v", testName+"/rows_affected", rowsAffected1)
	}

	// update 0 row
	result2, err := db.Exec(sql, "VN", "Ubuntu", "app0", "user2")
	if err != nil {
		t.Fatalf("%s failed: %s", testName+"/update", err)
	}
	rowsAffected2, err := result2.RowsAffected()
	if err != nil {
		t.Fatalf("%s failed: %s", testName+"/rows_affected", err)
	}
	if rowsAffected2 != 0 {
		t.Fatalf("%s failed: expected 0 row affected but received %#v", testName+"/rows_affected", rowsAffected2)
	}
}

func Test_Query_Update(t *testing.T) {
	testName := "Test_Query_Update"
	db := _openDb(t, testName)
	defer db.Close()
	_initTest(db)

	// setup table
	db.Exec(`DROP TABLE IF EXISTS tbltest`)
	db.Exec(`CREATE TABLE tbltest WITH PK=app:string WITH SK=user:string WITH rcu=100 WITH wcu=100`)
	_, err := db.Exec(`INSERT INTO "tbltest" VALUE {'app': ?, 'user': ?, 'platform': ?, 'location': ?}`, "app0", "user0", "Linux", "AU")
	if err != nil {
		t.Fatalf("%s failed: %s", testName+"/insert", err)
	}

	// update 1 row
	sql := `UPDATE "tbltest" SET location=? SET os=? WHERE "app"=? AND "user"=?`
	dbrows1, err := db.Query(sql+" RETURNING MODIFIED OLD *", "VN", "Ubuntu", "app0", "user0")
	if err != nil {
		t.Fatalf("%s failed: %s", testName+"/update", err)
	}
	// values of modified attributes should be returned
	rows1, _ := _fetchAllRows(dbrows1)
	if len(rows1) != 1 {
		t.Fatalf("%s failed: expected 1 affected row, but received %#v", testName, len(rows1))
	}
	expected := map[string]interface{}{"location": "AU"}
	if !reflect.DeepEqual(rows1[0], expected) {
		t.Fatalf("%s failed:\nexpected     %#v\nbut received %#v", testName+"/delete", expected, rows1[0])
	}

	dbrows2, err := db.Query(sql+" RETURNING ALL OLD *", "US", "Fedora", "app0", "user2")
	if err != nil {
		if aerr, ok := err.(*smithy.OperationError); ok {
			if herr, ok := aerr.Err.(*http.ResponseError); ok {
				fmt.Printf("DEBUG: %#v\n", herr.Err)
				fmt.Printf("DEBUG: %#v\n", reflect.TypeOf(herr.Err).Name())
				fmt.Printf("DEBUG: %#v\n", reflect.TypeOf(herr.Err).Elem().Name())
			}
		}
		t.Fatalf("%s failed: %s", testName+"/update", err)
	}
	rows2, _ := _fetchAllRows(dbrows2)
	if len(rows2) != 0 {
		t.Fatalf("%s failed: expected 0 affected row, but received %#v", testName, len(rows2))
	}
}
