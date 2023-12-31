package godynamo_test

import (
	"fmt"
	"strings"
	"testing"
)

func TestTx_Rollback(t *testing.T) {
	testName := "TestTx_Rollback"
	db := _openDb(t, testName)
	defer db.Close()
	_initTest(db)

	db.Exec(fmt.Sprintf(`CREATE TABLE %s WITH pk=id:string WITH rcu=1 WITH wcu=1`, tblTestTemp))

	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("%s failed: %s", testName, err)
	}
	_, err = tx.Exec(fmt.Sprintf(`INSERT INTO "%s" VALUE {'id': ?, 'active': ?}`, tblTestTemp), "1", true)
	if err != nil {
		t.Fatalf("%s failed: %s", testName+"/tx-exec", err)
	}
	err = tx.Rollback()
	if err != nil {
		t.Fatalf("%s failed: %s", testName+"/tx-rollback", err)
	}

	dbresult, err := db.Query(fmt.Sprintf(`SELECT * FROM "%s"`, tblTestTemp))
	if err != nil {
		t.Fatalf("%s failed: %s", testName+"/query", err)
	}
	rows, err := _fetchAllRows(dbresult)
	if err != nil {
		t.Fatalf("%s failed: %s", testName+"/fetch", err)
	}
	if len(rows) != 0 {
		t.Fatalf("%s failed: expected 0 rows but received %#v", testName+"/fetch", len(rows))
	}
}

func TestTx_Commit_Insert(t *testing.T) {
	testName := "TestTx_Commit_Insert"
	db := _openDb(t, testName)
	defer db.Close()
	_initTest(db)

	db.Exec(fmt.Sprintf(`CREATE TABLE %s WITH pk=id:string WITH rcu=1 WITH wcu=1`, tblTestTemp))

	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("%s failed: %s", testName, err)
	}
	result1, err1 := tx.Exec(fmt.Sprintf(`INSERT INTO "%s" VALUE {'id': ?, 'active': ?}`, tblTestTemp), "1", true)
	if err1 != nil {
		t.Fatalf("%s failed: %s", testName+"/tx-exec", err)
	}
	result2, err2 := tx.Exec(fmt.Sprintf(`INSERT INTO "%s" VALUE {'id': ?, 'grade': ?}`, tblTestTemp), "2", 2)
	if err2 != nil {
		t.Fatalf("%s failed: %s", testName+"/tx-exec", err)
	}
	err = tx.Commit()
	if err != nil {
		t.Fatalf("%s failed: %s", testName+"/tx-commit", err)
	}

	ra1, err := result1.RowsAffected()
	if err != nil {
		t.Fatalf("%s failed: %s", testName+"/row_affected", err)
	}
	if ra1 != 1 {
		t.Fatalf("%s failed: expected row-affected to be 1 but received %#v", testName+"/row_affected", ra1)
	}
	_, err = result1.LastInsertId()
	if err == nil || strings.Index(err.Error(), "not supported") < 0 {
		t.Fatalf("%s failed: expected 'not support' error, but received %s", testName+"/last_insert_id", err)
	}

	ra2, err := result2.RowsAffected()
	if err != nil {
		t.Fatalf("%s failed: %s", testName+"/row_affected", err)
	}
	if ra2 != 1 {
		t.Fatalf("%s failed: expected row-affected to be 1 but received %#v", testName+"/row_affected", ra2)
	}
	_, err = result2.LastInsertId()
	if err == nil || strings.Index(err.Error(), "not supported") < 0 {
		t.Fatalf("%s failed: expected 'not support' error, but received %s", testName+"/last_insert_id", err)
	}

	dbresult, err := db.Query(fmt.Sprintf(`SELECT * FROM "%s"`, tblTestTemp))
	if err != nil {
		t.Fatalf("%s failed: %s", testName+"/query", err)
	}
	rows, err := _fetchAllRows(dbresult)
	if err != nil {
		t.Fatalf("%s failed: %s", testName+"/fetch", err)
	}
	if len(rows) != 2 {
		t.Fatalf("%s failed: expected 2 rows but received %#v", testName+"/fetch", len(rows))
	}
}

func TestTx_Commit_UpdateDelete(t *testing.T) {
	testName := "TestTx_Commit_UpdateDelete"
	db := _openDb(t, testName)
	defer db.Close()
	_initTest(db)

	db.Exec(fmt.Sprintf(`CREATE TABLE %s WITH pk=id:string WITH rcu=1 WITH wcu=1`, tblTestTemp))
	db.Exec(fmt.Sprintf(`INSERT INTO "%s" VALUE {'id': ?, 'active': ?}`, tblTestTemp), "1", true)
	db.Exec(fmt.Sprintf(`INSERT INTO "%s" VALUE {'id': ?, 'grade': ?}`, tblTestTemp), "2", 2)

	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("%s failed: %s", testName, err)
	}
	result1, err1 := tx.Exec(fmt.Sprintf(`UPDATE "%s" SET duration=? WHERE "id"=?`, tblTestTemp), 1.2, "2")
	if err1 != nil {
		t.Fatalf("%s failed: %s", testName+"/tx-exec", err)
	}
	result2, err2 := tx.Exec(fmt.Sprintf(`DELETE FROM "%s" WHERE "id"=?`, tblTestTemp), "1")
	if err2 != nil {
		t.Fatalf("%s failed: %s", testName+"/tx-exec", err)
	}
	err = tx.Commit()
	if err != nil {
		t.Fatalf("%s failed: %s", testName+"/tx-commit", err)
	}

	ra1, err := result1.RowsAffected()
	if err != nil {
		t.Fatalf("%s failed: %s", testName+"/row_affected", err)
	}
	if ra1 != 1 {
		t.Fatalf("%s failed: expected row-affected to be 1 but received %#v", testName+"/row_affected", ra1)
	}

	ra2, err := result2.RowsAffected()
	if err != nil {
		t.Fatalf("%s failed: %s", testName+"/row_affected", err)
	}
	if ra2 != 1 {
		t.Fatalf("%s failed: expected row-affected to be 1 but received %#v", testName+"/row_affected", ra2)
	}

	dbresult, err := db.Query(fmt.Sprintf(`SELECT * FROM "%s"`, tblTestTemp))
	if err != nil {
		t.Fatalf("%s failed: %s", testName+"/query", err)
	}
	rows, err := _fetchAllRows(dbresult)
	if err != nil {
		t.Fatalf("%s failed: %s", testName+"/fetch", err)
	}
	if len(rows) != 1 {
		t.Fatalf("%s failed: expected 1 rows but received %#v", testName+"/fetch", len(rows))
	}
	if rows[0]["id"] != "2" {
		t.Fatalf("%s failed: expected row #2 but received %#v", testName+"/fetch", rows[0])
	}
}
