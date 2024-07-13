package godynamo_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"github.com/miyamo2/godynamo"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"testing"
)

func TestTx_Empty(t *testing.T) {
	testName := "TestTx_Empty"
	db := _openDb(t, testName)
	defer func() { _ = db.Close() }()
	_initTest(db)

	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("%s failed: %s", testName+"/Begin", err)
	}
	err = tx.Commit()
	if err != nil {
		t.Fatalf("%s failed: %s", testName+"/Commit", err)
	}
}

func _txPrepareData(db *sql.DB, tableName string) error {
	_initTest(db)
	if _, err := db.Exec(fmt.Sprintf(`CREATE TABLE %s WITH pk=id:string WITH rcu=7 WITH wcu=7`, tableName)); err != nil {
		return err
	}
	for i := 1; i <= 6; i++ {
		if _, err := db.Exec(fmt.Sprintf(`INSERT INTO "%s" VALUE {'id': ?, 'grade': ?}`, tableName), strconv.Itoa(i), i*2); err != nil {
			return err
		}
	}
	return nil
}

func _txVerifyData(db *sql.DB, tableName string, expected []map[string]interface{}) error {
	dbresult, err := db.Query(fmt.Sprintf(`SELECT * FROM %s`, tableName))
	if err != nil {
		return err
	}
	rows, err := _fetchAllRows(dbresult)
	if err != nil {
		return err
	}
	sort.Slice(rows, func(i, j int) bool {
		return rows[i]["id"].(string) < rows[j]["id"].(string)
	})
	if len(rows) != len(expected) {
		return fmt.Errorf("expected %d rows but received %d", len(expected), len(rows))
	}
	for i, row := range rows {
		if !reflect.DeepEqual(row, expected[i]) {
			return fmt.Errorf("row #%d expected %#v but received %#v", i+1, expected[i], row)
		}
	}
	return nil
}

func TestTx_ConcurrentTrans(t *testing.T) {
	testName := "TestTx_ConcurrentTrans"
	db := _openDb(t, testName)
	defer func() { _ = db.Close() }()

	if err := _txPrepareData(db, tblTestTemp); err != nil {
		t.Fatalf("%s failed: %s", testName+"/prepare", err)
	}

	var tx1, tx2 *sql.Tx
	var err error
	conn, _ := db.Conn(context.Background())
	if tx1, err = conn.BeginTx(context.Background(), nil); err != nil {
		t.Fatalf("%s failed: %s", testName+"/tx1-begin", err)
	}
	if _, err = conn.BeginTx(context.Background(), nil); !errors.Is(err, godynamo.ErrInTx) {
		t.Fatalf("%s failed: expected %T but received %T", testName+"/tx2-begin", godynamo.ErrInTx, err)
	}
	if tx2, err = db.Begin(); err != nil {
		t.Fatalf("%s failed: %s", testName+"/tx3-begin", err)
	}

	if _, err = tx1.Exec(fmt.Sprintf(`UPDATE %s SET active=? WHERE id=?`, tblTestTemp), true, "1"); err != nil {
		t.Fatalf("%s failed: %s", testName+"/tx1-update", err)
	}
	if _, err = tx2.Exec(fmt.Sprintf(`UPDATE %s SET active=? WHERE id=?`, tblTestTemp), false, "3"); err != nil {
		t.Fatalf("%s failed: %s", testName+"/tx2-update", err)
	}
	if _, err = tx2.Exec(fmt.Sprintf(`DELETE FROM %s WHERE id=?`, tblTestTemp), "4"); err != nil {
		t.Fatalf("%s failed: %s", testName+"/tx2-delete", err)
	}
	if _, err = tx1.Exec(fmt.Sprintf(`DELETE FROM %s WHERE id=?`, tblTestTemp), "2"); err != nil {
		t.Fatalf("%s failed: %s", testName+"/tx1-delete", err)
	}
	if err = tx1.Commit(); err != nil {
		t.Fatalf("%s failed: %s", testName+"/tx1-commit", err)
	}
	if err = tx2.Rollback(); err != nil {
		t.Fatalf("%s failed: %s", testName+"/tx2-commit", err)
	}

	expected := []map[string]interface{}{
		{"id": "1", "grade": 2.0, "active": true},
		{"id": "3", "grade": 6.0, "active": nil},
		{"id": "4", "grade": 8.0, "active": nil},
		{"id": "5", "grade": 10.0, "active": nil},
		{"id": "6", "grade": 12.0, "active": nil},
	}
	if err = _txVerifyData(db, tblTestTemp, expected); err != nil {
		t.Fatalf("%s failed: %s", testName+"/verify", err)
	}
}

func TestTx_ConcurrentTxAndStmt(t *testing.T) {
	testName := "TestTx_ConcurrentTxAndStmt"
	db := _openDb(t, testName)
	defer func() { _ = db.Close() }()

	if err := _txPrepareData(db, tblTestTemp); err != nil {
		t.Fatalf("%s failed: %s", testName+"/prepare", err)
	}

	var tx *sql.Tx
	var err error
	conn, _ := db.Conn(context.Background())
	if tx, err = conn.BeginTx(context.Background(), nil); err != nil {
		t.Fatalf("%s failed: %s", testName+"/tx-begin", err)
	}

	if _, err = tx.Exec(fmt.Sprintf(`UPDATE %s SET active=? WHERE id=?`, tblTestTemp), true, "1"); err != nil {
		t.Fatalf("%s failed: %s", testName+"/tx-update", err)
	}
	if _, err = tx.Exec(fmt.Sprintf(`DELETE FROM %s WHERE id=?`, tblTestTemp), "2"); err != nil {
		t.Fatalf("%s failed: %s", testName+"/tx1-delete", err)
	}

	// Note: stmt is added to the existing transaction!
	if stmt, err := conn.PrepareContext(context.Background(), fmt.Sprintf(`UPDATE %s SET active=? WHERE id=?`, tblTestTemp)); err != nil {
		t.Fatalf("%s failed: %s", testName+"/conn-prepare-update", err)
	} else if dbresult, err := stmt.Exec(false, "3"); err != nil {
		t.Fatalf("%s failed: %s", testName+"/conn-exec-update", err)
	} else if rowsAffected, err := dbresult.RowsAffected(); !errors.Is(err, godynamo.ErrInTx) || rowsAffected != 0 {
		t.Fatalf("%s failed: expected %d rows affected and error %T but received %d rows affected and %T", testName+"/row-affected", 0, godynamo.ErrInTx, rowsAffected, err)
	}

	// Note: statement is added to the existing transaction!
	if dbresult, err := conn.ExecContext(context.Background(), fmt.Sprintf(`UPDATE %s SET grade=?, active=? WHERE id=?`, tblTestTemp), 0, false, "5"); err != nil {
		t.Fatalf("%s failed: %s", testName+"/db-update", err)
	} else if rowsAffected, err := dbresult.RowsAffected(); !errors.Is(err, godynamo.ErrInTx) || rowsAffected != 0 {
		t.Fatalf("%s failed: expected %d rows affected and error %T but received %d rows affected and %T", testName+"/row-affected", 0, godynamo.ErrInTx, rowsAffected, err)
	}

	if _, err = db.Exec(fmt.Sprintf(`DELETE FROM %s WHERE id=?`, tblTestTemp), "4"); err != nil {
		t.Fatalf("%s failed: %s", testName+"/db-delete", err)
	}
	if err = tx.Commit(); err != nil {
		t.Fatalf("%s failed: %s", testName+"/tx-commit", err)
	}

	expected := []map[string]interface{}{
		{"id": "1", "grade": 2.0, "active": true},
		{"id": "3", "grade": 6.0, "active": false},
		{"id": "5", "grade": 0.0, "active": false},
		{"id": "6", "grade": 12.0, "active": nil},
	}
	if err = _txVerifyData(db, tblTestTemp, expected); err != nil {
		t.Fatalf("%s failed: %s", testName+"/verify", err)
	}
}

func TestTx_Rollback(t *testing.T) {
	testName := "TestTx_Rollback"
	db := _openDb(t, testName)
	defer func() { _ = db.Close() }()

	if err := _txPrepareData(db, tblTestTemp); err != nil {
		t.Fatalf("%s failed: %s", testName+"/prepare", err)
	}

	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("%s failed: %s", testName+"/tx-begin", err)
	}
	if _, err = tx.Exec(fmt.Sprintf(`UPDATE %s SET active=? WHERE id=?`, tblTestTemp), true, "1"); err != nil {
		t.Fatalf("%s failed: %s", testName+"/tx-update", err)
	}
	if _, err = tx.Exec(fmt.Sprintf(`DELETE FROM %s WHERE id=?`, tblTestTemp), "2"); err != nil {
		t.Fatalf("%s failed: %s", testName+"/tx1-delete", err)
	}
	err = tx.Rollback()
	if err != nil {
		t.Fatalf("%s failed: %s", testName+"/tx-rollback", err)
	}
	expected := []map[string]interface{}{
		{"id": "1", "grade": 2.0},
		{"id": "2", "grade": 4.0},
		{"id": "3", "grade": 6.0},
		{"id": "4", "grade": 8.0},
		{"id": "5", "grade": 10.0},
		{"id": "6", "grade": 12.0},
	}
	if err = _txVerifyData(db, tblTestTemp, expected); err != nil {
		t.Fatalf("%s failed: %s", testName+"/verify", err)
	}
}

func TestTx_FailedCommit(t *testing.T) {
	testName := "TestTx_FailedCommit"
	db := _openDb(t, testName)
	defer func() { _ = db.Close() }()

	if err := _txPrepareData(db, tblTestTemp); err != nil {
		t.Fatalf("%s failed: %s", testName+"/prepare", err)
	}

	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("%s failed: %s", testName+"/tx-begin", err)
	}
	if _, err = tx.Exec(fmt.Sprintf(`INSERT INTO "%s" VALUE {'id': ?, 'active': ?}`, tblTestTemp), "1", true); err != nil {
		t.Fatalf("%s failed: %s", testName+"/tx-insert", err)
	}
	err = tx.Commit()
	if err == nil {
		t.Fatalf("%s failed: expecting failed commit", testName+"/tx-commit")
	}
	expected := []map[string]interface{}{
		{"id": "1", "grade": 2.0},
		{"id": "2", "grade": 4.0},
		{"id": "3", "grade": 6.0},
		{"id": "4", "grade": 8.0},
		{"id": "5", "grade": 10.0},
		{"id": "6", "grade": 12.0},
	}
	if err = _txVerifyData(db, tblTestTemp, expected); err != nil {
		t.Fatalf("%s failed: %s", testName+"/verify", err)
	}
}

func TestTx_Commit_Insert(t *testing.T) {
	testName := "TestTx_Commit_Insert"
	db := _openDb(t, testName)
	defer func() { _ = db.Close() }()
	_initTest(db)

	_, _ = db.Exec(fmt.Sprintf(`CREATE TABLE %s WITH pk=id:string WITH rcu=3 WITH wcu=3`, tblTestTemp))

	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("%s failed: %s", testName+"/tx-begin", err)
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

	if ra1, err := result1.RowsAffected(); err != nil || ra1 != 1 {
		t.Fatalf("%s failed: expected %d rows affected but received %d/%s", testName+"/row-affected-1", 1, ra1, err)
	} else if _, err = result1.LastInsertId(); err == nil || strings.Index(err.Error(), "not supported") < 0 {
		t.Fatalf("%s failed: expected 'not support' error, but received %s", testName+"/last-insert-id-1", err)
	}

	if ra2, err := result2.RowsAffected(); err != nil || ra2 != 1 {
		t.Fatalf("%s failed: expected %d rows affected but received %d/%s", testName+"/row-affected-2", 1, ra2, err)
	} else if _, err = result1.LastInsertId(); err == nil || strings.Index(err.Error(), "not supported") < 0 {
		t.Fatalf("%s failed: expected 'not support' error, but received %s", testName+"/last-insert-id-2", err)
	}

	expected := []map[string]interface{}{
		{"id": "1", "grade": nil, "active": true},
		{"id": "2", "grade": 2.0, "active": nil},
	}
	if err = _txVerifyData(db, tblTestTemp, expected); err != nil {
		t.Fatalf("%s failed: %s", testName+"/verify", err)
	}
}

func TestTx_Commit_UpdateDelete(t *testing.T) {
	testName := "TestTx_Commit_UpdateDelete"
	db := _openDb(t, testName)
	defer func() { _ = db.Close() }()

	if err := _txPrepareData(db, tblTestTemp); err != nil {
		t.Fatalf("%s failed: %s", testName+"/prepare", err)
	}

	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("%s failed: %s", testName+"/tx-begin", err)
	}
	result1, err1 := tx.Exec(fmt.Sprintf(`UPDATE "%s" SET duration=? WHERE "id"=?`, tblTestTemp), 1.2, "2")
	if err1 != nil {
		t.Fatalf("%s failed: %s", testName+"/tx-update", err)
	}
	result2, err2 := tx.Exec(fmt.Sprintf(`DELETE FROM "%s" WHERE "id"=?`, tblTestTemp), "1")
	if err2 != nil {
		t.Fatalf("%s failed: %s", testName+"/tx-update", err)
	}
	err = tx.Commit()
	if err != nil {
		t.Fatalf("%s failed: %s", testName+"/tx-commit", err)
	}

	if ra1, err := result1.RowsAffected(); err != nil || ra1 != 1 {
		t.Fatalf("%s failed: expected %d rows affected but received %d/%s", testName+"/row-affected-1", 1, ra1, err)
	} else if _, err = result1.LastInsertId(); err == nil || strings.Index(err.Error(), "not supported") < 0 {
		t.Fatalf("%s failed: expected 'not support' error, but received %s", testName+"/last-insert-id-1", err)
	}

	if ra2, err := result2.RowsAffected(); err != nil || ra2 != 1 {
		t.Fatalf("%s failed: expected %d rows affected but received %d/%s", testName+"/row-affected-2", 1, ra2, err)
	} else if _, err = result1.LastInsertId(); err == nil || strings.Index(err.Error(), "not supported") < 0 {
		t.Fatalf("%s failed: expected 'not support' error, but received %s", testName+"/last-insert-id-2", err)
	}

	expected := []map[string]interface{}{
		{"id": "2", "duration": 1.2, "grade": 4.0},
		{"id": "3", "duration": nil, "grade": 6.0},
		{"id": "4", "duration": nil, "grade": 8.0},
		{"id": "5", "duration": nil, "grade": 10.0},
		{"id": "6", "duration": nil, "grade": 12.0},
	}
	if err = _txVerifyData(db, tblTestTemp, expected); err != nil {
		t.Fatalf("%s failed: %s", testName+"/verify", err)
	}
}
