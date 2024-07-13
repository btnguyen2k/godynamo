package godynamo_test

import (
	"fmt"
	"github.com/aws/smithy-go"
	"github.com/miyamo2/godynamo"
	"reflect"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

func Test_Query_Insert(t *testing.T) {
	testName := "Test_Query_Insert"
	db := _openDb(t, testName)
	defer func() { _ = db.Close() }()

	_, err := db.Query(fmt.Sprintf(`INSERT INTO %s VALUE {}`, tblTestTemp))
	if err == nil || strings.Index(err.Error(), "not supported") < 0 {
		t.Fatalf("%s failed: expected 'not support' error, but received %#v", testName, err)
	}
}

func Test_Exec_Insert(t *testing.T) {
	testName := "Test_Exec_Insert"
	db := _openDb(t, testName)
	defer func() { _ = db.Close() }()
	_initTest(db)

	_, _ = db.Exec(fmt.Sprintf(`CREATE TABLE %s WITH pk=id:string WITH rcu=1 WITH wcu=1`, tblTestTemp))

	testData := []struct {
		name         string
		sql          string
		params       []interface{}
		affectedRows int64
	}{
		{name: "basic", sql: fmt.Sprintf(`INSERT INTO "%s" VALUE {'id': '1', 'name': 'User 1'}`, tblTestTemp), affectedRows: 1},
		{name: "parameterized", sql: fmt.Sprintf(`INSERT INTO "%s" VALUE {'id': ?, 'name': ?, 'active': ?, 'grade': ?, 'list': ?, 'map': ?}`, tblTestTemp), affectedRows: 1,
			params: []interface{}{"2", "User 2", true, 10, []interface{}{1.2, false, "3"}, map[string]interface{}{"N": -3.4, "B": false, "S": "3"}}},
	}

	for _, testCase := range testData {
		t.Run(testCase.name, func(t *testing.T) {
			execResult, err := db.Exec(testCase.sql, testCase.params...)
			if err != nil {
				t.Fatalf("%s failed: %s", testName+"/"+testCase.name+"/exec", err)
			}
			_, err = execResult.LastInsertId()
			if err == nil || strings.Index(err.Error(), "not supported") < 0 {
				t.Fatalf("%s failed: expected 'not support' error, but received %s", testName+"/"+testCase.name+"/last_insert_id", err)
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
	defer func() { _ = db.Close() }()

	_, err := db.Exec(fmt.Sprintf(`SELECT * FROM "%s" WHERE id='a'`, tblTestTemp))
	if err == nil || strings.Index(err.Error(), "not supported") < 0 {
		t.Fatalf("%s failed: expected 'not support' error, but received %#v", testName, err)
	}
}

func Test_Query_Select(t *testing.T) {
	testName := "Test_Query_Select"
	db := _openDb(t, testName)
	defer func() { _ = db.Close() }()
	_initTest(db)

	_, err := db.Exec(fmt.Sprintf(`CREATE TABLE %s WITH PK=app:string WITH SK=user:string WITH rcu=3 WITH wcu=3`, tblTestTemp))
	if err != nil {
		t.Fatalf("%s failed: %s", testName, err)
	}
	_, err = db.Exec(fmt.Sprintf(`INSERT INTO "%s" VALUE {'app': ?, 'user': ?, 'os': ?, 'active': ?, 'duration': ?}`, tblTestTemp), "app0", "user1", "Linux", true, 12.34)
	if err != nil {
		t.Fatalf("%s failed: %s", testName, err)
	}

	dbresult, err := db.Query(fmt.Sprintf(`SELECT * FROM "%s" WHERE app=?`, tblTestTemp), "app0")
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

func Test_Query_Select_withLimit(t *testing.T) {
	testName := "Test_Query_Select_withLimit"
	db := _openDb(t, testName)
	defer func() { _ = db.Close() }()
	_initTest(db)

	_, err := db.Exec(fmt.Sprintf(`CREATE TABLE %s WITH PK=app:string WITH SK=user:string WITH rcu=5 WITH wcu=5`, tblTestTemp))
	if err != nil {
		t.Fatalf("%s failed: %s", testName, err)
	}
	insData := [][]interface{}{
		{"app", "user1", "Linux", true, 1.0},
		{"app", "user2", "Windows", false, 2.3},
		{"app", "user3", "MacOS", true, 4.56},
	}
	for _, data := range insData {
		_, err = db.Exec(fmt.Sprintf(`INSERT INTO "%s" VALUE {'app': ?, 'user': ?, 'os': ?, 'active': ?, 'duration': ?}`, tblTestTemp), data...)
		if err != nil {
			t.Fatalf("%s failed: %s", testName+"/insert", err)
		}
	}

	dbresult, err := db.Query(fmt.Sprintf(`SELECT * FROM "%s" WHERE app=?`, tblTestTemp), "app")
	if err != nil {
		t.Fatalf("%s failed: %s", testName+"/select", err)
	}
	allRows, err := _fetchAllRows(dbresult)
	if err != nil {
		t.Fatalf("%s failed: %s", testName, err)
	}
	if len(allRows) != len(insData) {
		t.Fatalf("%s failed: expected %#v row but received %#v", testName+"/select", len(insData), len(allRows))
	}

	dbresult, err = db.Query(fmt.Sprintf(`SELECT * FROM "%s" WHERE app=? LIMIT 2`, tblTestTemp), "app")
	if err != nil {
		t.Fatalf("%s failed: %s", testName+"/select", err)
	}
	limitRows, err := _fetchAllRows(dbresult)
	if err != nil {
		t.Fatalf("%s failed: %s", testName, err)
	}
	if len(limitRows) != 2 {
		for i, row := range limitRows {
			fmt.Printf("%d: %#v\n", i, row)
		}
		t.Fatalf("%s failed: expected %#v row but received %#v", testName+"/select", 2, len(limitRows))
	}
}

func Test_Query_Select_with_columns_selection(t *testing.T) {
	testName := "Test_Query_Select_with_columns_selection"
	db := _openDb(t, testName)
	defer func() { _ = db.Close() }()
	_initTest(db)

	_, err := db.Exec(fmt.Sprintf(`CREATE TABLE %s WITH PK=app:string WITH SK=user:string WITH rcu=5 WITH wcu=5`, tblTestTemp))
	if err != nil {
		t.Fatalf("%s failed: %s", testName, err)
	}
	insData := [][]interface{}{
		{"app", "user1", "Linux", true, 1.0},
	}
	for _, data := range insData {
		_, err = db.Exec(fmt.Sprintf(`INSERT INTO "%s" VALUE {'app': ?, 'user': ?, 'os': ?, 'active': ?, 'duration': ?}`, tblTestTemp), data...)
		if err != nil {
			t.Fatalf("%s failed: %s", testName+"/insert", err)
		}
	}

	dbresult, err := db.Query(fmt.Sprintf(`SELECT * FROM "%s" WHERE app=?`, tblTestTemp), "app")
	if err != nil {
		t.Fatalf("%s failed: %s", testName+"/select", err)
	}
	allRows, err := _fetchAllRows(dbresult)
	if err != nil {
		t.Fatalf("%s failed: %s", testName, err)
	}
	if len(allRows) != len(insData) {
		t.Fatalf("%s failed: expected %#v row but received %#v", testName+"/select", len(insData), len(allRows))
	}

	dbresult, err = db.Query(fmt.Sprintf(`SELECT "duration", "app", "os", "active" FROM "%s" WHERE "app"=? AND "user"=?`, tblTestTemp), "app", "user1")
	if !dbresult.Next() {
		t.Fatalf("%s failed: %s", testName+"/select", err)
	}
	var (
		duration float64
		app, os  string
		active   bool
	)
	expected := []interface{}{1.0, "app", "Linux", true}
	_ = dbresult.Scan(&duration, &app, &os, &active)
	if !reflect.DeepEqual([]interface{}{duration, app, os, active}, expected) {
		t.Fatalf("%s failed: expected %#v but received %#v", testName+"/select", expected, []interface{}{duration, app, os, active})
	}
}

func Test_Exec_Delete(t *testing.T) {
	testName := "Test_Exec_Delete"
	db := _openDb(t, testName)
	defer func() { _ = db.Close() }()
	_initTest(db)

	// setup table
	_, _ = db.Exec(fmt.Sprintf(`DROP TABLE IF EXISTS %s`, tblTestTemp))
	_, _ = db.Exec(fmt.Sprintf(`CREATE TABLE %s WITH PK=app:string WITH SK=user:string WITH rcu=5 WITH wcu=5`, tblTestTemp))
	_, err := db.Exec(fmt.Sprintf(`INSERT INTO "%s" VALUE {'app': ?, 'user': ?, 'os': ?, 'active': ?, 'duration': ?}`, tblTestTemp), "app0", "user1", "Ubuntu", true, 12.34)
	if err != nil {
		t.Fatalf("%s failed: %s", testName+"/insert", err)
	}
	_, err = db.Exec(fmt.Sprintf(`INSERT INTO "%s" VALUE {'app': ?, 'user': ?, 'platform': ?, 'location': ?}`, tblTestTemp), "app0", "user2", "Windows", "AU")
	if err != nil {
		t.Fatalf("%s failed: %s", testName+"/insert", err)
	}

	// make sure table has 2 rows
	dbRows1, _ := db.Query(fmt.Sprintf(`SELECT * FROM "%s"`, tblTestTemp))
	rows1, _ := _fetchAllRows(dbRows1)
	if len(rows1) != 2 {
		t.Fatalf("%s failed: expected 2 rows in table, but there is %#v", testName, len(rows1))
	}

	// delete 1 row
	sql := fmt.Sprintf(`DELETE FROM "%s" WHERE "app"=? AND "user"=?`, tblTestTemp)
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
	dbRows2, _ := db.Query(fmt.Sprintf(`SELECT * FROM "%s"`, tblTestTemp))
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
	defer func() { _ = db.Close() }()
	_initTest(db)

	// setup table
	_, _ = db.Exec(fmt.Sprintf(`DROP TABLE IF EXISTS %s`, tblTestTemp))
	_, _ = db.Exec(fmt.Sprintf(`CREATE TABLE %s WITH PK=app:string WITH SK=user:string WITH rcu=5 WITH wcu=5`, tblTestTemp))
	_, err := db.Exec(fmt.Sprintf(`INSERT INTO "%s" VALUE {'app': ?, 'user': ?, 'os': ?, 'active': ?, 'duration': ?}`, tblTestTemp), "app0", "user1", "Ubuntu", true, 12.34)
	if err != nil {
		t.Fatalf("%s failed: %s", testName+"/insert", err)
	}
	_, err = db.Exec(fmt.Sprintf(`INSERT INTO "%s" VALUE {'app': ?, 'user': ?, 'platform': ?, 'location': ?}`, tblTestTemp), "app0", "user2", "Windows", "AU")
	if err != nil {
		t.Fatalf("%s failed: %s", testName+"/insert", err)
	}

	// make sure table has 2 rows
	dbRows1, _ := db.Query(fmt.Sprintf(`SELECT * FROM "%s"`, tblTestTemp))
	rows1, _ := _fetchAllRows(dbRows1)
	if len(rows1) != 2 {
		t.Fatalf("%s failed: expected 2 rows in table, but there is %#v", testName, len(rows1))
	}

	// delete 1 row
	sql := fmt.Sprintf(`DELETE FROM "%s" WHERE "app"=? AND "user"=? RETURNING ALL OLD *`, tblTestTemp)
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
	dbRows2, _ := db.Query(fmt.Sprintf(`SELECT * FROM "%s"`, tblTestTemp))
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
	defer func() { _ = db.Close() }()
	_initTest(db)

	// setup table
	_, _ = db.Exec(fmt.Sprintf(`DROP TABLE IF EXISTS %s`, tblTestTemp))
	_, _ = db.Exec(fmt.Sprintf(`CREATE TABLE %s WITH PK=app:string WITH SK=user:string WITH rcu=5 WITH wcu=5`, tblTestTemp))
	_, err := db.Exec(fmt.Sprintf(`INSERT INTO "%s" VALUE {'app': ?, 'user': ?, 'platform': ?, 'location': ?}`, tblTestTemp), "app0", "user0", "Linux", "AU")
	if err != nil {
		t.Fatalf("%s failed: %s", testName+"/insert", err)
	}

	// update 1 row
	sql := fmt.Sprintf(`UPDATE "%s" SET location=? SET os=? WHERE "app"=? AND "user"=?`, tblTestTemp)
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
	defer func() { _ = db.Close() }()
	_initTest(db)

	// setup table
	_, _ = db.Exec(fmt.Sprintf(`DROP TABLE IF EXISTS %s`, tblTestTemp))
	_, _ = db.Exec(fmt.Sprintf(`CREATE TABLE %s WITH PK=app:string WITH SK=user:string WITH rcu=5 WITH wcu=5`, tblTestTemp))
	_, err := db.Exec(fmt.Sprintf(`INSERT INTO "%s" VALUE {'app': ?, 'user': ?, 'platform': ?, 'location': ?}`, tblTestTemp), "app0", "user0", "Linux", "AU")
	if err != nil {
		t.Fatalf("%s failed: %s", testName+"/insert", err)
	}

	// update 1 row
	sql := fmt.Sprintf(`UPDATE "%s" SET location=? SET os=? WHERE "app"=? AND "user"=?`, tblTestTemp)
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

func TestResultResultSet_ColumnTypeDatabaseTypeName(t *testing.T) {
	testName := "TestResultResultSet_ColumnTypeDatabaseTypeName"
	db := _openDb(t, testName)
	defer func() { _ = db.Close() }()
	_initTest(db)

	_, _ = db.Exec(fmt.Sprintf(`CREATE TABLE %s WITH pk=id:string WITH rcu=1 WITH wcu=1`, tblTestTemp))
	testData := map[string]struct {
		val interface{}
		typ string
	}{
		"val_s":    {val: "a string", typ: "S"},
		"val_s_1":  {val: types.AttributeValueMemberS{Value: "a string"}, typ: "S"},
		"val_n":    {val: 123, typ: "N"},
		"val_n_1":  {val: types.AttributeValueMemberN{Value: "123.0"}, typ: "N"},
		"val_b":    {val: []byte("a binary"), typ: "B"},
		"val_b_1":  {val: types.AttributeValueMemberB{Value: []byte("a binary")}, typ: "B"},
		"val_ss":   {val: types.AttributeValueMemberSS{Value: []string{"a", "b", "c"}}, typ: "SS"},
		"val_ns":   {val: types.AttributeValueMemberNS{Value: []string{"1.2", "2.3", "3.4"}}, typ: "NS"},
		"val_bs":   {val: [][]byte{[]byte("a"), []byte("b"), []byte("c")}, typ: "BS"},
		"val_bs_1": {val: types.AttributeValueMemberBS{Value: [][]byte{[]byte("a"), []byte("b"), []byte("c")}}, typ: "BS"},
		"val_m":    {val: map[string]interface{}{"a": 1, "b": "2", "c": true, "d": []byte("4")}, typ: "M"},
		"val_m_1": {val: types.AttributeValueMemberM{Value: map[string]types.AttributeValue{
			"a": godynamo.ToAttributeValueUnsafe(1), "b": godynamo.ToAttributeValueUnsafe("2"), "c": godynamo.ToAttributeValueUnsafe(true), "d": godynamo.ToAttributeValueUnsafe([]byte("4"))},
		}, typ: "M"},
		"val_l": {val: []interface{}{1.2, "3", false, []byte("4")}, typ: "L"},
		"val_l_1": {val: types.AttributeValueMemberL{Value: []types.AttributeValue{
			godynamo.ToAttributeValueUnsafe(1.2), godynamo.ToAttributeValueUnsafe("3"), godynamo.ToAttributeValueUnsafe(false), godynamo.ToAttributeValueUnsafe([]byte("4"))},
		}, typ: "L"},
		"val_null":   {val: nil, typ: "NULL"},
		"val_null_1": {val: types.AttributeValueMemberNULL{Value: true}, typ: "NULL"},
		"val_bool":   {val: true, typ: "BOOL"},
		"val_bool_1": {val: types.AttributeValueMemberBOOL{Value: false}, typ: "BOOL"},
	}
	sql := fmt.Sprintf(`INSERT INTO "%s" VALUE {'id': 'myid'`, tblTestTemp)
	params := make([]interface{}, 0)
	for col, data := range testData {
		sql += fmt.Sprintf(", '%s': ?", col)
		params = append(params, data.val)
	}
	sql += "}"
	_, err := db.Exec(sql, params...)
	if err != nil {
		t.Fatalf("%s failed: %s", testName+"/insert", err)
	}

	dbrows, err := db.Query(fmt.Sprintf(`SELECT * FROM "%s" WHERE id=?`, tblTestTemp), "myid")
	if err != nil {
		t.Fatalf("%s failed: %s", testName+"/select", err)
	}
	cols, _ := dbrows.Columns()
	colTypes, _ := dbrows.ColumnTypes()
	for i, colType := range colTypes {
		col := cols[i]
		if col == "id" {
			continue
		}
		data := testData[col]
		if colType.DatabaseTypeName() != data.typ {
			t.Fatalf("%s failed: expected column <%s> to be type %s but received %s", testName+"/col_type", col, data.typ, colType.DatabaseTypeName())
		}
	}
}
