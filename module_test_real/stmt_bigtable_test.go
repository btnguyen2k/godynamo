package godynamo_test

import (
	"fmt"
	"math/rand"
	"strings"
	"testing"
	"time"
)

func Test_BigTable(t *testing.T) {
	testName := "Test_BigTable"
	db := _openDb(t, testName)
	defer func() { _ = db.Close() }()
	//_cleanupTables(db)
	//
	//if _, err := db.Exec(fmt.Sprintf(`CREATE TABLE %s WITH pk=id:string WITH rcu=7 WITH wcu=20`, tblTestTemp)); err != nil {
	//	t.Fatalf("%s failed: %s", testName+"/create_table", err)
	//}
	//ctx, cancelF := context.WithTimeout(context.Background(), 10*time.Second)
	//defer cancelF()
	//err := godynamo.WaitForTableStatus(ctx, db, tblTestTemp, []string{"ACTIVE"}, 500*time.Millisecond)
	//if err != nil {
	//	t.Fatalf("%s failed: %s", testName+"/WaitForTableStatus", err)
	//}

	type Row struct {
		id          string
		dataChar    string
		dataVchar   string
		dataBinchar []byte
		dataText    string
		dataUchar   string
		dataUvchar  string
		dataUtext   string
		dataClob    string
		dataUclob   string
		dataBlob    []byte
	}
	rowArr := make([]Row, 0)
	numRows := 100
	unicodeStr := "Chào buổi sáng, доброе утро, ສະ​ບາຍ​ດີ​ຕອນ​ເຊົ້າ, สวัสดีตอนเช้า"
	unicodeStrLong := "Chào buổi sáng, đây sẽ là một đoạn văn bản dài. доброе утро, ສະ​ບາຍ​ດີ​ຕອນ​ເຊົ້າ, สวัสดีตอนเช้า"
	sqlStm := `INSERT INTO "%s" VALUE {'id': ?, 'dataChar': ?, 'dataVchar': ?, 'dataBinchar': ?, 'dataText': ?, 'dataUchar': ?, 'dataUvchar': ?, 'dataUtext': ?, 'dataClob': ?, 'dataUclob': ?, 'dataBlob': ?}`
	sqlStm = fmt.Sprintf(sqlStm, tblTestTemp)
	for i := 1; i < numRows; i++ {
		id := fmt.Sprintf("%03d", i)
		row := Row{
			id:          id,
			dataChar:    "CHAR " + id,
			dataVchar:   "VCHAR " + id,
			dataBinchar: []byte("BINCHAR " + id),
			dataText:    strings.Repeat("This is supposed to be a long text ", i*2),
			dataUchar:   unicodeStr,
			dataUvchar:  unicodeStr,
			dataUtext:   strings.Repeat(unicodeStr, i*2),
			dataClob:    strings.Repeat("This is supposed to be a long text ", i*10),
			dataUclob:   strings.Repeat(unicodeStrLong, i*10),
			dataBlob:    []byte(strings.Repeat("This is supposed to be a long text ", i*10)),
		}
		rowArr = append(rowArr, row)
		//params := []interface{}{row.id, row.dataChar,
		//	row.dataVchar, row.dataBinchar, row.dataText, row.dataUchar, row.dataUvchar, row.dataUtext,
		//	row.dataClob, row.dataUclob, row.dataBlob}
		//_, err := db.Exec(sqlStm, params...)
		//if err != nil {
		//	t.Fatalf("%s failed: %s", testName+"/insert", err)
		//}
		//time.Sleep(time.Duration(4000+rand.Int63n(2000)) * time.Millisecond)
		//fmt.Printf("[DEBUG] %v\n", i)
	}

	{
		dbrows, err := db.Query(fmt.Sprintf(`SELECT * FROM %s`, tblTestTemp))
		if err != nil {
			t.Fatalf("%s failed: %s", testName+"/select", err)
		}
		rows, err := _fetchAllRows(dbrows)
		if err != nil {
			t.Fatalf("%s failed: %s", testName+"/fetchAllRows", err)
		}
		if len(rows) != len(rowArr) {
			t.Fatalf("%s failed: expected %d rows but received %d", testName, len(rowArr), len(rows))
		}
		time.Sleep(time.Duration(5000+rand.Int63n(2000)) * time.Millisecond)
	}

	{
		dbrows, err := db.Query(fmt.Sprintf(`SELECT * FROM %s WHERE id>'012'`, tblTestTemp))
		if err != nil {
			t.Fatalf("%s failed: %s", testName+"/select", err)
		}
		rows, err := _fetchAllRows(dbrows)
		if err != nil {
			t.Fatalf("%s failed: %s", testName+"/fetchAllRows", err)
		}
		if len(rows) != len(rowArr)-12 {
			t.Fatalf("%s failed: expected %d rows but received %d", testName, len(rowArr)-12, len(rows))
		}
		time.Sleep(time.Duration(5000+rand.Int63n(2000)) * time.Millisecond)
	}

	{
		limit := 13
		dbrows, err := db.Query(fmt.Sprintf(`SELECT * FROM %s LIMIT %d`, tblTestTemp, limit))
		if err != nil {
			t.Fatalf("%s failed: %s", testName+"/select", err)
		}
		rows, err := _fetchAllRows(dbrows)
		if err != nil {
			t.Fatalf("%s failed: %s", testName+"/fetchAllRows", err)
		}
		if len(rows) != limit {
			t.Fatalf("%s failed: expected %d rows but received %d", testName, limit, len(rows))
		}
	}
}
