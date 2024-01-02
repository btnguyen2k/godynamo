package godynamo_test

import (
	"fmt"
	"strings"
	"testing"
)

func Test_BigTable(t *testing.T) {
	testName := "Test_BigTable"
	db := _openDb(t, testName)
	defer func() { _ = db.Close() }()
	_initTest(db)

	if _, err := db.Exec(fmt.Sprintf(`CREATE TABLE %s WITH pk=id:string WITH rcu=3 WITH wcu=5`, tblTestTemp)); err != nil {
		t.Fatalf("%s failed: %s", testName+"/create_table", err)
	}
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
		params := []interface{}{row.id, row.dataChar,
			row.dataVchar, row.dataBinchar, row.dataText, row.dataUchar, row.dataUvchar, row.dataUtext,
			row.dataClob, row.dataUclob, row.dataBlob}
		_, err := db.Exec(sqlStm, params...)
		if err != nil {
			t.Fatalf("%s failed: %s", testName+"/insert", err)
		}
	}

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
}

func Test_BigTable_withWHERE(t *testing.T) {
	testName := "Test_BigTable_withWHERE"
	db := _openDb(t, testName)
	defer func() { _ = db.Close() }()
	_initTest(db)

	if _, err := db.Exec(fmt.Sprintf(`CREATE TABLE %s WITH pk=id:string WITH rcu=3 WITH wcu=5`, tblTestTemp)); err != nil {
		t.Fatalf("%s failed: %s", testName+"/create_table", err)
	}
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
		params := []interface{}{row.id, row.dataChar,
			row.dataVchar, row.dataBinchar, row.dataText, row.dataUchar, row.dataUvchar, row.dataUtext,
			row.dataClob, row.dataUclob, row.dataBlob}
		_, err := db.Exec(sqlStm, params...)
		if err != nil {
			t.Fatalf("%s failed: %s", testName+"/insert", err)
		}
	}

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
}

func Test_BigTable_withLIMIT(t *testing.T) {
	testName := "Test_BigTable_withLIMIT"
	db := _openDb(t, testName)
	defer func() { _ = db.Close() }()
	_initTest(db)

	if _, err := db.Exec(fmt.Sprintf(`CREATE TABLE %s WITH pk=id:string WITH rcu=3 WITH wcu=5`, tblTestTemp)); err != nil {
		t.Fatalf("%s failed: %s", testName+"/create_table", err)
	}
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
		params := []interface{}{row.id, row.dataChar,
			row.dataVchar, row.dataBinchar, row.dataText, row.dataUchar, row.dataUvchar, row.dataUtext,
			row.dataClob, row.dataUclob, row.dataBlob}
		_, err := db.Exec(sqlStm, params...)
		if err != nil {
			t.Fatalf("%s failed: %s", testName+"/insert", err)
		}
	}

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

//func Test_BigTable_withORDERBY(t *testing.T) {
//	testName := "Test_BigTable_withORDERBY"
//	db := _openDb(t, testName)
//	defer func() { _ = db.Close() }()
//	_initTest(db)
//
//	if _, err := db.Exec(fmt.Sprintf(`CREATE TABLE %s WITH pk=category:string WITH sk=id:string WITH rcu=11 WITH wcu=11`, tblTestTemp)); err != nil {
//		t.Fatalf("%s failed: %s", testName+"/create_table", err)
//	}
//	catList := []string{"PC", "Laptop", "Tablet", "Other"}
//	rand.Shuffle(len(catList), func(i, j int) { catList[i], catList[j] = catList[j], catList[i] })
//	catCount := map[string]int{"PC": 0, "Laptop": 0, "Tablet": 0, "Other": 0}
//	type Row struct {
//		id          string
//		category    string
//		dataChar    string
//		dataVchar   string
//		dataBinchar []byte
//		dataText    string
//		dataUchar   string
//		dataUvchar  string
//		dataUtext   string
//		dataClob    string
//		dataUclob   string
//		dataBlob    []byte
//	}
//	rowArr := make([]Row, 0)
//	numRows := 100
//	unicodeStr := "Chào buổi sáng, доброе утро, ສະ​ບາຍ​ດີ​ຕອນ​ເຊົ້າ, สวัสดีตอนเช้า"
//	unicodeStrLong := "Chào buổi sáng, đây sẽ là một đoạn văn bản dài. доброе утро, ສະ​ບາຍ​ດີ​ຕອນ​ເຊົ້າ, สวัสดีตอนเช้า"
//	sqlStm := `INSERT INTO "%s" VALUE {'id': ?, 'category': ?, 'dataChar': ?, 'dataVchar': ?, 'dataBinchar': ?, 'dataText': ?, 'dataUchar': ?, 'dataUvchar': ?, 'dataUtext': ?, 'dataClob': ?, 'dataUclob': ?, 'dataBlob': ?}`
//	sqlStm = fmt.Sprintf(sqlStm, tblTestTemp)
//	for i := 1; i < numRows; i++ {
//		id := fmt.Sprintf("%03d", i)
//		cat := catList[rand.Intn(len(catList))]
//		catCount[cat]++
//		row := Row{
//			id:          id,
//			category:    cat,
//			dataChar:    "CHAR " + id,
//			dataVchar:   "VCHAR " + id,
//			dataBinchar: []byte("BINCHAR " + id),
//			dataText:    strings.Repeat("This is supposed to be a long text ", i*2),
//			dataUchar:   unicodeStr,
//			dataUvchar:  unicodeStr,
//			dataUtext:   strings.Repeat(unicodeStr, i*2),
//			dataClob:    strings.Repeat("This is supposed to be a long text ", i*10),
//			dataUclob:   strings.Repeat(unicodeStrLong, i*10),
//			dataBlob:    []byte(strings.Repeat("This is supposed to be a long text ", i*10)),
//		}
//		rowArr = append(rowArr, row)
//		params := []interface{}{row.id, row.category,
//			row.dataChar, row.dataVchar, row.dataBinchar, row.dataText, row.dataUchar, row.dataUvchar, row.dataUtext,
//			row.dataClob, row.dataUclob, row.dataBlob}
//		_, err := db.Exec(sqlStm, params...)
//		if err != nil {
//			t.Fatalf("%s failed: %s", testName+"/insert", err)
//		}
//	}
//
//	for _, cat := range catList {
//		dbrows, err := db.Query(fmt.Sprintf(`SELECT * FROM %s WHERE category=? ORDER BY id DESC`, tblTestTemp), cat)
//		if err != nil {
//			t.Fatalf("%s failed: %s", testName+"/select", err)
//		}
//		rows, err := _fetchAllRows(dbrows)
//		if err != nil {
//			t.Fatalf("%s failed: %s", testName+"/fetchAllRows", err)
//		}
//		if catCount[cat] != len(rows) {
//			t.Fatalf("%s failed: expected %d rows but received %d", testName, catCount[cat], len(rows))
//		}
//		for i, row := range rows {
//			fmt.Printf("[DEBUG] %2d: %5s - %#v\n", i, row["category"], row["id"])
//			if row["category"] != cat {
//				t.Fatalf("%s failed: expected category %s but received %s", testName, cat, row["category"])
//			}
//			if i > 0 {
//				if row["id"].(string) > rows[i-1]["id"].(string) {
//					t.Fatalf("%s failed: expected id %s < %s", testName, row["id"], rows[i-1]["id"])
//				}
//			}
//		}
//	}
//}
