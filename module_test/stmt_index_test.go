package godynamo_test

import (
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/btnguyen2k/consu/reddo"
	"github.com/btnguyen2k/consu/semita"
)

func Test_Exec_DescribeLSI(t *testing.T) {
	testName := "Test_Exec_DescribeLSI"
	db := _openDb(t, testName)
	defer func() { _ = db.Close() }()

	_, err := db.Exec(fmt.Sprintf("DESCRIBE LSI idxname ON %s", tblTestTemp))
	if err == nil || strings.Index(err.Error(), "not supported") < 0 {
		t.Fatalf("%s failed: expected 'not support' error, but received %#v", testName, err)
	}
}

func Test_Query_DescribeLSI(t *testing.T) {
	testName := "Test_Query_DescribeLSI"
	db := _openDb(t, testName)
	_initTest(db)
	defer func() { _ = db.Close() }()

	_, err := db.Exec(fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s WITH PK=app:string WITH SK=user:string WITH LSI=idxtime:timestamp:number WITH LSI=idxbrowser:browser:string:* WITH LSI=idxos:os:string:os_name,os_version`, tblTestTemp))
	if err != nil {
		t.Fatalf("%s failed: %s", testName+"/createTable", err)
	}

	testData := []struct {
		name      string
		sql       string
		mustError bool
		numRows   int
		lsi       lsiInfo
	}{
		{name: "no_table", sql: fmt.Sprintf(`DESCRIBE LSI idx ON %s`, tblTestNotExist), mustError: true},
		{name: "no_index", sql: fmt.Sprintf(`DESCRIBE LSI idxnotexists ON %s`, tblTestTemp), numRows: 0},
		{name: "proj_key_only", sql: fmt.Sprintf(`DESCRIBE LSI idxtime ON %s`, tblTestTemp), numRows: 1, lsi: lsiInfo{projType: "KEYS_ONLY", lsiDef: lsiDef{indexName: "idxtime", attrName: "timestamp"}}},
		{name: "proj_all", sql: fmt.Sprintf(`DESCRIBE LSI idxbrowser ON %s`, tblTestTemp), numRows: 1, lsi: lsiInfo{projType: "ALL", lsiDef: lsiDef{indexName: "idxbrowser", attrName: "browser"}}},
		{name: "proj_included", sql: fmt.Sprintf(`DESCRIBE LSI idxos ON %s`, tblTestTemp), numRows: 1, lsi: lsiInfo{projType: "INCLUDE", lsiDef: lsiDef{indexName: "idxos", attrName: "os", projectedAttrs: "os_name,os_version"}}},
	}

	for _, testCase := range testData {
		t.Run(testCase.name, func(t *testing.T) {
			dbresult, err := db.Query(testCase.sql)
			if testCase.mustError && err == nil {
				t.Fatalf("%s failed: query must fail", testName+"/"+testCase.name)
			}
			if testCase.mustError {
				return
			}
			if err != nil {
				t.Fatalf("%s failed: %s", testName+"/"+testCase.name, err)
			}
			rows, err := _fetchAllRows(dbresult)
			if err != nil {
				t.Fatalf("%s failed: %s", testName+"/"+testCase.name, err)
			}
			if len(rows) != testCase.numRows {
				t.Fatalf("%s failed: expected %d row(s) but recelved %d", testName+"/"+testCase.name, testCase.numRows, len(rows))
			}
			if testCase.numRows > 0 {
				s := semita.NewSemita(rows[0])

				key := "IndexName"
				indexName, err := s.GetValueOfType(key, reddo.TypeString)
				if err != nil {
					t.Fatalf("%s failed: cannot fetch value at key <%s> / %s", testName+"/"+testCase.name, key, err)
				}
				if indexName != testCase.lsi.indexName {
					t.Fatalf("%s failed: expected value at key <%s> to be %#v but received %#v", testName+"/"+testCase.name, key, testCase.lsi.indexName, indexName)
				}

				key = "Projection.ProjectionType"
				projType, err := s.GetValueOfType(key, reddo.TypeString)
				if err != nil {
					t.Fatalf("%s failed: cannot fetch value at key <%s> / %s", testName+"/"+testCase.name, key, err)
				}
				if projType != testCase.lsi.projType {
					t.Fatalf("%s failed: expected value at key <%s> to be %#v but received %#v", testName+"/"+testCase.name, key, testCase.lsi.projType, projType)
				}

				if projType == "INCLUDE" {
					key = "Projection.NonKeyAttributes"
					nonKeyAttrs, err := s.GetValueOfType(key, reflect.TypeOf(make([]string, 0)))
					if err != nil {
						t.Fatalf("%s failed: cannot fetch value at key <%s> / %s", testName+"/"+testCase.name, key, err)
					}
					if !reflect.DeepEqual(nonKeyAttrs, strings.Split(testCase.lsi.projectedAttrs, ",")) {
						t.Fatalf("%s failed: expected value at key <%s> to be %#v but received %#v", testName+"/"+testCase.name, key, testCase.lsi.projectedAttrs, nonKeyAttrs)
					}
				}
			}
		})
	}
}

func Test_Query_CreateGSI(t *testing.T) {
	testName := "Test_Query_CreateGSI"
	db := _openDb(t, testName)
	defer func() { _ = db.Close() }()

	_, err := db.Query(fmt.Sprintf("CREATE GSI idx ON %s WITH pk=id:string", tblTestTemp))
	if err == nil || strings.Index(err.Error(), "not supported") < 0 {
		t.Fatalf("%s failed: expected 'not support' error, but received %#v", testName, err)
	}
}

func Test_Exec_CreateGSI(t *testing.T) {
	testName := "Test_Exec_CreateGSI"
	db := _openDb(t, testName)
	defer func() { _ = db.Close() }()
	_initTest(db)

	_, _ = db.Exec(fmt.Sprintf(`CREATE TABLE %s WITH pk=id:string WITH rcu=1 WITH wcu=1`, tblTestTemp))

	testData := []struct {
		name         string
		sql          string
		gsiInfo      *gsiInfo
		affectedRows int64
	}{
		{name: "basic", sql: fmt.Sprintf(`CREATE GSI index1 ON %s WITH PK=grade:number WITH wcu=1 WITH rcu=2`, tblTestTemp), affectedRows: 1,
			gsiInfo: &gsiInfo{indexName: "index1", wcu: 1, rcu: 2, pkAttr: "grade", pkType: "N", projectionType: "KEYS_ONLY"}},
		{name: "if_not_exists", sql: fmt.Sprintf(`CREATE GSI IF NOT EXISTS index1 ON %s WITH PK=id:string WITH wcu=2 WITH rcu=3 WITH projection=*`, tblTestTemp), affectedRows: 0,
			gsiInfo: &gsiInfo{indexName: "index1", wcu: 1, rcu: 2, pkAttr: "grade", pkType: "N", projectionType: "KEYS_ONLY"}},
		{name: "with_sk", sql: fmt.Sprintf(`CREATE GSI index2 ON %s WITH PK=grade:number WITH SK=class:string WITH wcu=3 WITH rcu=4 WITH projection=a,b,c`, tblTestTemp), affectedRows: 1,
			gsiInfo: &gsiInfo{indexName: "index2", wcu: 3, rcu: 4, pkAttr: "grade", pkType: "N", skAttr: "class", skType: "S", projectionType: "INCLUDE", projectedAttrs: "a,b,c"}},
		{name: "with_projection_all", sql: fmt.Sprintf(`CREATE GSI index3 ON %s WITH PK=grade:number WITH wcu=5 WITH rcu=6 WITH projection=*`, tblTestTemp), affectedRows: 1,
			gsiInfo: &gsiInfo{indexName: "index3", wcu: 5, rcu: 6, pkAttr: "grade", pkType: "N", projectionType: "ALL"}},
	}

	for _, testCase := range testData {
		t.Run(testCase.name, func(t *testing.T) {
			execResult, err := db.Exec(testCase.sql)
			if err != nil {
				t.Fatalf("%s failed: %s", testName+"/"+testCase.name+"/create_gsi", err)
			}
			affectedRows, err := execResult.RowsAffected()
			if err != nil {
				t.Fatalf("%s failed: %s", testName+"/"+testCase.name+"/rows_affected", err)
			}
			if affectedRows != testCase.affectedRows {
				t.Fatalf("%s failed: expected %#v affected-rows but received %#v", testName+"/"+testCase.name, testCase.affectedRows, affectedRows)
			}

			if testCase.gsiInfo == nil {
				return
			}
			dbresult, err := db.Query(fmt.Sprintf(`DESCRIBE GSI %s ON %s`, testCase.gsiInfo.indexName, tblTestTemp))
			if err != nil {
				t.Fatalf("%s failed: %s", testName+"/"+testCase.name+"/describe_gsi", err)
			}
			rows, err := _fetchAllRows(dbresult)
			if err != nil {
				t.Fatalf("%s failed: %s", testName+"/"+testCase.name+"/fetch_rows", err)
			}
			_verifyGSIInfo(t, testName+"/"+testCase.name, rows[0], testCase.gsiInfo)
		})
	}
}

func Test_Query_AlterGSI(t *testing.T) {
	testName := "Test_Query_AlterGSI"
	db := _openDb(t, testName)
	defer func() { _ = db.Close() }()

	_, err := db.Query(fmt.Sprintf("ALTER GSI idx ON %s WITH wcu=1 WITH rcu=2", tblTestTemp))
	if err == nil || strings.Index(err.Error(), "not supported") < 0 {
		t.Fatalf("%s failed: expected 'not support' error, but received %#v", testName, err)
	}
}

func Test_Exec_AlterGSI(t *testing.T) {
	testName := "Test_Exec_AlterGSI"
	db := _openDb(t, testName)
	defer func() { _ = db.Close() }()
	_initTest(db)

	_, _ = db.Exec(fmt.Sprintf(`CREATE TABLE %s WITH pk=id:string WITH rcu=1 WITH wcu=1`, tblTestTemp))
	_, _ = db.Exec(fmt.Sprintf(`CREATE GSI idxtest ON %s WITH pk=grade:number WITH rcu=3 WITH wcu=4`, tblTestTemp))
	time.Sleep(3 * time.Second)

	testData := []struct {
		name         string
		sql          string
		gsiInfo      *gsiInfo
		affectedRows int64
	}{
		{name: "basic", sql: fmt.Sprintf(`ALTER GSI idxtest ON %s WITH wcu=5 WITH rcu=6`, tblTestTemp), affectedRows: 1,
			gsiInfo: &gsiInfo{indexName: "idxtest", wcu: 5, rcu: 6, pkAttr: "grade", pkType: "N", projectionType: "KEYS_ONLY"}},
	}

	for _, testCase := range testData {
		t.Run(testCase.name, func(t *testing.T) {
			execResult, err := db.Exec(testCase.sql)
			if err != nil {
				t.Fatalf("%s failed: %s", testName+"/"+testCase.name+"/alter_gsi", err)
			}
			affectedRows, err := execResult.RowsAffected()
			if err != nil {
				t.Fatalf("%s failed: %s", testName+"/"+testCase.name+"/rows_affected", err)
			}
			if affectedRows != testCase.affectedRows {
				t.Fatalf("%s failed: expected %#v affected-rows but received %#v", testName+"/"+testCase.name, testCase.affectedRows, affectedRows)
			}

			if testCase.gsiInfo == nil {
				return
			}
			dbresult, err := db.Query(fmt.Sprintf(`DESCRIBE GSI %s ON %s`, testCase.gsiInfo.indexName, tblTestTemp))
			if err != nil {
				t.Fatalf("%s failed: %s", testName+"/"+testCase.name+"/describe_gsi", err)
			}
			rows, err := _fetchAllRows(dbresult)
			if err != nil {
				t.Fatalf("%s failed: %s", testName+"/"+testCase.name+"/fetch_rows", err)
			}
			_verifyGSIInfo(t, testName+"/"+testCase.name, rows[0], testCase.gsiInfo)
		})
	}
}

func Test_Exec_DescribeGSI(t *testing.T) {
	testName := "Test_Exec_DescribeGSI"
	db := _openDb(t, testName)
	defer func() { _ = db.Close() }()

	_, err := db.Exec(fmt.Sprintf("DESCRIBE GSI idxname ON %s", tblTestTemp))
	if err == nil || strings.Index(err.Error(), "not supported") < 0 {
		t.Fatalf("%s failed: expected 'not support' error, but received %#v", testName, err)
	}
}

func Test_Query_DescribeGSI(t *testing.T) {
	testName := "Test_Query_DescribeGSI"
	db := _openDb(t, testName)
	_initTest(db)
	defer func() { _ = db.Close() }()

	_, _ = db.Exec(fmt.Sprintf(`CREATE TABLE %s WITH pk=id:string WITH rcu=1 WITH wcu=2`, tblTestTemp))
	_, _ = db.Exec(fmt.Sprintf(`CREATE GSI idxtime ON %s WITH pk=time:number WITH rcu=3 WITH wcu=4`, tblTestTemp))
	_, _ = db.Exec(fmt.Sprintf(`CREATE GSI idxbrowser ON %s WITH pk=os:binary WITH SK=version:string WITH rcu=5 WITH wcu=6 WITH projection=*`, tblTestTemp))
	_, _ = db.Exec(fmt.Sprintf(`CREATE GSI idxplatform ON %s WITH pk=platform:string WITH rcu=7 WITH wcu=8 WITH projection=a,b,c`, tblTestTemp))
	time.Sleep(3 * time.Second)

	testData := []struct {
		name      string
		sql       string
		mustError bool
		numRows   int
		gsi       gsiInfo
	}{
		{name: "no_table", sql: fmt.Sprintf(`DESCRIBE GSI idxtest ON %s`, tblTestNotExist), mustError: true},
		{name: "no_index", sql: fmt.Sprintf(`DESCRIBE GSI idxnotexists ON %s`, tblTestTemp), numRows: 0},
		{name: "proj_key_only", sql: fmt.Sprintf(`DESCRIBE GSI idxtime ON %s`, tblTestTemp), numRows: 1,
			gsi: gsiInfo{indexName: "idxtime", rcu: 3, wcu: 4, pkAttr: "time", pkType: "N", projectionType: "KEYS_ONLY"}},
		{name: "proj_all", sql: fmt.Sprintf(`DESCRIBE GSI idxbrowser ON %s`, tblTestTemp), numRows: 1,
			gsi: gsiInfo{indexName: "idxbrowser", rcu: 5, wcu: 6, pkAttr: "os", pkType: "B", skAttr: "version", skType: "S", projectionType: "ALL"}},
		{name: "proj_include", sql: fmt.Sprintf(`DESCRIBE GSI idxplatform ON %s`, tblTestTemp), numRows: 1,
			gsi: gsiInfo{indexName: "idxplatform", rcu: 7, wcu: 8, pkAttr: "platform", pkType: "S", projectionType: "INCLUDE", projectedAttrs: "a,b,c"}},
	}

	for _, testCase := range testData {
		t.Run(testCase.name, func(t *testing.T) {
			dbresult, err := db.Query(testCase.sql)
			if testCase.mustError && err == nil {
				t.Fatalf("%s failed: query must fail", testName+"/"+testCase.name)
			}
			if testCase.mustError {
				return
			}
			if err != nil {
				t.Fatalf("%s failed: %s", testName+"/"+testCase.name, err)
			}
			rows, err := _fetchAllRows(dbresult)
			if err != nil {
				t.Fatalf("%s failed: %s", testName+"/"+testCase.name, err)
			}
			if len(rows) != testCase.numRows {
				t.Fatalf("%s failed: expected %d row(s) but recelved %d", testName+"/"+testCase.name, testCase.numRows, len(rows))
			}
			if testCase.numRows > 0 {
				s := semita.NewSemita(rows[0])

				key := "IndexName"
				indexName, err := s.GetValueOfType(key, reddo.TypeString)
				if err != nil {
					t.Fatalf("%s failed: cannot fetch value at key <%s> / %s", testName+"/"+testCase.name, key, err)
				}
				if indexName != testCase.gsi.indexName {
					t.Fatalf("%s failed: expected value at key <%s> to be %#v but received %#v", testName+"/"+testCase.name, key, testCase.gsi.indexName, indexName)
				}

				key = "Projection.ProjectionType"
				projectionType, err := s.GetValueOfType(key, reddo.TypeString)
				if err != nil {
					t.Fatalf("%s failed: cannot fetch value at key <%s> / %s", testName+"/"+testCase.name, key, err)
				}
				if projectionType != testCase.gsi.projectionType {
					t.Fatalf("%s failed: expected value at key <%s> to be %#v but received %#v", testName+"/"+testCase.name, key, testCase.gsi.projectionType, projectionType)
				}

				if projectionType == "INCLUDE" {
					key = "Projection.NonKeyAttributes"
					nonKeyAttrs, err := s.GetValueOfType(key, reflect.TypeOf(make([]string, 0)))
					if err != nil {
						t.Fatalf("%s failed: cannot fetch value at key <%s> / %s", testName+"/"+testCase.name, key, err)
					}
					if !reflect.DeepEqual(nonKeyAttrs, strings.Split(testCase.gsi.projectedAttrs, ",")) {
						t.Fatalf("%s failed: expected value at key <%s> to be %#v but received %#v", testName+"/"+testCase.name, key, testCase.gsi.projectedAttrs, nonKeyAttrs)
					}
				}
			}
		})
	}
}

func Test_Query_DropGSI(t *testing.T) {
	testName := "Test_Query_DropGSI"
	db := _openDb(t, testName)
	defer func() { _ = db.Close() }()

	_, err := db.Query(fmt.Sprintf("DROP GSI idxname ON %s", tblTestTemp))
	if err == nil || strings.Index(err.Error(), "not supported") < 0 {
		t.Fatalf("%s failed: expected 'not support' error, but received %#v", testName, err)
	}
}

func Test_Exec_DropGSI(t *testing.T) {
	testName := "Test_Exec_DropGSI"
	db := _openDb(t, testName)
	_initTest(db)
	defer func() { _ = db.Close() }()

	_, _ = db.Exec(fmt.Sprintf(`CREATE TABLE %s WITH pk=id:string WITH rcu=1 WITH wcu=2`, tblTestTemp))
	_, _ = db.Exec(fmt.Sprintf(`CREATE GSI idxtime ON %s WITH pk=time:number WITH rcu=3 WITH wcu=4`, tblTestTemp))
	_, _ = db.Exec(fmt.Sprintf(`CREATE GSI idxbrowser ON %s WITH pk=os:binary WITH SK=version:string WITH rcu=5 WITH wcu=6 WITH projection=*`, tblTestTemp))
	_, _ = db.Exec(fmt.Sprintf(`CREATE GSI idxplatform ON %s WITH pk=platform:string WITH rcu=7 WITH wcu=8 WITH projection=a,b,c`, tblTestTemp))
	time.Sleep(3 * time.Second)

	testData := []struct {
		name         string
		sql          string
		mustError    bool
		affectedRows int64
	}{
		{name: "no_table", sql: fmt.Sprintf(`DROP GSI idxtime ON %s`, tblTestNotExist), mustError: true},
		{name: "no_index", sql: fmt.Sprintf(`DROP GSI idxnotexists ON %s`, tblTestTemp), mustError: true},
		{name: "basic", sql: fmt.Sprintf(`DROP GSI idxtime ON %s`, tblTestTemp), affectedRows: 1},
		{name: "if_exists", sql: fmt.Sprintf(`DROP GSI IF EXISTS idxnotexists ON %s`, tblTestTemp), affectedRows: 0},
		{name: "no_table_if_exists", sql: fmt.Sprintf(`DROP GSI IF EXISTS idxtime ON %s`, tblTestNotExist), affectedRows: 0},
	}

	for _, testCase := range testData {
		t.Run(testCase.name, func(t *testing.T) {
			execResult, err := db.Exec(testCase.sql)
			if testCase.mustError && err == nil {
				t.Fatalf("%s failed: query must fail", testName+"/"+testCase.name)
			}
			if testCase.mustError {
				return
			}
			if err != nil {
				t.Fatalf("%s failed: %s", testName+"/"+testCase.name, err)
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

func TestRowsDescribeIndex_ColumnTypeDatabaseTypeName_LSI(t *testing.T) {
	testName := "TestRowsDescribeIndex_ColumnTypeDatabaseTypeName_LSI"
	db := _openDb(t, testName)
	_initTest(db)
	defer func() { _ = db.Close() }()

	expected := map[string]struct {
		scanType reflect.Type
		srcType  string
	}{
		"IndexName":      {srcType: "S", scanType: typeS},
		"KeySchema":      {srcType: "L", scanType: typeL},
		"Projection":     {srcType: "M", scanType: typeM},
		"IndexSizeBytes": {srcType: "N", scanType: typeN},
		"ItemCount":      {srcType: "N", scanType: typeN},
		"IndexArn":       {srcType: "S", scanType: typeS},
	}

	_, err := db.Exec(fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s WITH PK=app:string WITH SK=user:string WITH LSI=idxbrowser:browser:string:*`, tblTestTemp))
	if err != nil {
		t.Fatalf("%s failed: %s", testName+"/createTable", err)
	}
	dbresult, err := db.Query(fmt.Sprintf(`DESCRIBE LSI idxbrowser ON %s`, tblTestTemp))
	if err != nil {
		t.Fatalf("%s failed: %s", testName+"/describeLSI", err)
	}
	cols, _ := dbresult.Columns()
	colTypes, _ := dbresult.ColumnTypes()
	for i, col := range cols {
		srcType := colTypes[i].DatabaseTypeName()
		if expected[col].srcType != srcType {
			t.Fatalf("%s failed: expected column <%s> to be %s but received %s", testName, col, expected[col].srcType, srcType)
		}
		scanType := colTypes[i].ScanType()
		if expected[col].scanType != scanType {
			t.Fatalf("%s failed: expected column <%s> to be %s but received %s", testName, col, expected[col].scanType, scanType)
		}
	}
}

func TestRowsDescribeIndex_ColumnTypeDatabaseTypeName_GSI(t *testing.T) {
	testName := "TestRowsDescribeIndex_ColumnTypeDatabaseTypeName_GSI"
	db := _openDb(t, testName)
	_initTest(db)
	defer func() { _ = db.Close() }()

	expected := map[string]struct {
		scanType reflect.Type
		srcType  string
	}{
		"Backfilling":           {srcType: "BOOL", scanType: typeBool},
		"IndexArn":              {srcType: "S", scanType: typeS},
		"IndexName":             {srcType: "S", scanType: typeS},
		"IndexSizeBytes":        {srcType: "N", scanType: typeN},
		"IndexStatus":           {srcType: "S", scanType: typeS},
		"ItemCount":             {srcType: "N", scanType: typeN},
		"KeySchema":             {srcType: "L", scanType: typeL},
		"Projection":            {srcType: "M", scanType: typeM},
		"ProvisionedThroughput": {srcType: "M", scanType: typeM},
	}

	_, err := db.Exec(fmt.Sprintf(`CREATE TABLE %s WITH pk=id:string WITH rcu=1 WITH wcu=2`, tblTestTemp))
	if err != nil {
		t.Fatalf("%s failed: %s", testName+"/createTable", err)
	}
	_, err = db.Exec(fmt.Sprintf(`CREATE GSI idxbrowser ON %s WITH pk=os:binary WITH SK=version:string WITH rcu=5 WITH wcu=6 WITH projection=*`, tblTestTemp))
	if err != nil {
		t.Fatalf("%s failed: %s", testName+"/createGSI", err)
	}
	dbresult, err := db.Query(fmt.Sprintf(`DESCRIBE GSI idxbrowser ON %s`, tblTestTemp))
	if err != nil {
		t.Fatalf("%s failed: %s", testName+"/describeGSI", err)
	}
	cols, _ := dbresult.Columns()
	colTypes, _ := dbresult.ColumnTypes()
	for i, col := range cols {
		srcType := colTypes[i].DatabaseTypeName()
		if expected[col].srcType != srcType {
			t.Fatalf("%s failed: expected column <%s> to be %s but received %s", testName, col, expected[col].srcType, srcType)
		}
		scanType := colTypes[i].ScanType()
		if expected[col].scanType != scanType {
			t.Fatalf("%s failed: expected column <%s> to be %s but received %s", testName, col, expected[col].scanType, scanType)
		}
	}
}
