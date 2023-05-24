package godynamo

import (
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
	defer db.Close()

	_, err := db.Exec("DESCRIBE LSI idxname ON tblname")
	if err == nil || strings.Index(err.Error(), "not supported") < 0 {
		t.Fatalf("%s failed: expected 'not support' error, but received %#v", testName, err)
	}
}

func Test_Query_DescribeLSI(t *testing.T) {
	testName := "Test_Query_DescribeLSI"
	db := _openDb(t, testName)
	_initTest(db)
	defer db.Close()

	_, err := db.Exec(`CREATE TABLE IF NOT EXISTS tbltest WITH PK=app:string WITH SK=user:string WITH LSI=idxtime:timestamp:number WITH LSI=idxbrowser:browser:string:* WITH LSI=idxos:os:string:os_name,os_version`)
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
		{name: "no_table", sql: `DESCRIBE LSI idx ON tblnotexist`, mustError: true},
		{name: "no_index", sql: `DESCRIBE LSI idxnotexists ON tbltest`, numRows: 0},
		{name: "proj_key_only", sql: `DESCRIBE LSI idxtime ON tbltest`, numRows: 1, lsi: lsiInfo{projType: "KEYS_ONLY", lsiDef: lsiDef{indexName: "idxtime", attrName: "timestamp"}}},
		{name: "proj_all", sql: `DESCRIBE LSI idxbrowser ON tbltest`, numRows: 1, lsi: lsiInfo{projType: "ALL", lsiDef: lsiDef{indexName: "idxbrowser", attrName: "browser"}}},
		{name: "proj_included", sql: `DESCRIBE LSI idxos ON tbltest`, numRows: 1, lsi: lsiInfo{projType: "INCLUDE", lsiDef: lsiDef{indexName: "idxos", attrName: "os", projectedAttrs: "os_name,os_version"}}},
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
	defer db.Close()

	_, err := db.Query("CREATE GSI idx ON tbltemp WITH pk=id:string")
	if err == nil || strings.Index(err.Error(), "not supported") < 0 {
		t.Fatalf("%s failed: expected 'not support' error, but received %#v", testName, err)
	}
}

func Test_Exec_CreateGSI(t *testing.T) {
	testName := "Test_Exec_CreateGSI"
	db := _openDb(t, testName)
	defer db.Close()
	_initTest(db)

	db.Exec(`CREATE TABLE tbltest WITH pk=id:string WITH rcu=1 WITH wcu=1`)

	testData := []struct {
		name         string
		sql          string
		gsiInfo      *gsiInfo
		affectedRows int64
	}{
		{name: "basic", sql: `CREATE GSI index1 ON tbltest WITH PK=grade:number WITH wcu=1 WITH rcu=2`, affectedRows: 1, gsiInfo: &gsiInfo{indexName: "index1",
			wcu: 1, rcu: 2, pkAttr: "grade", pkType: "N", projectionType: "KEYS_ONLY"}},
		{name: "if_not_exists", sql: `CREATE GSI IF NOT EXISTS index1 ON tbltest WITH PK=id:string WITH wcu=2 WITH rcu=3 WITH projection=*`, affectedRows: 0, gsiInfo: &gsiInfo{indexName: "index1",
			wcu: 1, rcu: 2, pkAttr: "grade", pkType: "N", projectionType: "KEYS_ONLY"}},
		{name: "with_sk", sql: `CREATE GSI index2 ON tbltest WITH PK=grade:number WITH SK=class:string WITH wcu=3 WITH rcu=4 WITH projection=a,b,c`, affectedRows: 1, gsiInfo: &gsiInfo{indexName: "index2",
			wcu: 3, rcu: 4, pkAttr: "grade", pkType: "N", skAttr: "class", skType: "S", projectionType: "INCLUDE", projectedAttrs: "a,b,c"}},
		{name: "with_projection_all", sql: `CREATE GSI index3 ON tbltest WITH PK=grade:number WITH wcu=5 WITH rcu=6 WITH projection=*`, affectedRows: 1, gsiInfo: &gsiInfo{indexName: "index3",
			wcu: 5, rcu: 6, pkAttr: "grade", pkType: "N", projectionType: "ALL"}},
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
			dbresult, err := db.Query(`DESCRIBE GSI ` + testCase.gsiInfo.indexName + ` ON tbltest`)
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
	defer db.Close()

	_, err := db.Query("ALTER GSI idx ON tbltemp WITH wcu=1 WITH rcu=2")
	if err == nil || strings.Index(err.Error(), "not supported") < 0 {
		t.Fatalf("%s failed: expected 'not support' error, but received %#v", testName, err)
	}
}

func Test_Exec_AlterGSI(t *testing.T) {
	testName := "Test_Exec_AlterGSI"
	db := _openDb(t, testName)
	defer db.Close()
	_initTest(db)

	db.Exec(`CREATE TABLE tbltest WITH pk=id:string WITH rcu=1 WITH wcu=1`)
	db.Exec(`CREATE GSI idxtest ON tbltest WITH pk=grade:number WITH rcu=3 WITH wcu=4`)
	time.Sleep(3 * time.Second)

	testData := []struct {
		name         string
		sql          string
		gsiInfo      *gsiInfo
		affectedRows int64
	}{
		{name: "basic", sql: `ALTER GSI idxtest ON tbltest WITH wcu=5 WITH rcu=6`, affectedRows: 1, gsiInfo: &gsiInfo{indexName: "idxtest",
			wcu: 5, rcu: 6, pkAttr: "grade", pkType: "N", projectionType: "KEYS_ONLY"}},
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
			dbresult, err := db.Query(`DESCRIBE GSI ` + testCase.gsiInfo.indexName + ` ON tbltest`)
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
	defer db.Close()

	_, err := db.Exec("DESCRIBE GSI idxname ON tblname")
	if err == nil || strings.Index(err.Error(), "not supported") < 0 {
		t.Fatalf("%s failed: expected 'not support' error, but received %#v", testName, err)
	}
}

func Test_Query_DescribeGSI(t *testing.T) {
	testName := "Test_Query_DescribeGSI"
	db := _openDb(t, testName)
	_initTest(db)
	defer db.Close()

	db.Exec(`CREATE TABLE tbltest WITH pk=id:string WITH rcu=1 WITH wcu=2`)
	db.Exec(`CREATE GSI idxtime ON tbltest WITH pk=time:number WITH rcu=3 WITH wcu=4`)
	db.Exec(`CREATE GSI idxbrowser ON tbltest WITH pk=os:binary WITH SK=version:string WITH rcu=5 WITH wcu=6 WITH projection=*`)
	db.Exec(`CREATE GSI idxplatform ON tbltest WITH pk=platform:string WITH rcu=7 WITH wcu=8 WITH projection=a,b,c`)
	time.Sleep(3 * time.Second)

	testData := []struct {
		name      string
		sql       string
		mustError bool
		numRows   int
		gsi       gsiInfo
	}{
		{name: "no_table", sql: `DESCRIBE GSI idxtest ON tblnotexist`, mustError: true},
		{name: "no_index", sql: `DESCRIBE GSI idxnotexists ON tbltest`, numRows: 0},
		{name: "proj_key_only", sql: `DESCRIBE GSI idxtime ON tbltest`, numRows: 1, gsi: gsiInfo{indexName: "idxtime", rcu: 3, wcu: 4, pkAttr: "time", pkType: "N", projectionType: "KEYS_ONLY"}},
		{name: "proj_all", sql: `DESCRIBE GSI idxbrowser ON tbltest`, numRows: 1, gsi: gsiInfo{indexName: "idxbrowser", rcu: 5, wcu: 6, pkAttr: "os", pkType: "B", skAttr: "version", skType: "S", projectionType: "ALL"}},
		{name: "proj_include", sql: `DESCRIBE GSI idxplatform ON tbltest`, numRows: 1, gsi: gsiInfo{indexName: "idxplatform", rcu: 7, wcu: 8, pkAttr: "platform", pkType: "S", projectionType: "INCLUDE", projectedAttrs: "a,b,c"}},
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
	defer db.Close()

	_, err := db.Query("DROP GSI idxname ON tblname")
	if err == nil || strings.Index(err.Error(), "not supported") < 0 {
		t.Fatalf("%s failed: expected 'not support' error, but received %#v", testName, err)
	}
}

func Test_Exec_DropGSI(t *testing.T) {
	testName := "Test_Exec_DropGSI"
	db := _openDb(t, testName)
	_initTest(db)
	defer db.Close()

	db.Exec(`CREATE TABLE tbltest WITH pk=id:string WITH rcu=1 WITH wcu=2`)
	db.Exec(`CREATE GSI idxtime ON tbltest WITH pk=time:number WITH rcu=3 WITH wcu=4`)
	db.Exec(`CREATE GSI idxbrowser ON tbltest WITH pk=os:binary WITH SK=version:string WITH rcu=5 WITH wcu=6 WITH projection=*`)
	db.Exec(`CREATE GSI idxplatform ON tbltest WITH pk=platform:string WITH rcu=7 WITH wcu=8 WITH projection=a,b,c`)
	time.Sleep(3 * time.Second)

	testData := []struct {
		name         string
		sql          string
		mustError    bool
		affectedRows int64
	}{
		{name: "no_table", sql: `DROP GSI idxtime ON tblnotexist`, mustError: true},
		{name: "no_index", sql: `DROP GSI idxnotexists ON tbltest`, mustError: true},
		{name: "basic", sql: `DROP GSI idxtime ON tbltest`, affectedRows: 1},
		{name: "if_exists", sql: `DROP GSI IF EXISTS idxnotexists ON tbltest`, affectedRows: 0},
		{name: "no_table_if_exists", sql: `DROP GSI IF EXISTS idxtime ON tblnotexist`, affectedRows: 0},
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
