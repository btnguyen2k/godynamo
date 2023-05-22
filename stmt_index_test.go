package godynamo

import (
	"reflect"
	"strings"
	"testing"

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
	defer db.Close()

	defer func() {
		db.Exec("DROP TABLE IF EXISTS session")
	}()
	_, err := db.Exec(`CREATE TABLE IF NOT EXISTS session WITH PK=app:string WITH SK=user:string WITH LSI=idxtime:timestamp:number WITH LSI=idxbrowser:browser:string:* WITH LSI=idxos:os:string:os_name,os_version`)
	if err != nil {
		t.Fatalf("%s failed: %s", testName+"/createTable", err)
	}

	testData := []struct {
		name           string
		sql            string
		mustError      bool
		numRows        int
		indexName      string
		projectionType string
		nonKeyAttrs    []string
	}{
		{name: "no_table", sql: `DESCRIBE LSI idx ON tblnotexist`, mustError: true},
		{name: "no_index", sql: `DESCRIBE LSI idxnotexists ON session`, numRows: 0},
		{name: "proj_key_only", sql: `DESCRIBE LSI idxtime ON session`, numRows: 1, indexName: "idxtime", projectionType: "KEYS_ONLY"},
		{name: "proj_all", sql: `DESCRIBE LSI idxbrowser ON session`, numRows: 1, indexName: "idxbrowser", projectionType: "ALL"},
		{name: "proj_included", sql: `DESCRIBE LSI idxos ON session`, numRows: 1, indexName: "idxos", projectionType: "INCLUDE", nonKeyAttrs: []string{"os_name", "os_version"}},
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
				if indexName != testCase.indexName {
					t.Fatalf("%s failed: expected value at key <%s> to be %#v but received %#v", testName+"/"+testCase.name, key, testCase.indexName, indexName)
				}

				key = "Projection.ProjectionType"
				projectionType, err := s.GetValueOfType(key, reddo.TypeString)
				if err != nil {
					t.Fatalf("%s failed: cannot fetch value at key <%s> / %s", testName+"/"+testCase.name, key, err)
				}
				if projectionType != testCase.projectionType {
					t.Fatalf("%s failed: expected value at key <%s> to be %#v but received %#v", testName+"/"+testCase.name, key, testCase.projectionType, projectionType)
				}

				key = "Projection.NonKeyAttributes"
				nonKeyAttrs, err := s.GetValueOfType(key, reflect.TypeOf(make([]string, 0)))
				if err != nil {
					t.Fatalf("%s failed: cannot fetch value at key <%s> / %s", testName+"/"+testCase.name, key, err)
				}
				if testCase.nonKeyAttrs != nil && !reflect.DeepEqual(nonKeyAttrs, testCase.nonKeyAttrs) {
					t.Fatalf("%s failed: expected value at key <%s> to be %#v but received %#v", testName+"/"+testCase.name, key, testCase.nonKeyAttrs, nonKeyAttrs)
				}
			}
		})
	}
}
