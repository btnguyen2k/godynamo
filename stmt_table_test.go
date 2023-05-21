package godynamo

import (
	"strings"
	"testing"
)

func Test_Query_CreateTable(t *testing.T) {
	testName := "Test_Query_CreateTable"
	db := _openDb(t, testName)
	defer db.Close()

	_, err := db.Query("CREATE TABLE tbltemp WITH pk=id:string")
	if err == nil || strings.Index(err.Error(), "not supported") < 0 {
		t.Fatalf("%s failed: expected 'not support' error, but received %#v", testName, err)
	}
}

func Test_Exec_CreateTable(t *testing.T) {
	testName := "Test_Exec_CreateTable"
	db := _openDb(t, testName)
	defer db.Close()

	tableList := []string{"tbltemp1", "tbltemp2", "tbltemp3", "tbltemp4"}
	defer func() {
		for _, tbl := range tableList {
			db.Exec("DROP TABLE IF EXISTS " + tbl)
		}
	}()

	testData := []struct {
		name string
		sql  string
	}{
		{name: "basic", sql: `CREATE TABLE tbltemp1 WITH PK=id:string`},
		{name: "if_not_exists", sql: `CREATE TABLE IF NOT EXISTS tbltemp1 WITH PK=id:string`},
		{name: "with_sk", sql: `CREATE TABLE tbltemp2 WITH PK=id:string WITH sk=grade:number, WITH class=standard`},
		{name: "with_rcu_wcu", sql: `CREATE TABLE tbltemp3 WITH PK=id:number WITH rcu=1 WITH wcu=2 WITH class=standard_ia`},
		{name: "with_lsi", sql: `CREATE TABLE tbltemp4 WITH PK=id:binary WITH SK=username:string WITH LSI=index1:grade:number, WITH LSI=index2:dob:string:*, WITH LSI=index3:yob:binary:a,b,c`},
	}

	for _, testCase := range testData {
		t.Run(testCase.name, func(t *testing.T) {
			_, err := db.Exec(testCase.sql)
			if err != nil {
				t.Fatalf("%s failed: %s", testName+"/"+testCase.name, err)
			}
		})
	}
}
