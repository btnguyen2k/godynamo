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

	tableList := []string{"tbltemp1", "tbltemp2", "tbltemp3"}
	defer func() {
		for _, tbl := range tableList {
			db.Exec("DROP TABLE IF EXISTS " + tbl)
		}
	}()
}
