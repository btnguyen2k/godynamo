package godynamo_test

import (
	"database/sql"
	_ "github.com/miyamo2/godynamo"
	"os"
	"strings"
	"testing"
)

//var (
//	typeM    = reflect.TypeOf(make(map[string]interface{}))
//	typeL    = reflect.TypeOf(make([]interface{}, 0))
//	typeS    = reddo.TypeString
//	typeBool = reddo.TypeBool
//	typeN    = reddo.TypeFloat
//	typeTime = reddo.TypeTime
//)

func _openDb(t *testing.T, testName string) *sql.DB {
	driver := "godynamo"
	url := strings.ReplaceAll(os.Getenv("AWS_DYNAMODB_URL"), `"`, "")
	if url == "" {
		t.Skipf("%s skipped", testName)
	}
	db, err := sql.Open(driver, url)
	if err != nil {
		t.Fatalf("%s failed: %s", testName+"/sql.Open", err)
	}
	return db
}
