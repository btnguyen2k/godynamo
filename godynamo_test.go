package godynamo

import (
	"context"
	"database/sql"
	"os"
	"strings"
	"testing"
)

func Test_OpenDatabase(t *testing.T) {
	testName := "Test_OpenDatabase"
	driver := "godynamo"
	dsn := "dummy"
	db, err := sql.Open(driver, dsn)
	if err != nil {
		t.Fatalf("%s failed: %s", testName, err)
	}
	if db == nil {
		t.Fatalf("%s failed: nil", testName)
	}
}

// func TestDriver_invalidConnectionString(t *testing.T) {
// 	testName := "TestDriver_invalidConnectionString"
// 	driver := "godynamo"
// 	{
// 		db, _ := sql.Open(driver, "Endpoint;AkId=demo")
// 		if err := db.Ping(); err == nil {
// 			t.Fatalf("%s failed: should have error", testName)
// 		}
// 	}
// 	{
// 		db, _ := sql.Open(driver, "Endpoint=demo;AkId")
// 		if err := db.Ping(); err == nil {
// 			t.Fatalf("%s failed: should have error", testName)
// 		}
// 	}
// 	{
// 		db, _ := sql.Open(driver, "Endpoint=http://localhost:8000;AkId=demo/invalid_key")
// 		if err := db.Ping(); err == nil {
// 			t.Fatalf("%s failed: should have error", testName)
// 		}
// 	}
// }

/*----------------------------------------------------------------------*/

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

/*----------------------------------------------------------------------*/

func TestDriver_Conn(t *testing.T) {
	testName := "TestDriver_Conn"
	db := _openDb(t, testName)
	defer db.Close()
	conn, err := db.Conn(context.Background())
	if err != nil {
		t.Fatalf("%s failed: %s", testName, err)
	}
	defer conn.Close()
}

func TestDriver_Transaction(t *testing.T) {
	testName := "TestDriver_Transaction"
	db := _openDb(t, testName)
	defer db.Close()
	if tx, err := db.BeginTx(context.Background(), nil); tx != nil || err == nil {
		t.Fatalf("%s failed: transaction is not supported yet", testName)
	} else if strings.Index(err.Error(), "not supported") < 0 {
		t.Fatalf("%s failed: transaction is not supported yet / %s", testName, err)
	}
}

func TestDriver_Open(t *testing.T) {
	testName := "TestDriver_Open"
	db := _openDb(t, testName)
	defer db.Close()
	if err := db.Ping(); err != nil {
		t.Fatalf("%s failed: %s", testName, err)
	}
}

func TestDriver_Close(t *testing.T) {
	testName := "TestDriver_Close"
	db := _openDb(t, testName)
	defer db.Close()
	if err := db.Ping(); err != nil {
		t.Fatalf("%s failed: %s", testName, err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("%s failed: %s", testName, err)
	}
}
