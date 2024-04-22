package godynamo_test

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/btnguyen2k/consu/reddo"
	"github.com/btnguyen2k/godynamo"
	"os"
	"reflect"
	"strconv"
	"strings"
	"testing"
)

var (
	typeM    = reflect.TypeOf(make(map[string]interface{}))
	typeL    = reflect.TypeOf(make([]interface{}, 0))
	typeS    = reddo.TypeString
	typeBool = reddo.TypeBool
	typeN    = reddo.TypeFloat
	typeTime = reddo.TypeTime
)

func Test_OpenDatabase(t *testing.T) {
	testName := "Test_OpenDatabase"
	dbdriver := "godynamo"
	dsn := "dummy"
	db, err := sql.Open(dbdriver, dsn)
	if err != nil {
		t.Fatalf("%s failed: %s", testName, err)
	}
	if db == nil {
		t.Fatalf("%s failed: nil", testName)
	}
}

func Test_OpenDatabase_With_AWSConfig(t *testing.T) {
	testName := "Test_OpenDatabase_With_AWSConfig"
	dbdriver := "godynamo"
	dsn := "dummy"
	godynamo.RegisterAWSConfig(aws.Config{
		Region: "us-west-2",
		Credentials: credentials.NewStaticCredentialsProvider(
			"abcdefg123456789", "abcdefg123456789", ""),
	})
	defer godynamo.DeregisterAWSConfig()
	db, err := sql.Open(dbdriver, dsn)
	if err != nil {
		t.Fatalf("%s failed: %s", testName, err)
	}
	if db == nil {
		t.Fatalf("%s failed: nil", testName)
	}

	// with empty aws.Config
	godynamo.RegisterAWSConfig(aws.Config{})
	dbWithEmptyAWSConfig, err := sql.Open(dbdriver, dsn)
	if err != nil {
		t.Fatalf("%s failed: %s", testName, err)
	}
	if dbWithEmptyAWSConfig == nil {
		t.Fatalf("%s failed: nil", testName)
	}
}

func TestConn_ValuesToNamedValues(t *testing.T) {
	testName := "TestConn_ValuesToNamedValues"
	values := []driver.Value{1, "2", true}
	namedValues := godynamo.ValuesToNamedValues(values)
	for i, nv := range namedValues {
		if nv.Ordinal != i {
			t.Fatalf("%s failed: <Ordinal> expected %d received %d", testName, i, nv.Ordinal)
		}
		if nv.Name != "$"+strconv.Itoa(i+1) {
			t.Fatalf("%s failed: <Name> expected %s received %s", testName, "$"+strconv.Itoa(i+1), nv.Name)
		}
		if !reflect.DeepEqual(nv.Value, values[i]) {
			t.Fatalf("%s failed: <Value> expected %#v received %#v", testName, values[i], nv.Value)
		}
	}
}

/*----------------------------------------------------------------------*/

func _openDb(t *testing.T, testName string) *sql.DB {
	dbdriver := "godynamo"
	url := strings.ReplaceAll(os.Getenv("AWS_DYNAMODB_URL"), `"`, "")
	if url == "" {
		t.Skipf("%s skipped", testName)
	}
	db, err := sql.Open(dbdriver, url)
	if err != nil {
		t.Fatalf("%s failed: %s", testName+"/sql.Open", err)
	}
	return db
}

/*----------------------------------------------------------------------*/

func TestDriver_Conn(t *testing.T) {
	testName := "TestDriver_Conn"
	db := _openDb(t, testName)
	defer func() { _ = db.Close() }()
	conn, err := db.Conn(context.Background())
	if err != nil {
		t.Fatalf("%s failed: %s", testName, err)
	}
	defer func() { _ = conn.Close() }()
}

// func TestDriver_Transaction(t *testing.T) {
// 	testName := "TestDriver_Transaction"
// 	db := _openDb(t, testName)
// 	defer db.Close()
// 	if tx, err := db.BeginTx(context.Background(), nil); tx != nil || err == nil {
// 		t.Fatalf("%s failed: transaction is not supported yet", testName)
// 	} else if strings.Index(err.Error(), "not supported") < 0 {
// 		t.Fatalf("%s failed: transaction is not supported yet / %s", testName, err)
// 	}
// }

func TestDriver_Open(t *testing.T) {
	testName := "TestDriver_Open"
	db := _openDb(t, testName)
	defer func() { _ = db.Close() }()
	if err := db.Ping(); err != nil {
		t.Fatalf("%s failed: %s", testName, err)
	}
}

func TestDriver_Close(t *testing.T) {
	testName := "TestDriver_Close"
	db := _openDb(t, testName)
	defer func() { _ = db.Close() }()
	if err := db.Ping(); err != nil {
		t.Fatalf("%s failed: %s", testName, err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("%s failed: %s", testName, err)
	}
}

func TestDriver_Open_With_AWSConfig(t *testing.T) {
	testName := "TestDriver_Open_With_AWSConfig"
	godynamo.RegisterAWSConfig(aws.Config{
		Region: "us-west-2",
		Credentials: credentials.NewStaticCredentialsProvider(
			"abcdefg123456789", "abcdefg123456789", ""),
	})
	defer godynamo.DeregisterAWSConfig()
	db := _openDb(t, testName)
	defer func() { _ = db.Close() }()
	if err := db.Ping(); err != nil {
		t.Fatalf("%s failed: %s", testName, err)
	}

	// with empty aws.Config
	godynamo.RegisterAWSConfig(aws.Config{})
	dbWithEmptyAWSConfig := _openDb(t, testName)
	defer func() { _ = dbWithEmptyAWSConfig.Close() }()
	if err := dbWithEmptyAWSConfig.Ping(); err != nil {
		t.Fatalf("%s failed: %s", testName, err)
	}
}
