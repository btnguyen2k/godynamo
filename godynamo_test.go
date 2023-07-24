package godynamo

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"os"
	"reflect"
	"strconv"
	"strings"
	"testing"

	"github.com/btnguyen2k/consu/reddo"
)

func Test_parseConnString_parseParamValue(t *testing.T) {
	testName := "Test_parseConnString_parseParamValue"
	type paramValueStruct struct {
		pnames        []string
		enames        []string
		paramType     reflect.Type
		defaultValue  interface{}
		expectedValue interface{}
		validator     func(val interface{}) bool
	}

	os.Setenv("ENV_TIMEOUT", "1234")
	os.Setenv("ENV_TIMEOUT_INVALID", "-123")

	testCases := []struct {
		name           string
		connStr        string
		expectedParams map[string]string
		paramValues    []paramValueStruct
	}{
		{
			name:    "endpoint",
			connStr: "endpoint=http://localhost:8000",
			expectedParams: map[string]string{
				"ENDPOINT": "http://localhost:8000",
			},
			paramValues: []paramValueStruct{
				{
					pnames:        []string{"EP", "ENDPOINT"},
					paramType:     reddo.TypeString,
					expectedValue: "http://localhost:8000",
				},
				{
					pnames:        []string{"TIMEOUT"},
					paramType:     reddo.TypeInt,
					defaultValue:  int64(123),
					expectedValue: int64(123),
				},
				{
					pnames: []string{"TIMEOUT_ENV"},
					enames: []string{"ENV_TIMEOUT"},
					validator: func(val interface{}) bool {
						return val.(int64) >= 0
					},
					paramType:     reddo.TypeInt,
					expectedValue: int64(1234),
				},
				{
					pnames: []string{"TIMEOUT_DEFAULT"},
					enames: []string{"ENV_TIMEOUT_INVALID"},
					validator: func(val interface{}) bool {
						return val.(int64) >= 0
					},
					paramType:     reddo.TypeInt,
					defaultValue:  int64(12345),
					expectedValue: int64(12345),
				},
			},
		},
		{
			name:    "empty",
			connStr: "endpoint=http://localhost:8000;timeout",
			expectedParams: map[string]string{
				"ENDPOINT": "http://localhost:8000",
				"TIMEOUT":  "",
			},
			paramValues: []paramValueStruct{
				{
					pnames:        []string{"T", "TIMEOUT"},
					paramType:     reddo.TypeInt,
					defaultValue:  int64(1234),
					expectedValue: int64(1234),
				},
			},
		},
		{
			name:    "invalid_timeout_value",
			connStr: "endpoint=http://localhost:8000;timeout=-1",
			expectedParams: map[string]string{
				"ENDPOINT": "http://localhost:8000",
				"TIMEOUT":  "-1",
			},
			paramValues: []paramValueStruct{
				{
					pnames:        []string{"T", "TIMEOUT"},
					paramType:     reddo.TypeInt,
					defaultValue:  int64(1234),
					expectedValue: int64(1234),
					validator: func(val interface{}) bool {
						return val.(int64) >= 0
					},
				},
			},
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			params := parseConnString(testCase.connStr)
			if !reflect.DeepEqual(params, testCase.expectedParams) {
				t.Fatalf("%s failed: expected %#v received %#v", testName+"/"+testCase.name, testCase.expectedParams, params)
			}
			for _, paramValue := range testCase.paramValues {
				val := parseParamValue(params, paramValue.paramType, paramValue.validator, paramValue.defaultValue, paramValue.pnames, paramValue.enames)
				if !reflect.DeepEqual(val, paramValue.expectedValue) {
					t.Fatalf("%s failed: <%s> expected %#v received %#v", testName+"/"+testCase.name, paramValue.pnames, paramValue.expectedValue, val)
				}
			}
		})
	}
}

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

func TestConn_ValuesToNamedValues(t *testing.T) {
	testName := "TestConn_ValuesToNamedValues"
	values := []driver.Value{1, "2", true}
	namedValues := ValuesToNamedValues(values)
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
