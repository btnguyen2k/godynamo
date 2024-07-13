package godynamo_test

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/miyamo2/godynamo"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/btnguyen2k/consu/reddo"
	"github.com/btnguyen2k/consu/semita"
)

type lsiDef struct {
	indexName, attrName, attrType string
	projectedAttrs                string
}

type lsiInfo struct {
	lsiDef
	projType string
}

type gsiInfo struct {
	indexName                      string
	rcu, wcu                       int64
	pkAttr, pkType                 string
	skAttr, skType                 string
	projectionType, projectedAttrs string
}

type tableInfo struct {
	tableName      string
	billingMode    string
	rcu, wcu       int64
	pkAttr, pkType string
	skAttr, skType string
	lsi            map[string]lsiInfo
}

const (
	tblTestNotExist  = "test_not_exist"
	tblTestNotExists = "test_not_exists"
	tblTestTemp      = "test_temp"
)

func _initTest(db *sql.DB) {
	_, _ = db.Exec(`DROP TABLE IF EXISTS ` + tblTestNotExist)
	_, _ = db.Exec(`DROP TABLE IF EXISTS ` + tblTestNotExists)
	_, _ = db.Exec(`DROP TABLE IF EXISTS ` + tblTestTemp)
	_ = godynamo.WaitForTableStatus(nil, db, tblTestTemp, []string{""}, 500*time.Millisecond)
	for i := 0; i < 10; i++ {
		_, _ = db.Exec(`DROP TABLE IF EXISTS ` + tblTestTemp + strconv.Itoa(i))
		_ = godynamo.WaitForTableStatus(nil, db, tblTestTemp+strconv.Itoa(i), []string{""}, 500*time.Millisecond)
	}
}

func _verifyTableInfo(t *testing.T, testName string, row map[string]interface{}, tableInfo *tableInfo) {
	s := semita.NewSemita(row)
	var key string

	if tableInfo.billingMode != "" {
		key = "BillingModeSummary.BillingMode"
		billingMode, _ := s.GetValueOfType(key, reddo.TypeString)
		if billingMode != tableInfo.billingMode {
			t.Fatalf("%s failed: expected value at key <%s> to be %#v but received %#v", testName, key, tableInfo.billingMode, billingMode)
		}
	}

	key = "ProvisionedThroughput.ReadCapacityUnits"
	rcu, _ := s.GetValueOfType(key, reddo.TypeInt)
	if rcu != tableInfo.rcu {
		t.Fatalf("%s failed: expected value at key <%s> to be %#v but received %#v", testName, key, tableInfo.rcu, rcu)
	}

	key = "ProvisionedThroughput.WriteCapacityUnits"
	wcu, _ := s.GetValueOfType(key, reddo.TypeInt)
	if wcu != tableInfo.wcu {
		t.Fatalf("%s failed: expected value at key <%s> to be %#v but received %#v", testName, key, tableInfo.wcu, wcu)
	}

	keySchema := make([]types.KeySchemaElement, 0)
	{
		js, _ := json.Marshal(row["KeySchema"])
		_ = json.Unmarshal(js, &keySchema)
		found := false
		for _, keySchemaElement := range keySchema {
			if *keySchemaElement.AttributeName == tableInfo.pkAttr {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("%s failed: table PK is expected to be %#v but not found", testName, tableInfo.pkAttr)
		}
	}

	attrDefs := make([]types.AttributeDefinition, 0)
	{
		js, _ := json.Marshal(row["AttributeDefinitions"])
		_ = json.Unmarshal(js, &attrDefs)
		found := false
		for _, attrDefElement := range attrDefs {
			if *attrDefElement.AttributeName == tableInfo.pkAttr && string(attrDefElement.AttributeType) == tableInfo.pkType {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("%s failed: PK field %s be of type %#v but not found", testName, tableInfo.pkAttr, tableInfo.pkType)
		}
	}

	if tableInfo.skAttr != "" {
		found := false
		for _, keySchemaElement := range keySchema {
			if *keySchemaElement.AttributeName == tableInfo.skAttr {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("%s failed: table SK is expected to be %#v but not found", testName, tableInfo.skAttr)
		}

		found = false
		for _, attrDefElement := range attrDefs {
			if *attrDefElement.AttributeName == tableInfo.skAttr && string(attrDefElement.AttributeType) == tableInfo.skType {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("%s failed: SK field %s be of type %#v but not found", testName, tableInfo.skAttr, tableInfo.skType)
		}
	}

	for expectedIdxName, expectedLsi := range tableInfo.lsi {
		found := false
		tableLsi, _ := s.GetValueOfType("LocalSecondaryIndexes", reflect.TypeOf(make([]interface{}, 0)))
		for i := 0; i < len(tableLsi.([]interface{})); i++ {
			key = fmt.Sprintf("LocalSecondaryIndexes[%d].IndexName", i)
			idxName, _ := s.GetValueOfType(key, reddo.TypeString)
			if idxName == expectedIdxName {
				found = true

				key = fmt.Sprintf("LocalSecondaryIndexes[%d].Projection.ProjectionType", i)
				projType, _ := s.GetValueOfType(key, reddo.TypeString)
				if projType != expectedLsi.projType {
					t.Fatalf("%s failed: expected value at key <%s> to be %#v but received %#v", testName, key, expectedLsi.projType, projType)
				}
				if projType == "INCLUDE" {
					key = fmt.Sprintf("LocalSecondaryIndexes[%d].Projection.NonKeyAttributes", i)
					nonKeyAttrs, _ := s.GetValueOfType(key, reflect.TypeOf(make([]string, 0)))
					if !reflect.DeepEqual(nonKeyAttrs, strings.Split(expectedLsi.projectedAttrs, ",")) {
						t.Fatalf("%s failed: expected value at key <%s> to be %#v but received %#v", testName, key, expectedLsi.projectedAttrs, nonKeyAttrs)
					}
				}

				key = fmt.Sprintf("LocalSecondaryIndexes[%d].KeySchema[1].AttributeName", i)
				attrName, _ := s.GetValueOfType(key, reddo.TypeString)
				if attrName != expectedLsi.attrName {
					t.Fatalf("%s failed: expected value at key <%s> to be %#v but received %#v", testName, key, expectedLsi.attrName, attrName)
				}

				tableAttrs, _ := s.GetValueOfType("AttributeDefinitions", reflect.TypeOf(make([]interface{}, 0)))
				foundAttr := false
				for j := 0; j < len(tableAttrs.([]interface{})); j++ {
					k := fmt.Sprintf("AttributeDefinitions[%d].AttributeName", j)
					attrName, _ := s.GetValueOfType(k, reddo.TypeString)
					if attrName == expectedLsi.attrName {
						foundAttr = true
						k = fmt.Sprintf("AttributeDefinitions[%d].AttributeType", j)
						attrType, _ := s.GetValueOfType(k, reddo.TypeString)
						if attrType != expectedLsi.attrType {
							t.Fatalf("%s failed: expected value at key <%s> to be %#v but received %#v", testName, k, expectedLsi.attrType, attrType)
						}
					}
				}
				if !foundAttr {
					t.Fatalf("%s failed: no attribute definition found for LSI <%s>", testName, expectedIdxName)
				}
			}
		}
		if !found {
			t.Fatalf("%s failed: no LSI <%s> found", testName, expectedIdxName)
		}
	}
}

func _verifyGSIInfo(t *testing.T, testName string, row map[string]interface{}, gsiInfo *gsiInfo) {
	s := semita.NewSemita(row)
	var key string

	key = "IndexName"
	indexName, _ := s.GetValueOfType(key, reddo.TypeString)
	if indexName != gsiInfo.indexName {
		t.Fatalf("%s failed: expected value at key <%s> to be %#v but received %#v", testName, key, gsiInfo.indexName, indexName)
	}

	key = "ProvisionedThroughput.ReadCapacityUnits"
	rcu, _ := s.GetValueOfType(key, reddo.TypeInt)
	if rcu != gsiInfo.rcu {
		t.Fatalf("%s failed: expected value at key <%s> to be %#v but received %#v", testName, key, gsiInfo.rcu, rcu)
	}

	key = "ProvisionedThroughput.WriteCapacityUnits"
	wcu, _ := s.GetValueOfType(key, reddo.TypeInt)
	if wcu != gsiInfo.wcu {
		t.Fatalf("%s failed: expected value at key <%s> to be %#v but received %#v", testName, key, gsiInfo.wcu, wcu)
	}

	key = "KeySchema[0].AttributeName"
	pkAttr, _ := s.GetValueOfType(key, reddo.TypeString)
	if pkAttr != gsiInfo.pkAttr {
		t.Fatalf("%s failed: expected value at key <%s> to be %#v but received %#v", testName, key, gsiInfo.pkAttr, pkAttr)
	}

	if gsiInfo.skAttr != "" {
		key = "KeySchema[1].AttributeName"
		skAttr, _ := s.GetValueOfType(key, reddo.TypeString)
		if skAttr != gsiInfo.skAttr {
			t.Fatalf("%s failed: expected value at key <%s> to be %#v but received %#v", testName, key, gsiInfo.skAttr, skAttr)
		}
	}

	key = "Projection.ProjectionType"
	projectionType, _ := s.GetValueOfType(key, reddo.TypeString)
	if projectionType != gsiInfo.projectionType {
		t.Fatalf("%s failed: expected value at key <%s> to be %#v but received %#v", testName, key, gsiInfo.projectionType, projectionType)
	}
	if projectionType == "INCLUDE" {
		key = "Projection.NonKeyAttributes"
		nonKeyAttrs, _ := s.GetValueOfType(key, reflect.TypeOf(make([]string, 0)))
		if !reflect.DeepEqual(nonKeyAttrs, strings.Split(gsiInfo.projectedAttrs, ",")) {
			t.Fatalf("%s failed: expected value at key <%s> to be %#v but received %#v", testName, key, gsiInfo.projectedAttrs, nonKeyAttrs)
		}
	}
}

func _fetchAllRows(dbRows *sql.Rows) ([]map[string]interface{}, error) {
	colTypes, err := dbRows.ColumnTypes()
	if err != nil {
		return nil, err
	}

	numCols := len(colTypes)
	rows := make([]map[string]interface{}, 0)
	for dbRows.Next() {
		vals := make([]interface{}, numCols)
		scanVals := make([]interface{}, numCols)
		for i := 0; i < numCols; i++ {
			scanVals[i] = &vals[i]
		}
		if err := dbRows.Scan(scanVals...); err == nil {
			row := make(map[string]interface{})
			for i := range colTypes {
				row[colTypes[i].Name()] = vals[i]
			}
			rows = append(rows, row)
		} else if err != sql.ErrNoRows {
			return nil, err
		}
	}
	return rows, nil
}
