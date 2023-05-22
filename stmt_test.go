package godynamo

import (
	"database/sql"
)

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
