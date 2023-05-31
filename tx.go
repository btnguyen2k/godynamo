package godynamo

import (
	"fmt"
)

// TxResultNoResultSet is transaction-aware version of ResultNoResultSet.
//
// @Available since v0.2.0
type TxResultNoResultSet struct {
	hasOutput    bool
	outputFn     executeStatementOutputWrapper
	affectedRows int64
}

// LastInsertId implements driver.Result/LastInsertId.
func (t *TxResultNoResultSet) LastInsertId() (int64, error) {
	return 0, fmt.Errorf("this operation is not supported")
}

// RowsAffected implements driver.Result/RowsAffected.
func (t *TxResultNoResultSet) RowsAffected() (int64, error) {
	if !t.hasOutput {
		output := t.outputFn()
		if output != nil {
			t.hasOutput = true
			t.affectedRows = 1
		}
	}
	if !t.hasOutput {
		return 0, ErrInTx
	}
	return t.affectedRows, nil
}

// // TxResultResultSet is transaction-aware version of ResultResultSet.
// //
// // @Available since v0.2.0
// type TxResultResultSet struct {
// 	wrap      ResultResultSet
// 	hasOutput bool
// 	outputFn  executeStatementOutputWrapper
// }
//
// func (r *TxResultResultSet) checkOutput() {
// 	if !r.hasOutput {
// 		r.wrap.stmtOutput = r.outputFn()
// 		fmt.Println("DEBUG", r.wrap.stmtOutput)
// 		if r.wrap.stmtOutput != nil {
// 			r.wrap.err = nil
// 			r.hasOutput = true
// 			r.wrap.init()
// 		}
// 	}
// }
//
// // Columns implements driver.Rows/Columns.
// func (r *TxResultResultSet) Columns() []string {
// 	r.checkOutput()
// 	return r.wrap.Columns()
// }
//
// // ColumnTypeScanType implements driver.RowsColumnTypeScanType/ColumnTypeScanType
// func (r *TxResultResultSet) ColumnTypeScanType(index int) reflect.Type {
// 	r.checkOutput()
// 	return r.wrap.ColumnTypeScanType(index)
// }
//
// // ColumnTypeDatabaseTypeName implements driver.RowsColumnTypeDatabaseTypeName/ColumnTypeDatabaseTypeName
// func (r *TxResultResultSet) ColumnTypeDatabaseTypeName(index int) string {
// 	r.checkOutput()
// 	return r.wrap.ColumnTypeDatabaseTypeName(index)
// }
//
// // Close implements driver.Rows/Close.
// func (r *TxResultResultSet) Close() error {
// 	r.checkOutput()
// 	if !r.hasOutput {
// 		return ErrInTx
// 	}
// 	return nil
// }
//
// // Next implements driver.Rows/Next.
// func (r *TxResultResultSet) Next(dest []driver.Value) error {
// 	r.checkOutput()
// 	return r.wrap.Next(dest)
// }

/*----------------------------------------------------------------------*/

// Tx is AWS DynamoDB implementation of driver.Tx.
//
// @Available since v0.2.0
type Tx struct {
	conn *Conn
}

// Commit implements driver.Tx/Commit
func (t *Tx) Commit() error {
	return t.conn.commit()
}

// Rollback implements driver.Tx/Rollback
func (t *Tx) Rollback() error {
	return t.conn.rollback()
}
