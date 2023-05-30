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
