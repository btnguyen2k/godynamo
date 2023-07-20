package godynamo

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"regexp"
	"strconv"
)

var (
	rePlaceholder = regexp.MustCompile(`(?m)\?\s*[\,\]\}\s]`)
	reReturning   = regexp.MustCompile(`(?im)\s+RETURNING\s+((ALL\s+OLD)|(MODIFIED\s+OLD)|(ALL\s+NEW)|(MODIFIED\s+NEW))\s+\*\s*$`)
	reLimit       = regexp.MustCompile(`(?im)\s+LIMIT\s+(\d+)\s*$`)
)

/*----------------------------------------------------------------------*/

// StmtExecutable is the base implementation for INSERT, SELECT, UPDATE and DELETE statements.
type StmtExecutable struct {
	*Stmt
}

func (s *StmtExecutable) parse() error {
	matches := rePlaceholder.FindAllString(s.query+" ", -1)
	s.numInput = len(matches)
	// Look for LIMIT keyword
	limitMatch := reLimit.FindStringSubmatch(s.query)
	if len(limitMatch) > 0 {
		limitString := limitMatch[1]
		limit, err := strconv.Atoi(limitString)
		if err != nil {
			return fmt.Errorf("invalid LIMIT value <%s>: %s", limitString, err)
		}
		s.limit = int32(limit)
		// Remove LIMIT keyword from query
		s.query = reLimit.ReplaceAllString(s.query, "")
	}
	return nil
}

func (s *StmtExecutable) validate() error {
	return nil
}

/*----------------------------------------------------------------------*/

// StmtInsert implements "INSERT" statement.
//
// Syntax: follow "PartiQL insert statements for DynamoDB" https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/ql-reference.insert.html
type StmtInsert struct {
	*StmtExecutable
}

// Query implements driver.Stmt/Query.
// This function is not implemented, use Exec instead.
func (s *StmtInsert) Query(_ []driver.Value) (driver.Rows, error) {
	return nil, errors.New("this operation is not supported, please use Exec")
}

// QueryContext implements driver.StmtQueryContext/QueryContext.
// This function is not implemented, use ExecContext instead.
func (s *StmtInsert) QueryContext(_ context.Context, _ []driver.NamedValue) (driver.Rows, error) {
	return nil, errors.New("this operation is not supported, please use ExecContext")
}

// Exec implements driver.Stmt/Exec.
func (s *StmtInsert) Exec(values []driver.Value) (driver.Result, error) {
	return s.ExecContext(nil, ValuesToNamedValues(values))
}

// ExecContext implements driver.StmtExecContext/ExecContext.
//
// @Available since v0.2.0
func (s *StmtInsert) ExecContext(ctx context.Context, values []driver.NamedValue) (driver.Result, error) {
	outputFn, err := s.conn.executeContext(ctx, s.Stmt, values)
	if err == ErrInTx {
		return &TxResultNoResultSet{outputFn: outputFn}, nil
	}
	affectedRows := int64(0)
	if err == nil {
		affectedRows = 1
	}
	return &ResultNoResultSet{err: err, affectedRows: affectedRows}, err
}

/*----------------------------------------------------------------------*/

// StmtSelect implements "SELECT" statement.
//
// Syntax: follow "PartiQL select statements for DynamoDB" https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/ql-reference.select.html
type StmtSelect struct {
	*StmtExecutable
}

// Exec implements driver.Stmt/Exec.
// This function is not implemented, use Query instead.
func (s *StmtSelect) Exec(_ []driver.Value) (driver.Result, error) {
	return nil, errors.New("this operation is not supported, please use Query")
}

// ExecContext implements driver.StmtExecContext/ExecContext.
// This function is not implemented, use QueryContext instead.
func (s *StmtSelect) ExecContext(_ context.Context, _ []driver.NamedValue) (driver.Result, error) {
	return nil, errors.New("this operation is not supported, please use QueryContext")
}

// Query implements driver.Stmt/Query.
func (s *StmtSelect) Query(values []driver.Value) (driver.Rows, error) {
	return s.QueryContext(nil, ValuesToNamedValues(values))
}

// QueryContext implements driver.StmtQueryContext/QueryContext.
//
// @Available since v0.2.0
func (s *StmtSelect) QueryContext(ctx context.Context, values []driver.NamedValue) (driver.Rows, error) {
	outputFn, err := s.conn.executeContext(ctx, s.Stmt, values)
	// TODO Query is not supported yet in tx mode
	// if err == ErrInTx {
	// 	return &TxResultResultSet{wrap: ResultResultSet{err: err}, outputFn: outputFn}, nil
	// }
	result := (&ResultResultSet{stmtOutput: outputFn()}).init()
	return result, err
}

/*----------------------------------------------------------------------*/

// StmtUpdate implements "UPDATE" statement.
//
// Syntax: follow "PartiQL update statements for DynamoDB" https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/ql-reference.update.html
//
// Note: StmtUpdate returns the updated item by appending "RETURNING ALL OLD *" to the statement.
type StmtUpdate struct {
	*StmtExecutable
}

func (s *StmtUpdate) parse() error {
	if !reReturning.MatchString(s.query) && s.conn.txMode == txNone {
		s.query += " RETURNING ALL OLD *"
	}
	return s.StmtExecutable.parse()
}

// Query implements driver.Stmt/Query.
func (s *StmtUpdate) Query(values []driver.Value) (driver.Rows, error) {
	return s.QueryContext(nil, ValuesToNamedValues(values))
}

// QueryContext implements driver.StmtQueryContext/QueryContext.
//
// @Available since v0.2.0
func (s *StmtUpdate) QueryContext(ctx context.Context, values []driver.NamedValue) (driver.Rows, error) {
	outputFn, err := s.conn.executeContext(ctx, s.Stmt, values)
	result := (&ResultResultSet{stmtOutput: outputFn()}).init()
	if err == nil || IsAwsError(err, "ConditionalCheckFailedException") {
		err = nil
	}
	return result, err
}

// Exec implements driver.Stmt/Exec.
func (s *StmtUpdate) Exec(values []driver.Value) (driver.Result, error) {
	return s.ExecContext(nil, ValuesToNamedValues(values))
}

// ExecContext implements driver.StmtExecContext/ExecContext.
//
// @Available since v0.2.0
func (s *StmtUpdate) ExecContext(ctx context.Context, values []driver.NamedValue) (driver.Result, error) {
	outputFn, err := s.conn.executeContext(ctx, s.Stmt, values)
	if err == ErrInTx {
		return &TxResultNoResultSet{outputFn: outputFn}, nil
	}
	affectedRows := int64(0)
	if err == nil {
		affectedRows = int64(len(outputFn().Items))
	}
	if IsAwsError(err, "ConditionalCheckFailedException") {
		err = nil
	}
	return &ResultNoResultSet{err: err, affectedRows: affectedRows}, err
}

/*----------------------------------------------------------------------*/

// StmtDelete implements "DELETE" statement.
//
// Syntax: follow "PartiQL delete statements for DynamoDB" https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/ql-reference.delete.html
//
// Note: StmtDelete returns the deleted item by appending "RETURNING ALL OLD *" to the statement.
type StmtDelete struct {
	*StmtExecutable
}

func (s *StmtDelete) parse() error {
	if !reReturning.MatchString(s.query) && s.conn.txMode == txNone {
		s.query += " RETURNING ALL OLD *"
	}
	return s.StmtExecutable.parse()
}

// Query implements driver.Stmt/Query.
func (s *StmtDelete) Query(values []driver.Value) (driver.Rows, error) {
	return s.QueryContext(nil, ValuesToNamedValues(values))
}

// QueryContext implements driver.StmtQueryContext/QueryContext.
//
// @Available since v0.2.0
func (s *StmtDelete) QueryContext(ctx context.Context, values []driver.NamedValue) (driver.Rows, error) {
	outputFn, err := s.conn.executeContext(ctx, s.Stmt, values)
	result := (&ResultResultSet{stmtOutput: outputFn()}).init()
	if err == nil || IsAwsError(err, "ConditionalCheckFailedException") {
		err = nil
	}
	return result, err
}

// Exec implements driver.Stmt/Exec.
func (s *StmtDelete) Exec(values []driver.Value) (driver.Result, error) {
	return s.ExecContext(nil, ValuesToNamedValues(values))
}

// ExecContext implements driver.StmtExecContext/ExecContext.
//
// @Available since v0.2.0
func (s *StmtDelete) ExecContext(ctx context.Context, values []driver.NamedValue) (driver.Result, error) {
	outputFn, err := s.conn.executeContext(ctx, s.Stmt, values)
	if err == ErrInTx {
		return &TxResultNoResultSet{outputFn: outputFn}, nil
	}
	affectedRows := int64(0)
	if err == nil {
		affectedRows = int64(len(outputFn().Items))
	}
	if IsAwsError(err, "ConditionalCheckFailedException") {
		err = nil
	}
	return &ResultNoResultSet{err: err, affectedRows: affectedRows}, err
}
