package godynamo

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

var (
	ErrInTx           = errors.New("there is an ongoing transaction, new transaction/statement or fetching result is not allowed")
	ErrInvalidTxStage = errors.New("invalid transaction stage")
	ErrNoTx           = errors.New("no transaction is in progress")
	ErrTxCommitting   = errors.New("transaction is being committed")
	ErrTxRollingBack  = errors.New("transaction is being rolled back")
)

type txMode int

const (
	txNone txMode = iota
	txStarted
	txCommitting
	txRollingBack
)

// txStmt holds a statement to be executed in a transaction.
type txStmt struct {
	stmt   *Stmt
	values []driver.NamedValue
	output *dynamodb.ExecuteStatementOutput
}

type executeStatementOutputWrapper func() *dynamodb.ExecuteStatementOutput

// Conn is AWS DynamoDB implementation of driver.Conn.
type Conn struct {
	client     *dynamodb.Client // AWS DynamoDB client
	timeout    time.Duration
	lock       sync.Mutex
	tx         *Tx
	txMode     txMode
	txStmtList []*txStmt
}

func (c *Conn) newContext() context.Context {
	ctx, cancelFunc := context.WithTimeout(context.Background(), c.timeout)
	go func() {
		time.Sleep(c.timeout)
		cancelFunc()
	}()
	return ctx
}

func (c *Conn) ensureContext(ctx context.Context) context.Context {
	if ctx == nil {
		ctx = c.newContext()
	}
	return ctx
}

func (c *Conn) commit() error {
	c.lock.Lock()
	defer c.lock.Unlock()
	if c.tx == nil {
		return ErrNoTx
	}
	if c.txMode == txRollingBack {
		return ErrTxRollingBack
	}
	if c.txMode != txStarted && c.txMode != txCommitting {
		return ErrInvalidTxStage
	}
	c.txMode = txCommitting
	defer func() {
		c.tx = nil
		c.txMode = txNone
		c.txStmtList = nil
	}()

	if len(c.txStmtList) == 0 {
		//empty transaction should be successful
		return nil
	}

	txStmts := make([]types.ParameterizedStatement, len(c.txStmtList))
	for i, txStmt := range c.txStmtList {
		params := make([]types.AttributeValue, len(txStmt.values))
		var err error
		for j, v := range txStmt.values {
			params[j], err = ToAttributeValue(v.Value)
			if err != nil {
				return fmt.Errorf("error marshalling parameter %d-th for statement <%s>: %s", j+1, txStmt.stmt.query, err)
			}
		}
		txStmts[i] = types.ParameterizedStatement{Statement: &txStmt.stmt.query, Parameters: params}
	}
	input := &dynamodb.ExecuteTransactionInput{
		TransactStatements:     txStmts,
		ReturnConsumedCapacity: types.ReturnConsumedCapacityTotal,
	}
	outputExecuteTransaction, err := c.client.ExecuteTransaction(c.newContext(), input)
	if err == nil {
		for i, txStmt := range c.txStmtList {
			txStmt.output = &dynamodb.ExecuteStatementOutput{ResultMetadata: outputExecuteTransaction.ResultMetadata}
			if len(outputExecuteTransaction.ConsumedCapacity) > i {
				txStmt.output.ConsumedCapacity = &outputExecuteTransaction.ConsumedCapacity[i]
			}
			if len(outputExecuteTransaction.Responses) > i {
				txStmt.output.Items = []map[string]types.AttributeValue{outputExecuteTransaction.Responses[i].Item}
			}
		}
	}
	return err
}

func (c *Conn) rollback() error {
	c.lock.Lock()
	defer c.lock.Unlock()
	if c.tx == nil {
		return ErrNoTx
	}
	if c.txMode == txCommitting {
		return ErrTxCommitting
	}
	if c.txMode != txStarted && c.txMode != txRollingBack {
		return ErrInvalidTxStage
	}
	c.txMode = txRollingBack
	defer func() {
		c.tx = nil
		c.txMode = txNone
		c.txStmtList = nil
	}()
	return nil
}

// execute executes a PartiQL query and returns the result output.
func (c *Conn) executeContext(ctx context.Context, stmt *Stmt, values []driver.NamedValue) (executeStatementOutputWrapper, error) {
	//fmt.Printf("[DEBUG] executeContext: in-tx %5v - %s\n", c.tx != nil, stmt.query)
	if c.txMode == txStarted {
		// transaction has started and not yet committed or rolled back
		// --> can add more statements to the transaction
		txStmt := txStmt{stmt: stmt, values: values}
		c.txStmtList = append(c.txStmtList, &txStmt)
		return func() *dynamodb.ExecuteStatementOutput {
			return txStmt.output
		}, ErrInTx
	}
	if c.txMode != txNone {
		// transaction is in the middle of committing or rolling back
		// --> can neither add more statements to the transaction nor execute any statement
		return nil, ErrInvalidTxStage
	}

	/* not in transaction mode, execute the statement normally */

	params := make([]types.AttributeValue, len(values))
	var err error
	for i, v := range values {
		params[i], err = ToAttributeValue(v.Value)
		if err != nil {
			return nil, fmt.Errorf("error marshalling parameter %d-th: %s", i+1, err)
		}
	}

	input := &dynamodb.ExecuteStatementInput{
		Statement:              &stmt.query,
		ReturnConsumedCapacity: types.ReturnConsumedCapacityTotal,
		Limit:                  stmt.limit,
	}
	if len(params) > 0 {
		input.Parameters = params
	}
	if consistentRead, ok := stmt.withOpts["CONSISTENT_READ"]; ok {
		input.ConsistentRead = aws.Bool(consistentRead.FirstBool())
	} else if consistentRead, ok = stmt.withOpts["CONSISTENTREAD"]; ok {
		input.ConsistentRead = aws.Bool(consistentRead.FirstBool())
	}

	if !reSelect.MatchString(stmt.query) {
		output, err := c.client.ExecuteStatement(c.ensureContext(ctx), input)
		return func() *dynamodb.ExecuteStatementOutput {
			return output
		}, err
	}

	return c.executeSelectContext(ctx, stmt, input)
}

// SELECT query could be paged, need to fetch all pages
func (c *Conn) executeSelectContext(ctx context.Context, stmt *Stmt, input *dynamodb.ExecuteStatementInput) (executeStatementOutputWrapper, error) {
	ctx = c.ensureContext(ctx)
	var firstOutput *dynamodb.ExecuteStatementOutput
	var err error
	var limitNumItems int32 = 0
	if stmt.limit != nil {
		limitNumItems = *stmt.limit
	}
	//idx := 0                         // FIXME
	//fetched := make(map[string]bool) // FIXME
	for {
		var output *dynamodb.ExecuteStatementOutput
		output, err = c.client.ExecuteStatement(ctx, input)
		if err != nil {
			return func() *dynamodb.ExecuteStatementOutput {
				return output
			}, err
		}

		//// FIXME
		//idx++
		//for _, item := range output.Items {
		//	fetched[item["id"].(*types.AttributeValueMemberS).Value] = true
		//}
		//fmt.Printf("[DEBUG] %2d / %s (LIMIT %#v) / LastEvaluatedKey: %d - NextToken: %5v / Fetched: %2d - Total: %2d\n", idx, stmt.query, stmt.limit, len(output.LastEvaluatedKey), output.NextToken != nil, len(output.Items), len(fetched))
		//// END FIXME

		if firstOutput == nil {
			firstOutput = output
		} else {
			firstOutput.ResultMetadata = output.ResultMetadata
			firstOutput.LastEvaluatedKey = output.LastEvaluatedKey
			firstOutput.NextToken = output.NextToken
			firstOutput.ConsumedCapacity = output.ConsumedCapacity
			firstOutput.Items = append(firstOutput.Items, output.Items...)
		}
		input.NextToken = output.NextToken

		//merge result
		if limitNumItems > 0 {
			if len(firstOutput.Items) >= int(limitNumItems) {
				firstOutput.Items = firstOutput.Items[:limitNumItems]
				break
			}
			input.Limit = aws.Int32(limitNumItems - int32(len(firstOutput.Items)))
		}

		if output.NextToken == nil {
			break
		}
	}
	return func() *dynamodb.ExecuteStatementOutput {
		return firstOutput
	}, err
}

// Prepare implements driver.Conn/Prepare.
func (c *Conn) Prepare(query string) (driver.Stmt, error) {
	return c.PrepareContext(context.Background(), query)
}

// PrepareContext implements driver.ConnPrepareContext/PrepareContext.
//
// Note: since <<VERSION>>, this function returns ErrInTx if there is an outgoing transaction.
//
// @Available since v0.2.0
func (c *Conn) PrepareContext(_ context.Context, query string) (driver.Stmt, error) {
	return parseQuery(c, query)
}

// Close implements driver.Conn/Close.
func (c *Conn) Close() error {
	if c.tx != nil {
		//rolling back any outgoing transaction
		return c.tx.Rollback()
	}
	return nil
}

// Begin implements driver.Conn/Begin.
func (c *Conn) Begin() (driver.Tx, error) {
	return c.BeginTx(context.Background(), driver.TxOptions{})
}

// BeginTx implements driver.Conn/BeginTx.
//
// @Available since v0.2.0
func (c *Conn) BeginTx(_ context.Context, _ driver.TxOptions) (driver.Tx, error) {
	c.lock.Lock()
	defer c.lock.Unlock()
	if c.tx == nil {
		c.tx = &Tx{conn: c}
		c.txMode = txStarted
		c.txStmtList = make([]*txStmt, 0)
		return c.tx, nil
	}
	return c.tx, ErrInTx
}

// CheckNamedValue implements driver.NamedValueChecker/CheckNamedValue.
func (c *Conn) CheckNamedValue(_ *driver.NamedValue) error {
	// since DynamoDB is document db, it accepts any value types
	return nil
}
