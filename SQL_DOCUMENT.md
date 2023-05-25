# godynamo - Supported statements for document

- `INSERT`
- `SELECT`
- `UPDATE`
- `DELETE`

## INSERT

Syntax: [PartiQL insert statements for DynamoDB](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/ql-reference.insert.html)

Example:
```go
result, err := db.Exec(`INSERT INTO "session" VALUE {'app': ?, 'user': ?, 'active': ?}`, "frontend", "user1", true)
if err == nil {
	numAffectedRow, err := result.RowsAffected()
	...
}
```

Description: use the `INSERT` statement to add an item to a table.

- If the statement is executed successfully, `RowsAffected()` returns `1, nil`.
- Note: the `INSERT` must follow [PartiQL syntax](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/ql-reference.insert.html), e.g. attribute names are enclosed by _single_ quotation marks ('attr-name'), table name is enclosed by _double_ quotation marks ("table-name"), etc.
