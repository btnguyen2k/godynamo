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

## SELECT

Syntax: [PartiQL select statements for DynamoDB](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/ql-reference.select.html)

Example:
```go
result, err := db.Query(`SELECT * FROM "session" WHERE app='frontend'`)
if err == nil {
	...
}
```

Description: use the `SELECT` statement to retrieve data from a table.

- Note: the `SELECT` must follow [PartiQL syntax](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/ql-reference.select.html).

Sample result:
|active|app|user|
|------|---|----|
|true|"frontend"|"user1"|

## DELETE

Syntax: [PartiQL delete statements for DynamoDB](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/ql-reference.delete.html)

Example:
```go
result, err := db.Exec(`DELETE FROM "tbltest" WHERE "app"=? AND "user"=?`, "app0", "user1")
if err == nil {
	numAffectedRow, err := result.RowsAffected()
	...
}
```

`Query` can also be used to have the content of the old item returned.
```go
if err == nil {
	...
}
```

Description: use the `DELETE` statement to delete an existing item from a table.

- Note: the `DELETE` must follow [PartiQL syntax](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/ql-reference.delete.html).

Sample result:
|app|location|platform|user|
|---|--------|--------|----|
|"app0"|"AU"|"Windows"|"user2"|
