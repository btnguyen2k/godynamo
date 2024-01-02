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
dbrows, err := db.Query(`SELECT * FROM "session" WHERE app='frontend'`)
if err == nil {
	fetchAndPrintAllRows(dbrows)
}
```

Description: use the `SELECT` statement to retrieve data from a table.

- Note: the `SELECT` must follow [PartiQL syntax](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/ql-reference.select.html).

Sample result:

| active | app        | user    |
|--------|------------|---------|
| true   | "frontend" | "user1" |

> Since [v0.3.0](RELEASE-NOTES.md), `godynamodb` supports `LIMIT` clause for `SELECT` statement. Example:
> 
>       dbrows, err := db.Query(`SELECT * FROM "session" WHERE app='frontend' LIMIT 10`)
>
> Note:
> - The `LIMIT` clause is extension offered by `godynamodb` and is not part of [PartiQL syntax](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/ql-reference.select.html).
> - The value for `LIMIT` must be a _positive integer_.

> Since [v0.4.0](RELEASE-NOTES.md), `godynamodb` supports ConsistentRead for `SELECT` statement via clause `WITH ConsistentRead=true` or `WITH Consistent_Read=true`.
> Example:
>
>       dbrows, err := db.Query(`SELECT * FROM "session" WHERE app='frontend' WITH ConsistentRead=true`)
>
> Note: the WITH clause must be placed _at the end_ of the SELECT statement.

## UPDATE

Syntax: [PartiQL update statements for DynamoDB](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/ql-reference.update.html)

Example:
```go
result, err := db.Exec(`UPDATE "tbltest" SET location=? SET os=? WHERE "app"=? AND "user"=?`, "VN", "Ubuntu", "app0", "user1")
if err == nil {
	numAffectedRow, err := result.RowsAffected()
	...
}
```

Description: use the `UPDATE` statement to modify the value of one or more attributes within an item in a table.

- Note: the `UPDATE` must follow [PartiQL syntax](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/ql-reference.update.html).

`Query` can also be used to fetch returned values.
```go
dbrows, err := db.Query(`UPDATE "tbltest" SET location=? SET os=? WHERE "app"=? AND "user"=? RETURNING MODIFIED OLD *`, "VN", "Ubuntu", "app0", "user0")
if err == nil {
	fetchAndPrintAllRows(dbrows)
}
```

Sample result:

| location |
|----------|
| "AU"     |

> If there is no matched item, the error `ConditionalCheckFailedException` is suspended. That means:
> - `RowsAffected()` returns `(0, nil)`
> - `Query` returns empty result set.

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

Description: use the `DELETE` statement to delete an existing item from a table.

- Note: the `DELETE` must follow [PartiQL syntax](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/ql-reference.delete.html).

`Query` can also be used to have the content of the old item returned.
```go
dbrows, err := db.Query(`DELETE FROM "tbltest" WHERE "app"=? AND "user"=?`, "app0", "user1")
if err == nil {
	fetchAndPrintAllRows(dbrows)
}
```

Sample result:

| app    | location | platform  | user    |
|--------|----------|-----------|---------|
| "app0" | "AU"     | "Windows" | "user1" |

> If there is no matched item, the error `ConditionalCheckFailedException` is suspended. That means:
> - `RowsAffected()` returns `(0, nil)`
> - `Query` returns empty result set.
