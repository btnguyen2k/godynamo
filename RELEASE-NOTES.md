# godynamo release notes

## 2023-07-xx - v0.3.0

- `ColumnTypeDatabaseTypeName` returns DynamoDB's native data types (e.g. `B`, `N`, `S`, `SS`, `NS`, `BS`, `BOOL`, `L`, `M`, `NULL`).
- `RowsDescribeTable.ColumnTypeScanType` and `RowsDescribeIndex.ColumnTypeScanType` return correct Go types based on DynamoDB spec.g

## 2023-05-31 - v0.2.0

- Add transaction support.

## 2023-05-27 - v0.1.0

- Driver for `database/sql`, supported statements:
  - Table: `CREATE TABLE`, `LIST TABLES`, `DESCRIBE TABLE`, `ALTER TABLE`, `DROP TABLE`.
  - Index: `DESCRIBE LSI`, `CREATE GSI`, `DESCRIBE GSI`, `ALTER GSI`, `DROP GSI`.
  - Document: `INSERT`, `SELECT`, `UPDATE`, `DELETE`.
