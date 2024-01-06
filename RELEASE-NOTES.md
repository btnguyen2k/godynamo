# godynamo release notes

## 2024-01-06 - v1.2.0

### Added/Refactoring

- Refactor transaction support

### Fixed/Improvement

- Fix: empty transaction should be committed successfully

## 2024-01-02 - v1.1.1

### Fixed/Improvement

- Fix: result returned from a SELECT can be paged if too big

## 2023-12-31 - v1.1.0

### Added/Refactoring

- Add function WaitForTableStatus
- Add function WaitForGSIStatus
- Add method TransformInsertStmToPartiQL

### Fixed/Improvement

- Fix: empty LSI should be nil

## 2023-12-27 - v1.0.0

### Changed

- BREAKING: bump Go version to 1.18

### Added/Refactoring

- Refactor to follow go-module-template structure

### Fixed/Improvement

- Fix GoLint
- Fix CodeQL alerts

## 2023-07-27 - v0.4.0

- Support `ConsistentRead` option for `SELECT` query.

## 2023-07-25 - v0.3.1

- Fix: placeholder parsing.

## 2023-07-24 - v0.3.0

- `ColumnTypeDatabaseTypeName` returns DynamoDB's native data types (e.g. `B`, `N`, `S`, `SS`, `NS`, `BS`, `BOOL`, `L`, `M`, `NULL`).
- `RowsDescribeTable.ColumnTypeScanType` and `RowsDescribeIndex.ColumnTypeScanType` return correct Go types based on DynamoDB spec.
- Support `LIMIT` clause for `SELECT` query.

## 2023-05-31 - v0.2.0

- Add transaction support.

## 2023-05-27 - v0.1.0

- Driver for `database/sql`, supported statements:
  - Table: `CREATE TABLE`, `LIST TABLES`, `DESCRIBE TABLE`, `ALTER TABLE`, `DROP TABLE`.
  - Index: `DESCRIBE LSI`, `CREATE GSI`, `DESCRIBE GSI`, `ALTER GSI`, `DROP GSI`.
  - Document: `INSERT`, `SELECT`, `UPDATE`, `DELETE`.
