package godynamo

import (
	"database/sql/driver"
	"fmt"
	"regexp"
	"strings"
)

const (
	field       = `([\w\-]+)`
	ifNotExists = `(\s+IF\s+NOT\s+EXISTS)?`
	ifExists    = `(\s+IF\s+EXISTS)?`
	with        = `(\s+WITH\s+` + field + `\s*=\s*([\w/\.\*,;:'"-]+)((\s+|\s*,\s+|\s+,\s*)WITH\s+` + field + `\s*=\s*([\w/\.\*,;:'"-]+))*)?`
)

var (
	reCreateTable = regexp.MustCompile(`(?im)^CREATE\s+TABLE` + ifNotExists + `\s+` + field + with + `$`)
	reDropTable   = regexp.MustCompile(`(?im)^(DROP|DELETE)\s+TABLE` + ifExists + `\s+` + field + `$`)
	reListTables  = regexp.MustCompile(`(?im)^LIST\s+TABLES?$`)
)

func parseQuery(c *Conn, query string) (driver.Stmt, error) {
	query = strings.TrimSpace(query)
	if re := reCreateTable; re.MatchString(query) {
		groups := re.FindAllStringSubmatch(query, -1)
		stmt := &StmtCreateTable{
			Stmt:        &Stmt{query: query, conn: c, numInput: 0},
			ifNotExists: strings.TrimSpace(groups[0][1]) != "",
			tableName:   strings.TrimSpace(groups[0][2]),
			withOptsStr: " " + strings.TrimSpace(groups[0][3]),
		}
		if err := stmt.parse(); err != nil {
			return nil, err
		}
		return stmt, stmt.validate()
	}
	if re := reDropTable; re.MatchString(query) {
		groups := re.FindAllStringSubmatch(query, -1)
		stmt := &StmtDropTable{
			Stmt:      &Stmt{query: query, conn: c, numInput: 0},
			tableName: strings.TrimSpace(groups[0][3]),
			ifExists:  strings.TrimSpace(groups[0][2]) != "",
		}
		return stmt, stmt.validate()
	}
	if re := reListTables; re.MatchString(query) {
		stmt := &StmtListTables{
			Stmt: &Stmt{query: query, conn: c, numInput: 0},
		}
		return stmt, stmt.validate()
	}
	// if re := reAlterColl; re.MatchString(query) {
	// 	groups := re.FindAllStringSubmatch(query, -1)
	// 	stmt := &StmtAlterCollection{
	// 		Stmt:        &Stmt{query: query, conn: c, numInput: 0},
	// 		dbName:      strings.TrimSpace(groups[0][3]),
	// 		collName:    strings.TrimSpace(groups[0][4]),
	// 		withOptsStr: strings.TrimSpace(groups[0][5]),
	// 	}
	// 	if stmt.dbName == "" {
	// 		stmt.dbName = defaultDb
	// 	}
	// 	if err := stmt.parse(); err != nil {
	// 		return nil, err
	// 	}
	// 	return stmt, stmt.validate()
	// }

	// if re := reInsert; re.MatchString(query) {
	// 	groups := re.FindAllStringSubmatch(query, -1)
	// 	stmt := &StmtInsert{
	// 		Stmt:      &Stmt{query: query, conn: c, numInput: 0},
	// 		isUpsert:  strings.ToUpper(strings.TrimSpace(groups[0][1])) == "UPSERT",
	// 		dbName:    strings.TrimSpace(groups[0][3]),
	// 		collName:  strings.TrimSpace(groups[0][4]),
	// 		fieldsStr: strings.TrimSpace(groups[0][5]),
	// 		valuesStr: strings.TrimSpace(groups[0][6]),
	// 	}
	// 	if stmt.dbName == "" {
	// 		stmt.dbName = defaultDb
	// 	}
	// 	if err := stmt.parse(); err != nil {
	// 		return nil, err
	// 	}
	// 	return stmt, stmt.validate()
	// }
	// if re := reSelect; re.MatchString(query) {
	// 	groups := re.FindAllStringSubmatch(query, -1)
	// 	stmt := &StmtSelect{
	// 		Stmt:             &Stmt{query: query, conn: c, numInput: 0},
	// 		isCrossPartition: strings.TrimSpace(groups[0][1]) != "",
	// 		collName:         strings.TrimSpace(groups[0][2]),
	// 		dbName:           defaultDb,
	// 		selectQuery:      strings.ReplaceAll(strings.ReplaceAll(query, groups[0][1], ""), groups[0][3], ""),
	// 	}
	// 	if err := stmt.parse(groups[0][3]); err != nil {
	// 		return nil, err
	// 	}
	// 	return stmt, stmt.validate()
	// }
	// if re := reUpdate; re.MatchString(query) {
	// 	groups := re.FindAllStringSubmatch(query, -1)
	// 	stmt := &StmtUpdate{
	// 		Stmt:      &Stmt{query: query, conn: c, numInput: 0},
	// 		dbName:    strings.TrimSpace(groups[0][2]),
	// 		collName:  strings.TrimSpace(groups[0][3]),
	// 		updateStr: strings.TrimSpace(groups[0][4]),
	// 		idStr:     strings.TrimSpace(groups[0][5]),
	// 	}
	// 	if stmt.dbName == "" {
	// 		stmt.dbName = defaultDb
	// 	}
	// 	if err := stmt.parse(); err != nil {
	// 		return nil, err
	// 	}
	// 	return stmt, stmt.validate()
	// }
	// if re := reDelete; re.MatchString(query) {
	// 	groups := re.FindAllStringSubmatch(query, -1)
	// 	stmt := &StmtDelete{
	// 		Stmt:     &Stmt{query: query, conn: c, numInput: 0},
	// 		dbName:   strings.TrimSpace(groups[0][2]),
	// 		collName: strings.TrimSpace(groups[0][3]),
	// 		idStr:    strings.TrimSpace(groups[0][4]),
	// 	}
	// 	if stmt.dbName == "" {
	// 		stmt.dbName = defaultDb
	// 	}
	// 	if err := stmt.parse(); err != nil {
	// 		return nil, err
	// 	}
	// 	return stmt, stmt.validate()
	// }

	return nil, fmt.Errorf("invalid query: %s", query)
}

type OptStrings []string

func (s OptStrings) FirstString() string {
	return s[0]
}

// Stmt is AWS DynamoDB prepared statement handler.
type Stmt struct {
	query    string // the SQL query
	conn     *Conn  // the connection that this prepared statement is bound to
	numInput int    // number of placeholder parameters
	withOpts map[string]OptStrings
}

var reWithOpts = regexp.MustCompile(`(?im)^(\s+|\s*,\s+|\s+,\s*)WITH\s+` + field + `\s*=\s*([\w/\.\*,;:'"-]+)`)

// parseWithOpts parses "WITH..." clause and store result in withOpts map.
// This function returns no error. Sub-implementations may override this behavior.
func (s *Stmt) parseWithOpts(withOptsStr string) error {
	s.withOpts = make(map[string]OptStrings)
	for {
		matches := reWithOpts.FindStringSubmatch(withOptsStr)
		if matches == nil {
			break
		}
		k := strings.TrimSpace(strings.ToUpper(matches[2]))
		s.withOpts[k] = append(s.withOpts[k], strings.TrimSuffix(strings.TrimSpace(matches[3]), ","))
		withOptsStr = withOptsStr[len(matches[0]):]
	}
	return nil
}

// Close implements driver.Stmt.Close.
func (s *Stmt) Close() error {
	return nil
}

// NumInput implements driver.Stmt.NumInput.
func (s *Stmt) NumInput() int {
	return s.numInput
}
