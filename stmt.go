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
	reCreateTable   = regexp.MustCompile(`(?im)^CREATE\s+TABLE` + ifNotExists + `\s+` + field + with + `$`)
	reListTables    = regexp.MustCompile(`(?im)^LIST\s+TABLES?$`)
	reDescribeTable = regexp.MustCompile(`(?im)^DESCRIBE\s+TABLE\s+` + field + `$`)
	reAlterTable    = regexp.MustCompile(`(?im)^ALTER\s+TABLE\s+` + field + with + `$`)
	reDropTable     = regexp.MustCompile(`(?im)^(DROP|DELETE)\s+TABLE` + ifExists + `\s+` + field + `$`)

	reDescribeLSI = regexp.MustCompile(`(?im)^DESCRIBE\s+LSI\s+` + field + `\s+ON\s+` + field + `$`)
	reCreateGSI   = regexp.MustCompile(`(?im)^CREATE\s+GSI` + ifNotExists + `\s+` + field + `\s+ON\s+` + field + with + `$`)
	reDescribeGSI = regexp.MustCompile(`(?im)^DESCRIBE\s+GSI\s+` + field + `\s+ON\s+` + field + `$`)
	reAlterGSI    = regexp.MustCompile(`(?im)^ALTER\s+GSI\s+` + field + `\s+ON\s+` + field + with + `$`)
	reDropGSI     = regexp.MustCompile(`(?im)^(DROP|DELETE)\s+GSI` + ifExists + `\s+` + field + `\s+ON\s+` + field + `$`)

	reInsert = regexp.MustCompile(`(?im)^INSERT\s+INTO\s+`)
	reSelect = regexp.MustCompile(`(?im)^SELECT\s+`)
	reUpdate = regexp.MustCompile(`(?im)^UPDATE\s+`)
	reDelete = regexp.MustCompile(`(?im)^DELETE\s+FROM\s+`)
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
	if re := reListTables; re.MatchString(query) {
		stmt := &StmtListTables{
			Stmt: &Stmt{query: query, conn: c, numInput: 0},
		}
		return stmt, stmt.validate()
	}
	if re := reDescribeTable; re.MatchString(query) {
		groups := re.FindAllStringSubmatch(query, -1)
		stmt := &StmtDescribeTable{
			Stmt:      &Stmt{query: query, conn: c, numInput: 0},
			tableName: strings.TrimSpace(groups[0][1]),
		}
		return stmt, stmt.validate()
	}
	if re := reAlterTable; re.MatchString(query) {
		groups := re.FindAllStringSubmatch(query, -1)
		stmt := &StmtAlterTable{
			Stmt:        &Stmt{query: query, conn: c, numInput: 0},
			tableName:   strings.TrimSpace(groups[0][1]),
			withOptsStr: " " + strings.TrimSpace(groups[0][2]),
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

	if re := reDescribeLSI; re.MatchString(query) {
		groups := re.FindAllStringSubmatch(query, -1)
		stmt := &StmtDescribeLSI{
			Stmt:      &Stmt{query: query, conn: c, numInput: 0},
			tableName: strings.TrimSpace(groups[0][2]),
			indexName: strings.TrimSpace(groups[0][1]),
		}
		return stmt, stmt.validate()
	}

	if re := reCreateGSI; re.MatchString(query) {
		groups := re.FindAllStringSubmatch(query, -1)
		stmt := &StmtCreateGSI{
			Stmt:        &Stmt{query: query, conn: c, numInput: 0},
			ifNotExists: strings.TrimSpace(groups[0][1]) != "",
			indexName:   strings.TrimSpace(groups[0][2]),
			tableName:   strings.TrimSpace(groups[0][3]),
			withOptsStr: " " + strings.TrimSpace(groups[0][4]),
		}
		if err := stmt.parse(); err != nil {
			return nil, err
		}
		return stmt, stmt.validate()
	}
	if re := reDescribeGSI; re.MatchString(query) {
		groups := re.FindAllStringSubmatch(query, -1)
		stmt := &StmtDescribeGSI{
			Stmt:      &Stmt{query: query, conn: c, numInput: 0},
			tableName: strings.TrimSpace(groups[0][2]),
			indexName: strings.TrimSpace(groups[0][1]),
		}
		return stmt, stmt.validate()
	}
	if re := reAlterGSI; re.MatchString(query) {
		groups := re.FindAllStringSubmatch(query, -1)
		stmt := &StmtAlterGSI{
			Stmt:        &Stmt{query: query, conn: c, numInput: 0},
			indexName:   strings.TrimSpace(groups[0][1]),
			tableName:   strings.TrimSpace(groups[0][2]),
			withOptsStr: " " + strings.TrimSpace(groups[0][3]),
		}
		if err := stmt.parse(); err != nil {
			return nil, err
		}
		return stmt, stmt.validate()
	}
	if re := reDropGSI; re.MatchString(query) {
		groups := re.FindAllStringSubmatch(query, -1)
		stmt := &StmtDropGSI{
			Stmt:      &Stmt{query: query, conn: c, numInput: 0},
			tableName: strings.TrimSpace(groups[0][4]),
			indexName: strings.TrimSpace(groups[0][3]),
			ifExists:  strings.TrimSpace(groups[0][2]) != "",
		}
		return stmt, stmt.validate()
	}

	if re := reInsert; re.MatchString(query) {
		stmt := &StmtInsert{
			StmtExecutable: &StmtExecutable{Stmt: &Stmt{query: query, conn: c, numInput: 0}},
		}
		if err := stmt.parse(); err != nil {
			return nil, err
		}
		return stmt, stmt.validate()
	}
	if re := reSelect; re.MatchString(query) {
		stmt := &StmtSelect{
			StmtExecutable: &StmtExecutable{Stmt: &Stmt{query: query, conn: c, numInput: 0}},
		}
		if err := stmt.parse(); err != nil {
			return nil, err
		}
		return stmt, stmt.validate()
	}
	if re := reUpdate; re.MatchString(query) {
		stmt := &StmtUpdate{
			StmtExecutable: &StmtExecutable{Stmt: &Stmt{query: query, conn: c, numInput: 0}},
		}
		if err := stmt.parse(); err != nil {
			return nil, err
		}
		return stmt, stmt.validate()
	}
	if re := reDelete; re.MatchString(query) {
		stmt := &StmtDelete{
			StmtExecutable: &StmtExecutable{Stmt: &Stmt{query: query, conn: c, numInput: 0}},
		}
		if err := stmt.parse(); err != nil {
			return nil, err
		}
		return stmt, stmt.validate()
	}

	return nil, fmt.Errorf("invalid query: %s", query)
}

type OptStrings []string

func (s OptStrings) FirstString() string {
	if len(s) > 0 {
		return s[0]
	}
	return ""
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
