package godynamo

import (
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

var (
	reSqlInsert = regexp.MustCompile(`(?is)^INSERT\s+INTO\s+` + field + `\s*\(([^)]*?)\)\s*VALUES\s*\(([^)]*?)\)$`)

	ErrNotValidInsertStm       = errors.New("input is not an invalid INSERT statement")
	ErrFieldsAndValuesNotMatch = errors.New("number of fields and values mismatch")

	reValNull               = regexp.MustCompile(`(?i)(null)\s*,?`)
	reValNumber             = regexp.MustCompile(`([\d\.xe+-]+)\s*,?`)
	reValBoolean            = regexp.MustCompile(`(?i)(true|false)\s*,?`)
	reValStringDoubleQuoted = regexp.MustCompile(`(?i)("(\\"|[^"])*?")\s*,?`)
	reValStringSingleQuoted = regexp.MustCompile(`(?i)'(?:[^']+|'')*'\s*,?`)
	reValPlaceholder        = regexp.MustCompile(`(?i)\?\s*,?`)
	reValRaw                = regexp.MustCompile(`(?i)([^,]+)\s*,?`)
)

type valPlaceholder struct{}

func _isSpace(r rune) bool {
	switch r {
	case '\t', '\n', '\v', '\f', '\r', ' ', 0x85, 0xA0, '=':
		return true
	default:
		return false
	}
}

func _parseValue(input string, separator rune) (value interface{}, leftOver string, err error) {
	if loc := reValPlaceholder.FindStringIndex(input); loc != nil && loc[0] == 0 {
		return valPlaceholder{}, input[loc[1]:], nil
	}
	if loc := reValNull.FindStringIndex(input); loc != nil && loc[0] == 0 {
		return nil, input[loc[1]:], nil
	}
	if loc := reValNumber.FindStringIndex(input); loc != nil && loc[0] == 0 {
		token := strings.TrimFunc(input[loc[0]:loc[1]], func(r rune) bool { return _isSpace(r) || r == separator })
		var data interface{}
		err := json.Unmarshal([]byte(token), &data)
		if err != nil {
			err = errors.New("(number) cannot parse query, invalid token at: " + token)
		}
		return data, input[loc[1]:], err
	}
	if loc := reValBoolean.FindStringIndex(input); loc != nil && loc[0] == 0 {
		token := strings.TrimFunc(input[loc[0]:loc[1]], func(r rune) bool { return _isSpace(r) || r == separator })
		var data bool
		err := json.Unmarshal([]byte(token), &data)
		if err != nil {
			err = errors.New("(bool) cannot parse query, invalid token at: " + token)
		}
		return data, input[loc[1]:], err
	}
	if loc := reValStringDoubleQuoted.FindStringIndex(input); loc != nil && loc[0] == 0 {
		val, err := strconv.Unquote(strings.TrimFunc(input[loc[0]:loc[1]], func(r rune) bool { return _isSpace(r) || r == separator }))
		if err != nil {
			err = errors.New("(unquote) cannot parse query, invalid token at: " + val)
		} else {
			val = strings.ReplaceAll(val, "\a", `\a`)
			val = strings.ReplaceAll(val, "\b", `\b`)
			val = strings.ReplaceAll(val, "\f", `\f`)
			val = strings.ReplaceAll(val, "\n", `\n`)
			val = strings.ReplaceAll(val, "\r", `\r`)
			val = strings.ReplaceAll(val, "\t", `\t`)
			val = strings.ReplaceAll(val, "\v", `\v`)
			// string value must be single-quoted (https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/ql-reference.insert.html)
			val = "'" + val + "'"
		}
		return val, input[loc[1]:], err
	}
	if loc := reValStringSingleQuoted.FindStringIndex(input); loc != nil && loc[0] == 0 {
		val := strings.TrimFunc(input[loc[0]:loc[1]], func(r rune) bool { return _isSpace(r) || r == separator })
		val = val[1 : len(val)-1]
		// string value must be single-quoted (https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/ql-reference.insert.html)
		val = "'" + val + "'"
		return val, input[loc[1]:], err
	}
	if loc := reValRaw.FindStringIndex(input); loc != nil && loc[0] == 0 {
		val := strings.TrimFunc(input[loc[0]:loc[1]], func(r rune) bool { return _isSpace(r) || r == separator })
		return val, input[loc[1]:], nil
	}
	return nil, input, errors.New("cannot parse query, invalid token at: " + input)
}

// TransformInsertStmToPartiQL converts an INSERT statement to a PartiQL statement.
//
// e.g. INSERT INTO table_name (field1, field2, field3) VALUES ('val1', ?, 3) will be converted to
// INSERT INTO table_name VALUE {'field1': 'val1', 'field2': ?, 'field3': 3}
//
// Note:
//
//   - table name is automatically double-quoted per PartiQL syntax.
//   - field name is automatically single-quoted per PartiQL syntax.
//   - value is automatically single-quoted if it is a string per PartiQL syntax.
//   - Order of fields after conversion is the same as the order of fields in the original INSERT statement.
//   - Other than the above, the value is kept as-is! It is the caller's responsibility to ensure the value is valid.
//   - PartiQL syntax for INSERT statement: https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/ql-reference.insert.html
//
// @Available since <<VERSION>>
func TransformInsertStmToPartiQL(insStm string) (string, error) {
	insSql := strings.TrimSpace(insStm)
	if !reSqlInsert.MatchString(insSql) {
		return insStm, ErrNotValidInsertStm
	}
	groups := reSqlInsert.FindAllStringSubmatch(insSql, -1)
	tableName := strings.Trim(strings.TrimSpace(groups[0][1]), `'"`)

	fieldsStr := strings.TrimSpace(groups[0][2])
	fields := regexp.MustCompile(`[,\s]+`).Split(fieldsStr, -1)

	valuesStr := strings.TrimSpace(groups[0][3])
	values := make([]interface{}, 0)
	for temp := strings.TrimSpace(valuesStr); temp != ""; temp = strings.TrimSpace(temp) {
		value, leftOver, err := _parseValue(temp, ',')
		if err == nil {
			if value == nil {
				values = append(values, "NULL")
			} else {
				switch v := value.(type) {
				case valPlaceholder:
					values = append(values, "?")
				default:
					values = append(values, v)
				}
			}
			temp = leftOver
			continue
		}
		return insSql, err
	}

	if len(fields) != len(values) {
		return insSql, ErrFieldsAndValuesNotMatch
	}

	fieldsAndVals := make([]string, 0)
	for i, field := range fields {
		// field name must be single-quoted (https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/ql-reference.insert.html)
		fieldsAndVals = append(fieldsAndVals, fmt.Sprintf(`'%s': %v`, field, values[i]))
	}

	// table name must be double-quoted (https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/ql-reference.insert.html)
	finalSql := fmt.Sprintf(`INSERT INTO "%s" VALUE {%s}`, tableName, strings.Join(fieldsAndVals, ", "))
	return finalSql, nil
}
