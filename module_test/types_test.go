package godynamo_test

import (
	"database/sql/driver"
	"time"
)

var _ driver.Valuer = (*JSTRFC3339)(nil)

type JSTRFC3339 time.Time

func (j JSTRFC3339) Value() (driver.Value, error) {
	jst, err := time.LoadLocation("Asia/Tokyo")
	if err != nil {
		panic(err)
	}
	return time.Time(j).In(jst).Format(time.RFC3339), nil
}
