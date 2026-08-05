package http_storage

import "fmt"

var (
	ErrBaseURLEmpty        = fmt.Errorf("BaseURL can not empty")
	ErrConnectionManagerNil = fmt.Errorf("ConnectionManager can not nil")
)