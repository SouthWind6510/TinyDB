package constants

import "errors"

var (
	ErrKeyNotFound             = errors.New("key not found")
	ErrInconsistentCRC         = errors.New("inconsistent CRC")
	ErrReadNullEntry           = errors.New("read null entry")
	ErrWrongNumberArgs         = errors.New("wrong number arguments")
	ErrListLengthLimitExceeded = errors.New("list length limit exceeded")
	ErrListIndexOutOfRange     = errors.New("index out of range")
)
