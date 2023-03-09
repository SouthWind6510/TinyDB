package constants

import "errors"

var (
	InconsistentCRC  = errors.New("inconsistent CRC")
	FileWriteErr     = errors.New("file write error")
	FileReadErr      = errors.New("file read error")
	OpenFileErr      = errors.New("open file error")
	ReadNullEntryErr = errors.New("read null entry error")
)
