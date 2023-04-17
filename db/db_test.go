package db

import (
	"fmt"
)

func openDB(fileSize int64) *TinyDB {
	opt := DefaultOptions("/Users/southwind/TinyDB/test/0")
	opt.FileSizeLimit = fileSize
	if fileSize == 0 {
		opt.FileSizeLimit = 1 << 10
	}
	tinyDB, err := Open(opt)
	if err != nil {
		panic(fmt.Sprintf("%+v\n", err))
	}
	return tinyDB
}
