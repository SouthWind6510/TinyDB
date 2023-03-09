package main

import (
	"SouthWind6510/TinyDB/db"
	"fmt"
)

func main() {
	opt := db.DefaultOptions("/Users/southwind/TinyDB")
	opt.FileSizeLimit = 1 << 10
	tinyDB, err := db.Open(opt)
	if err != nil {
		panic(fmt.Sprintf("%+v\n", err))
	}
	err = tinyDB.Set([]byte("hello"), []byte("world"))
	if err != nil {
		panic(fmt.Sprintf("%+v\n", err))
	}
	err = tinyDB.Set([]byte("hello"), []byte("world2"))
	if err != nil {
		panic(fmt.Sprintf("%+v\n", err))
	}
	bytes, err := tinyDB.Get([]byte("hello"))
	if err != nil {
		panic(fmt.Sprintf("%+v\n", err))
	}
	fmt.Printf("hello: %v", string(bytes))
}
