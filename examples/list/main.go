package main

import (
	"SouthWind6510/TinyDB/db"
	"SouthWind6510/TinyDB/pkg/constants"
	"fmt"
	"os"
)

func main() {
	_ = os.Setenv(constants.DebugEnv, "1")

	opt := db.DefaultOptions("/Users/southwind/TinyDB/0")
	opt.FileSizeLimit = 1 << 10
	tinyDB, err := db.Open(opt)
	if err != nil {
		panic(fmt.Sprintf("%+v\n", err))
	}

	println("push elements")
	lLen, err := tinyDB.LPush([]byte("list"), true, [][]byte{[]byte("a"), []byte("b"), []byte("c")}...)
	if err != nil {
		panic(fmt.Sprintf("%+v\n", err))
	}
	fmt.Printf("list length: %v\n", lLen)

	lLen, err = tinyDB.LPush([]byte("list"), false, [][]byte{[]byte("d"), []byte("e"), []byte("f")}...)
	if err != nil {
		panic(fmt.Sprintf("%+v\n", err))
	}
	fmt.Printf("list length: %v\n", lLen)

	println("\npop elements")
	pops, err := tinyDB.LPop([]byte("list"), 1, true)
	if err != nil {
		panic(fmt.Sprintf("%+v\n", err))
	}
	for _, pop := range pops {
		println(pop)
	}
	pops, err = tinyDB.LPop([]byte("list"), 2, false)
	if err != nil {
		panic(fmt.Sprintf("%+v\n", err))
	}
	for _, pop := range pops {
		println(pop)
	}

	println("\nrange elements")
	strings, err := tinyDB.LRange([]byte("list"), 0, -1)
	if err != nil {
		panic(fmt.Sprintf("%+v\n", err))
	}
	for _, str := range strings {
		println(str)
	}

	println("\nset elements")
	err = tinyDB.LSet([]byte("list"), 2, []byte("gg"))
	if err != nil {
		panic(fmt.Sprintf("%+v\n", err))
	}

	println("\nrange elements")
	strings, err = tinyDB.LRange([]byte("list"), 0, -1)
	if err != nil {
		panic(fmt.Sprintf("%+v\n", err))
	}
	for _, str := range strings {
		println(str)
	}
	tinyDB.Close()
}
