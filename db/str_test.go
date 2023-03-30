package db

import (
	"SouthWind6510/TinyDB/pkg/constants"
	"os"
	"testing"
)

func Test_Str(t *testing.T) {
	_ = os.Setenv(constants.DebugEnv, "1")
	tinyDB := openDB()
	defer tinyDB.Close()

	if err := tinyDB.Set([]byte("key1"), []byte("value1")); err != nil {
		t.Error("Set failed")
	}

	if res, _ := tinyDB.Get([]byte("key1")); string(res) != "value1" {
		t.Error("Get failed")
	}

	if res := tinyDB.SetNX([]byte("key1"), []byte("value2")); res != 0 {
		t.Error("SetNX failed")
	}
	if res := tinyDB.SetNX([]byte("key2"), []byte("value2")); res != 1 {
		t.Error("SetNX failed")
	}

	if res := tinyDB.MSetNX([]byte("key1"), []byte("value1"), []byte("key2"), []byte("value2"), []byte("key3"), []byte("value3")); res != 0 {
		t.Error("MSetNX failed")
	}
	if res := tinyDB.MSetNX([]byte("key3"), []byte("value3"), []byte("key4"), []byte("value4")); res != 1 {
		t.Error("MSetNX failed")
	}

	if res, _ := tinyDB.SetRange([]byte("key3"), []byte("suf"), 10); res != 13 {
		t.Error("SetRange failed")
	}
	if res, _ := tinyDB.SetRange([]byte("key5"), []byte("value5"), 0); res != 6 {
		t.Error("SetRange failed")
	}
	tinyDB.SetRange([]byte("key5"), []byte("b"), 1)
	if res, _ := tinyDB.Get([]byte("key5")); string(res) != "vblue5" {
		t.Error("SetRange failed")
	}
	tinyDB.SetRange([]byte("key5"), []byte("aaa"), 4)
	if res, _ := tinyDB.Get([]byte("key5")); string(res) != "vbluaaa" {
		t.Error("SetRange failed")
	}

	if res, _ := tinyDB.GetRange([]byte("key5"), 0, 3); string(res) != "vblu" {
		t.Error("GetRange failed")
	}
	if res, _ := tinyDB.GetRange([]byte("key5"), 0, 10); string(res) != "vbluaaa" {
		t.Error("GetRange failed")
	}
	if res, _ := tinyDB.GetRange([]byte("key5"), -2, -1); string(res) != "aa" {
		t.Error("GetRange failed")
	}

	if res, _ := tinyDB.Incr([]byte("key6"), 1); res != 1 {
		t.Error("Incr failed")
	}
	if res, _ := tinyDB.Incr([]byte("key6"), 1); res != 2 {
		t.Error("Incr failed")
	}

	if res, _ := tinyDB.IncrByFloat([]byte("key7"), 1.2); res != 1.2 {
		t.Error("IncrByFloat failed")
	}
	if res, _ := tinyDB.IncrByFloat([]byte("key7"), 1.2); res != 2.4 {
		t.Error("IncrByFloat failed")
	}

	if res, _ := tinyDB.Append([]byte("key5"), []byte("value5")); res != 13 {
		t.Error("Append failed")
	}

	if res := tinyDB.GetDel([]byte("key5")); res != "vbluaaavalue5" {
		t.Error("GetDel failed")
	}
	if res, _ := tinyDB.Get([]byte("key5")); res != nil {
		t.Error("GetDel failed")
	}
}
