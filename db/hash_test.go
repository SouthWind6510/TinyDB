package db

import (
	"SouthWind6510/TinyDB/pkg/constants"
	"os"
	"testing"
)

func Test_Hash(t *testing.T) {
	_ = os.Setenv(constants.DebugEnv, "1")
	tinyDB := openDB()

	if res, _ := tinyDB.HSet([]byte("hash"), []byte("a"), []byte("1"), []byte("b"), []byte("2"), []byte("c"), []byte("d")); res != 3 {
		t.Errorf("HSet error")
	}

	if res, _ := tinyDB.HGetAll([]byte("hash")); len(res) != 3 {
		t.Errorf("HGetAll error")
	}

	if res, _ := tinyDB.HGet([]byte("hash"), []byte("a")); res != "1" {
		t.Errorf("HGet error")
	}

	if res, _ := tinyDB.HSetNX([]byte("hash"), []byte("a"), []byte("0")); res != 0 {
		t.Errorf("HSetNX error")
	}
	if res, _ := tinyDB.HSetNX([]byte("hash"), []byte("d"), []byte("1")); res != 1 {
		t.Errorf("HSetNX error")
	}

	if res, _ := tinyDB.HExists([]byte("hash"), []byte("a")); res != 1 {
		t.Errorf("HExists error")
	}

	if res, _ := tinyDB.HLen([]byte("hash")); res != 4 {
		t.Errorf("HLen error")
	}

	if res, _ := tinyDB.HIncrBy([]byte("hash"), []byte("a"), 1); res != 2 {
		t.Errorf("HIncrBy error")
	}
	if _, err := tinyDB.HIncrBy([]byte("hash"), []byte("c"), 1); err != constants.ErrHashValueIsNotInteger {
		t.Errorf("HIncrBy error")
	}

	if res, _ := tinyDB.HKeys([]byte("hash")); len(res) != 4 {
		t.Errorf("HKeys error")
	}

	if res, _ := tinyDB.HVals([]byte("hash")); len(res) != 4 {
		t.Errorf("HVals error")
	}

	if res, _ := tinyDB.HDel([]byte("hash"), []byte("a"), []byte("b"), []byte("c"), []byte("d")); res != 4 {
		t.Errorf("HDel error")
	}
	if res, _ := tinyDB.HLen([]byte("hash")); res != 0 {
		t.Errorf("HDel error")
	}

	tinyDB.Close()
}
