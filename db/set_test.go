package db

import (
	"SouthWind6510/TinyDB/pkg/constants"
	"os"
	"testing"
)

func Test_Set(t *testing.T) {
	_ = os.Setenv(constants.DebugEnv, "1")
	tinyDB := openDB(0)
	defer tinyDB.Close()

	// 1 2 3 4 5
	if res, _ := tinyDB.SAdd([]byte("set1"), []byte("1"), []byte("2"), []byte("3"), []byte("4"), []byte("5"), []byte("5")); res != 5 {
		t.Log(res)
		t.Error("SAdd failed")
	}

	// 1 2 3 4 5
	if res, _ := tinyDB.SIsMember([]byte("set1"), []byte("1")); res != 1 {
		t.Error("SIsMember failed")
	}
	if res, _ := tinyDB.SIsMember([]byte("set1"), []byte("6")); res != 0 {
		t.Error("SIsMember failed")
	}

	// 1 2 3 4 5
	if res, _ := tinyDB.SMIsMember([]byte("set1"), []byte("1"), []byte("2"), []byte("6")); len(res) != 3 || res[0] != 1 || res[1] != 1 || res[2] != 0 {
		t.Error("SMIsMember failed")
	}

	// 2 3 4 5
	if res, _ := tinyDB.SRem([]byte("set1"), []byte("1")); res != 1 {
		t.Error("SRem failed")
	}

	if res, _ := tinyDB.SPop([]byte("set1"), 2); len(res) != 2 {
		t.Error("SPop failed")
	}

	if res, _ := tinyDB.SCard([]byte("set1")); res != 2 {
		t.Error("SCard failed")
	}

	if res, _ := tinyDB.SMembers([]byte("set1")); len(res) != 2 {
		t.Error("SMembers failed")
	}

	if res, _ := tinyDB.SRandMember([]byte("set1"), 10); len(res) != 2 {
		t.Error("SRandMember failed")
	}
	if res, _ := tinyDB.SRandMember([]byte("set1"), -10); len(res) != 10 {
		t.Error("SRandMember failed")
	}
}
