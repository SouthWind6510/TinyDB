package db

import (
	"testing"
)

func Test_ZSet(t *testing.T) {
	//_ = os.Setenv(constants.DebugEnv, "1")
	tinyDB := openDB(0)
	defer tinyDB.Close()

	if res, _ := tinyDB.ZCard([]byte("zset1")); res != 0 {
		t.Log(res)
		t.Errorf("ZCard error")
	}
	// a 1
	// c 3
	// d 4
	// b 5
	if res, _ := tinyDB.ZAdd([]byte("zset1"), "", "", "", "", []byte("1"), []byte("a"), []byte("2"), []byte("b"), []byte("3"), []byte("c")); res != 3 {
		t.Errorf("ZAdd error")
	}
	if res, _ := tinyDB.ZAdd([]byte("zset1"), "", "gt", "ch", "", []byte("5"), []byte("b"), []byte("4"), []byte("d")); res != 2 {
		t.Errorf("ZAdd error")
	}

	if res, _ := tinyDB.ZCard([]byte("zset1")); res != 4 {
		t.Errorf("ZCard error")
	}

	if res, _ := tinyDB.ZCount([]byte("zset1"), 1, 5); res != 4 {
		t.Errorf("ZCount error")
	}

	// a 1
	// c 3
	// b 5
	// d 5
	if res, _ := tinyDB.ZIncrBy([]byte("zset1"), 1, []byte("d")); res != 5 {
		t.Errorf("ZIncrBy error")
	}

	if res, _ := tinyDB.ZMScore([]byte("zset1"), []byte("a"), []byte("b"), []byte("c"), []byte("d"), []byte("e")); res[0] != 1.0 || res[1] != 5.0 || res[2] != 3.0 || res[3] != 5.0 || res[4] != nil {
		t.Errorf("ZMScore error")
	}

	// c 3
	// b 5
	if res, _ := tinyDB.ZPop([]byte("zset1"), true, 1); res[0] != "a" || res[1] != 1.0 {
		t.Errorf("ZPop error")
	}
	if res, _ := tinyDB.ZPop([]byte("zset1"), false, 1); res[0] != "d" || res[1] != 5.0 {
		t.Errorf("ZPop error")
	}

	if res, _ := tinyDB.ZRandMember([]byte("zset1"), 2, false); res[0] != "c" || res[1] != "b" {
		t.Errorf("ZRandMember error")
	}

	if res, _ := tinyDB.ZRange([]byte("zset1"), 0, 1, false, false, 1); res[0] != "c" || res[1] != 3.0 || res[2] != "b" || res[3] != 5.0 {
		t.Errorf("ZRange error")
	}
	if res, _ := tinyDB.ZRange([]byte("zset1"), 1, 5, true, false, 1); res[0] != "c" || res[1] != 3.0 || res[2] != "b" || res[3] != 5.0 {
		t.Errorf("ZRange error")
	}
	if res, _ := tinyDB.ZRange([]byte("zset1"), 1, 5, true, true, 1); res[0] != "b" || res[1] != 5.0 || res[2] != "c" || res[3] != 3.0 {
		t.Errorf("ZRange error")
	}

	// a 1
	// c 3
	// d 4
	// b 5
	// f 6
	if res, _ := tinyDB.ZAdd([]byte("zset1"), "", "", "", "", []byte("1"), []byte("a"), []byte("4"), []byte("d"), []byte("6"), []byte("f")); res != 3 {
		t.Errorf("ZAdd error")
	}

	if res, _ := tinyDB.ZRank([]byte("zset1"), []byte("f"), true); res[0] != int64(4) || res[1] != 6.0 {
		t.Errorf("ZRank error")
	}

	// c 3
	// d 4
	// b 5
	// f 6
	if res, _ := tinyDB.ZRem([]byte("zset1"), []byte("a")); res != 1 {
		t.Errorf("ZRem error")
	}

	if res, _ := tinyDB.ZRemRange([]byte("zset1"), 0, 3, false); res != 4 {
		t.Errorf("ZRemRange by rank error")
	}

	if res, _ := tinyDB.ZAdd([]byte("zset1"), "", "", "", "", []byte("1"), []byte("a"), []byte("4"), []byte("d"), []byte("6"), []byte("f")); res != 3 {
		t.Errorf("ZAdd error")
	}
	if res, _ := tinyDB.ZRemRange([]byte("zset1"), 0, 6, true); res != 3 {
		t.Errorf("ZRemRange by score error")
	}

	if res, _ := tinyDB.ZCard([]byte("zset1")); res != 0 {
		t.Errorf("ZCard error")
	}
}
