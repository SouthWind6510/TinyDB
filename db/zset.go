package db

import (
	"SouthWind6510/TinyDB/data"
	"SouthWind6510/TinyDB/pkg/constants"
	"SouthWind6510/TinyDB/pkg/logger"
	"SouthWind6510/TinyDB/util"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"strconv"
	"time"
)

func (db *TinyDB) ZSetInsertEntry(key []byte, member []byte, score float64) (err error) {
	entry := data.NewEntry(encodeSubKey(key, member), []byte(fmt.Sprintf("%v", score)), data.Insert)
	_, err = db.WriteEntry(entry, data.ZSet)
	return
}

func (db *TinyDB) ZSetDeleteEntry(key []byte, member []byte) (err error) {
	entry := data.NewEntry(encodeSubKey(key, member), []byte{}, data.Delete)
	_, err = db.WriteEntry(entry, data.ZSet)
	return
}

// ZAdd
// opt1: NX | XX
// opt2: GT | LT
// opt3: CH
// opt4: INCR
func (db *TinyDB) ZAdd(key []byte, opt1, opt2, opt3, opt4 string, args ...[]byte) (res int, err error) {
	for i := 0; i+1 < len(args); i += 2 {
		var score float64
		// redis 协议要求score取值为[2^53, -2^53]
		if string(args[i]) == "-inf" {
			score = math.MinInt64
		} else if string(args[i]) == "+inf" {
			score = math.MaxInt64
		} else {
			score, err = strconv.ParseFloat(string(args[i]), 64)
			if err != nil {
				logger.Log.Warnf("zadd parse float err: %v, string: %v", err, string(args[i]))
				continue
			}
		}
		getScore, err := db.zsetKeydir.GetScore(string(key), string(args[i+1]))
		if err != nil && !errors.Is(err, constants.ErrKeyNotFound) && !errors.Is(err, constants.ErrMemberNotExist) {
			logger.Log.Errorf("zadd get score err: %v", err)
			continue
		}
		exists := err == nil
		// 只添加不更新
		if opt1 == "nx" && exists {
			continue
		}
		// 只更新不添加
		if opt1 == "xx" && !exists {
			continue
		}
		// 只添加或更新比当前score小的
		if exists && opt2 == "lt" && score >= getScore {
			continue
		}
		// 只添加或更新比当前score大的
		if exists && opt2 == "gt" && score <= getScore {
			continue
		}
		if opt4 == "incr" {
			score += getScore
		}
		// 持久化
		err = db.ZSetInsertEntry(key, args[i+1], score)
		if err != nil {
			logger.Log.Errorf("zadd insert entry err: %v", err)
			continue
		}
		// 更新索引
		if exists && score != getScore {
			db.zsetKeydir.Update(string(key), string(args[i+1]), getScore, score)
		} else {
			db.zsetKeydir.Set(string(key), string(args[i+1]), score)
		}
		if opt3 == "ch" && (!exists || score != getScore) {
			res++
		} else if !exists {
			res++
		}
	}
	return
}

func (db *TinyDB) ZCard(key []byte) (res int64, err error) {
	return db.zsetKeydir.GetMemberCount(string(key)), nil
}

// ZCount score在区间内的member数，仅支持闭区间
func (db *TinyDB) ZCount(key []byte, min, max float64) (res int64, err error) {
	return db.zsetKeydir.GetCountByScore(string(key), min, max), nil
}

func (db *TinyDB) ZIncrBy(key []byte, increment float64, member []byte) (res float64, err error) {
	getScore, err := db.zsetKeydir.GetScore(string(key), string(member))
	if err != nil && !errors.Is(err, constants.ErrKeyNotFound) {
		return
	}
	exists := err == nil
	// 持久化
	err = db.ZSetInsertEntry(key, member, getScore+increment)
	if err != nil {
		return
	}
	if !exists {
		db.zsetKeydir.Set(string(key), string(member), increment)
	} else {
		db.zsetKeydir.Update(string(key), string(member), getScore, getScore+increment)
	}
	return getScore + increment, nil
}

func (db *TinyDB) ZMScore(key []byte, members ...[]byte) (res []interface{}, err error) {
	res = make([]interface{}, len(members))
	for i, member := range members {
		res[i], err = db.zsetKeydir.GetScore(string(key), string(member))
		if err != nil {
			res[i] = nil
		}
	}
	return res, nil
}

func (db *TinyDB) ZPop(key []byte, isLeft bool, count int) (res []interface{}, err error) {
	length := db.zsetKeydir.GetMemberCount(string(key))
	res = make([]interface{}, util.MinInt(count, int(length))*2)
	for i := 0; i < len(res); i += 2 {
		rank := int64(-1)
		if isLeft {
			rank = int64(0)
		}
		member, score, _ := db.zsetKeydir.GetMemberByRank(string(key), rank)
		// 持久化
		err = db.ZSetDeleteEntry(key, []byte(member))
		if err != nil {
			continue
		}
		// 更新索引
		db.zsetKeydir.Del(string(key), member, score)
		res[i] = member
		res[i+1] = score
	}
	return
}

func (db *TinyDB) ZRandMember(key []byte, count int, withScores bool) (res []interface{}, err error) {
	length := db.zsetKeydir.GetMemberCount(string(key))
	if count > 0 {
		res = make([]interface{}, util.MinInt(count, int(length))*(1+util.BoolToInt(withScores)))
		for i := 0; i < util.MinInt(count, int(length)); i++ {
			member, score, _ := db.zsetKeydir.GetMemberByRank(string(key), int64(i))
			if withScores {
				res[2*i] = member
				res[2*i+1] = score
			} else {
				res[i] = member
			}
		}
	} else {
		res = make([]interface{}, -count*(1+util.BoolToInt(withScores)))
		rand.Seed(time.Now().Unix())
		for i := 0; i < -count; i++ {
			rank := rand.Int63n(length)
			member, score, _ := db.zsetKeydir.GetMemberByRank(string(key), rank)
			if withScores {
				res[2*i] = member
				res[2*i+1] = score
			} else {
				res[i] = member
			}
		}
	}
	return
}

func (db *TinyDB) ZRange(key []byte, start, end float64, byScore, rev bool, withScores int) (res []interface{}, err error) {
	var members []string
	var scores []float64
	if byScore {
		members, scores, _ = db.zsetKeydir.GetRangeByScore(string(key), start, end, rev)
	} else {
		members, scores, _ = db.zsetKeydir.GetRangeByRank(string(key), int64(start), int64(end), rev)
	}
	res = make([]interface{}, len(members)*(1+withScores))
	for i := 0; i < len(members); i++ {
		if withScores == 1 {
			res[2*i] = members[i]
			res[2*i+1] = scores[i]
		} else {
			res[i] = members[i]
		}
	}
	return
}

func (db *TinyDB) ZRank(key []byte, member []byte, withScore bool) (res []interface{}, err error) {
	rank, score, err := db.zsetKeydir.GetRank(string(key), string(member))
	if err != nil {
		return nil, err
	}
	res = append(res, rank)
	if withScore {
		res = append(res, score)
	}
	return
}

func (db *TinyDB) ZRem(key []byte, members ...[]byte) (res int64, err error) {
	for _, member := range members {
		// 持久化
		err = db.ZSetDeleteEntry(key, member)
		if err != nil {
			continue
		}
		// 更新索引
		if db.zsetKeydir.DeleteWithoutScore(string(key), string(member)) {
			res++
		}
	}
	return
}

func (db *TinyDB) ZRemRange(key []byte, start, end float64, byScore bool) (res int64, err error) {
	var members []string
	if byScore {
		members = db.zsetKeydir.DeleteRangeByScore(string(key), start, end)
	} else {
		members = db.zsetKeydir.DeleteRangeByRank(string(key), int64(start), int64(end))
	}
	// 持久化
	for _, member := range members {
		_ = db.ZSetDeleteEntry(key, []byte(member))
	}
	return int64(len(members)), nil
}
