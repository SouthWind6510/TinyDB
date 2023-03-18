package db

import (
	"SouthWind6510/TinyDB/data"
	"SouthWind6510/TinyDB/pkg/constants"
	"SouthWind6510/TinyDB/util"
	"encoding/binary"
	"math"
	"strconv"
	"strings"

	"github.com/pkg/errors"
)

const (
	MetaIndex    = -1
	ListLenLimit = math.MaxUint32
)

type ListMeta struct {
	head int64
	tail int64
}

func (db *TinyDB) getListMeta(key []byte) (head, tail uint32, err error) {
	pos, err := db.listKeydir.Get(string(key), MetaIndex)
	if errors.Is(err, constants.ErrKeyNotFound) {
		head = ListLenLimit / 2
		tail = ListLenLimit/2 - 1
		return head, tail, nil
	} else if err != nil {
		return 0, 0, err
	}
	entry, err := db.ReadEntry(data.List, pos)
	if err != nil {
		return 0, 0, err
	}
	head = binary.LittleEndian.Uint32(entry.Value[:4])
	tail = binary.LittleEndian.Uint32(entry.Value[4:])
	return
}

func (db *TinyDB) getListKeyIndex(k string) (key string, res int) {
	strs := strings.Split(k, "##")
	key = strs[0]
	res, _ = strconv.Atoi(strs[len(strs)-1])
	return
}

func (db *TinyDB) getListKey(key []byte, index int) []byte {
	return []byte(string(key) + "##" + strconv.Itoa(index))
}

// 更新ListMeta
func (db *TinyDB) saveListMeta(key []byte, head, tail uint32) {
	// TODO 失败怎么办
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint32(buf[:4], head)
	binary.LittleEndian.PutUint32(buf[4:], tail)
	entry := data.NewEntry(key, buf, data.InsertListMeta)
	pos, err := db.WriteEntry(entry, data.List)
	if err != nil {
		return
	}
	db.listKeydir.Set(string(key), MetaIndex, pos)
}

func (db *TinyDB) LPush(key []byte, isLeft bool, values ...[]byte) (len int, err error) {
	head, tail, err := db.getListMeta(key)
	if err != nil {
		return 0, err
	}

	// 写入list节点
	for _, value := range values {
		var index int
		index = int(tail) + 1
		if isLeft {
			index = int(head) - 1
		}
		if index < 0 || index >= ListLenLimit {
			err = constants.ErrListLengthLimitExceeded
			break
		}
		entry := data.NewEntry(db.getListKey(key, index), value, data.Insert)
		pos, err := db.WriteEntry(entry, data.List)
		if err != nil {
			continue
		}
		db.listKeydir.Set(string(key), index, pos)
		if isLeft {
			head--
		} else {
			tail++
		}
	}

	db.saveListMeta(key, head, tail)
	return int(tail - head + 1), err
}

func (db *TinyDB) LPop(key []byte, count int, isLeft bool) (res []string, err error) {
	res = make([]string, 0)
	head, tail, err := db.getListMeta(key)
	if err != nil {
		return res, err
	}

	for i := 0; i < count; i++ {
		if tail < head {
			break
		}
		// 读list节点
		var index uint32
		if isLeft {
			index = head
			head++
		} else {
			index = tail
			tail--
		}
		pos, err := db.listKeydir.Get(string(key), int(index))
		if err != nil {
			continue
		}
		entry, err := db.ReadEntry(data.List, pos)
		if err != nil {
			continue
		}
		res = append(res, string(entry.Value))

		// 删除节点
		entry = data.NewEntry(db.getListKey(key, int(index)), []byte{}, data.Delete)
		pos, err = db.WriteEntry(entry, data.List)
		if err != nil {
			continue
		}
		db.listKeydir.Set(string(key), int(index), pos)
	}

	db.saveListMeta(key, head, tail)
	return
}

func (db *TinyDB) LIndex(key []byte, offset int) (res interface{}, err error) {
	head, tail, err := db.getListMeta(key)
	if err != nil {
		return nil, err
	}

	index := int(head) + offset
	if offset < 0 {
		index = int(tail) + offset + 1
	}
	if index < int(head) || index > int(tail) {
		return nil, constants.ErrListLengthLimitExceeded
	}
	pos, err := db.listKeydir.Get(string(key), index)
	if err != nil {
		return nil, err
	}
	entry, err := db.ReadEntry(data.List, pos)
	if err != nil {
		return nil, err
	}
	return string(entry.Value), nil
}

func (db *TinyDB) LLen(key []byte) (len int, err error) {
	head, tail, err := db.getListMeta(key)
	if err != nil {
		return
	}
	return int(tail - head + 1), nil
}

func (db *TinyDB) LRange(key []byte, sOffset, eOffset int) (res []string, err error) {
	res = make([]string, 0)
	head, tail, err := db.getListMeta(key)
	if err != nil || head > tail {
		return
	}

	start := util.MinInt(int(head)+sOffset, int(tail))
	if sOffset < 0 {
		start = util.MaxInt(int(tail)+sOffset+1, int(head))
	}
	end := util.MinInt(int(head)+eOffset, int(tail))
	if eOffset < 0 {
		end = util.MaxInt(int(tail)+eOffset+1, int(head))
	}

	for index := start; index <= end; index++ {
		pos, err := db.listKeydir.Get(string(key), index)
		if err != nil {
			continue
		}
		entry, err := db.ReadEntry(data.List, pos)
		if err != nil {
			continue
		}
		res = append(res, string(entry.Value))
	}
	return
}

func (db *TinyDB) LSet(key []byte, offset int, value []byte) (err error) {
	head, tail, err := db.getListMeta(key)
	if err != nil {
		return
	}

	index := int(head) + offset
	if offset < 0 {
		index = int(tail) + offset + 1
	}
	if index < int(head) || index > int(tail) {
		return constants.ErrListIndexOutOfRange
	}
	entry := data.NewEntry(db.getListKey(key, index), value, data.Insert)
	pos, err := db.WriteEntry(entry, data.List)
	if err != nil {
		return
	}
	db.listKeydir.Set(string(key), index, pos)
	return
}
