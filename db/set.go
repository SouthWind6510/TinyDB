package db

import (
	"SouthWind6510/TinyDB/data"
	"SouthWind6510/TinyDB/util"
)

func (db *TinyDB) SAdd(key []byte, args ...[]byte) (res int, err error) {
	for _, member := range args {
		if db.setKeydir.IsExists(string(key), string(member)) {
			continue
		}
		entry := data.NewEntry(encodeSubKey(key, member), []byte{}, data.Insert)
		_, err := db.WriteEntry(entry, data.Set)
		if err != nil {
			continue
		}
		db.setKeydir.Set(string(key), string(member))
		res++
	}
	return
}

func (db *TinyDB) SRem(key []byte, args ...[]byte) (res int, err error) {
	for _, member := range args {
		entry := data.NewEntry(encodeSubKey(key, member), []byte{}, data.Delete)
		_, err := db.WriteEntry(entry, data.Set)
		if err != nil {
			continue
		}
		db.setKeydir.Del(string(key), string(member))
		res++
	}
	return
}

func (db *TinyDB) SPop(key []byte, count int) (res []string, err error) {
	sz, err := db.setKeydir.GetMemberCount(string(key))
	if err != nil {
		return nil, err
	}
	res = make([]string, util.MinInt(count, sz))
	for i := 0; i < util.MinInt(count, sz); i++ {
		pop, err := db.setKeydir.Pop(string(key))
		if err != nil {
			continue
		}
		res[i] = pop
	}
	return
}

func (db *TinyDB) SCard(key []byte) (res int, err error) {
	return db.setKeydir.GetMemberCount(string(key))
}

func (db *TinyDB) SMembers(key []byte) (res []string, err error) {
	return db.setKeydir.GetMembers(string(key))
}

func (db *TinyDB) SIsMember(key []byte, member []byte) (res int, err error) {
	exists := db.setKeydir.IsExists(string(key), string(member))
	if exists {
		return 1, nil
	}
	return
}

func (db *TinyDB) SMIsMember(keys []byte, members ...[]byte) (res []int, err error) {
	res = make([]int, len(members))
	for i, member := range members {
		res[i] = 0
		if db.setKeydir.IsExists(string(keys), string(member)) {
			res[i] = 1
		}
	}
	return
}

func (db *TinyDB) SRandMember(key []byte, count int) (res []string, err error) {
	if count > 0 {
		return db.setKeydir.RandMembers(string(key), count)
	}
	count = -count
	res = make([]string, count)
	for i := 0; i < count; i++ {
		member, err := db.setKeydir.RandMember(string(key))
		if err != nil {
			continue
		}
		res[i] = member
	}
	return
}
