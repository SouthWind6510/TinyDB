package db

import (
	"SouthWind6510/TinyDB/data"
	"SouthWind6510/TinyDB/pkg/constants"
	"strconv"
)

func (db *TinyDB) HSet(key []byte, args ...[]byte) (res int, err error) {
	for i := 0; i+1 < len(args); i += 2 {
		entry := data.NewEntry(encodeSubKey(key, args[i]), args[i+1], data.Insert)
		pos, err := db.WriteEntry(entry, data.Hash)
		if err != nil {
			continue
		}
		db.hashKeydir.Set(string(key), string(args[i]), pos)
		res++
	}
	return
}

func (db *TinyDB) HGet(key []byte, field []byte) (res interface{}, err error) {
	pos, err := db.hashKeydir.Get(string(key), string(field))
	if err != nil {
		return nil, err
	}
	entry, err := db.ReadEntry(data.Hash, pos)
	if err != nil {
		return nil, err
	}
	return string(entry.Value), nil
}

func (db *TinyDB) HGetAll(key []byte) (res map[string]string, err error) {
	res = make(map[string]string)
	fields, err := db.hashKeydir.GetFields(string(key))
	if err != nil {
		return nil, err
	}
	for _, field := range fields {
		pos, err := db.hashKeydir.Get(string(key), field)
		if err != nil {
			continue
		}
		entry, err := db.ReadEntry(data.Hash, pos)
		if err != nil {
			continue
		}
		res[field] = string(entry.Value)
	}
	return
}

func (db *TinyDB) HDel(key []byte, args ...[]byte) (res int, err error) {
	for _, field := range args {
		entry := data.NewEntry(encodeSubKey(key, field), []byte{}, data.Delete)
		_, err := db.WriteEntry(entry, data.Hash)
		if err != nil {
			continue
		}
		db.hashKeydir.Del(string(key), string(field))
		res++
	}
	return
}

func (db *TinyDB) HExists(key []byte, field []byte) (res int, err error) {
	_, err = db.hashKeydir.Get(string(key), string(field))
	if err == constants.ErrKeyNotFound {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	return 1, nil
}

func (db *TinyDB) HLen(key []byte) (res int, err error) {
	return db.hashKeydir.GetFieldCount(string(key))
}

func (db *TinyDB) HKeys(key []byte) (res []string, err error) {
	return db.hashKeydir.GetFields(string(key))
}

func (db *TinyDB) HVals(key []byte) (res []string, err error) {
	fields, err := db.hashKeydir.GetFields(string(key))
	if err != nil {
		return nil, err
	}
	for _, field := range fields {
		pos, err := db.hashKeydir.Get(string(key), field)
		if err != nil {
			continue
		}
		entry, err := db.ReadEntry(data.Hash, pos)
		if err != nil {
			continue
		}
		res = append(res, string(entry.Value))
	}
	return
}

func (db *TinyDB) HIncrBy(key []byte, field []byte, incr int) (res int, err error) {
	pos, err := db.hashKeydir.Get(string(key), string(field))
	if err != nil {
		return 0, err
	}
	entry, err := db.ReadEntry(data.Hash, pos)
	if err != nil {
		return 0, err
	}
	cur, err := strconv.Atoi(string(entry.Value))
	if err != nil {
		return 0, constants.ErrHashValueIsNotInteger
	}
	res = cur + incr
	entry = data.NewEntry(encodeSubKey(key, field), []byte(strconv.Itoa(res)), data.Insert)
	pos, err = db.WriteEntry(entry, data.Hash)
	if err != nil {
		return 0, err
	}
	db.hashKeydir.Set(string(key), string(field), pos)
	return
}

func (db *TinyDB) HMGet(key []byte, fields ...[]byte) (res []string, err error) {
	for _, field := range fields {
		pos, err := db.hashKeydir.Get(string(key), string(field))
		if err != nil {
			continue
		}
		entry, err := db.ReadEntry(data.Hash, pos)
		if err != nil {
			continue
		}
		res = append(res, string(entry.Value))
	}
	return
}

func (db *TinyDB) HMSet(key []byte, args ...[]byte) (err error) {
	for i := 0; i+1 < len(args); i += 2 {
		entry := data.NewEntry(encodeSubKey(key, args[i]), args[i+1], data.Insert)
		pos, err := db.WriteEntry(entry, data.Hash)
		if err != nil {
			continue
		}
		db.hashKeydir.Set(string(key), string(args[i]), pos)
	}
	return
}

func (db *TinyDB) HSetNX(key []byte, field []byte, value []byte) (res int, err error) {
	exists, err := db.HExists(key, field)
	if err != nil {
		return 0, err
	}
	if exists == 1 {
		return 0, nil
	}
	entry := data.NewEntry(encodeSubKey(key, field), value, data.Insert)
	pos, err := db.WriteEntry(entry, data.Hash)
	if err != nil {
		return 0, err
	}
	db.hashKeydir.Set(string(key), string(field), pos)
	return 1, nil
}
