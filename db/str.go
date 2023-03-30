package db

import (
	"SouthWind6510/TinyDB/data"
	"SouthWind6510/TinyDB/pkg/constants"
	"SouthWind6510/TinyDB/util"
	"errors"
	"fmt"
	"strconv"
)

func (db *TinyDB) Set(key, value []byte) (err error) {
	entry := data.NewEntry(key, value, data.Insert)
	pos, err := db.WriteEntry(entry, data.String)
	if err != nil {
		return err
	}
	db.strKeydir.Set(string(key), pos)
	return
}

func (db *TinyDB) SetNX(key, value []byte) (res int) {
	_, err := db.strKeydir.Get(string(key))
	if errors.Is(err, constants.ErrKeyNotFound) {
		return util.BoolToInt(db.Set(key, value) == nil)
	}
	return 0
}

func (db *TinyDB) MSetNX(args ...[]byte) (res int) {
	for i := 0; i < len(args); i += 2 {
		_, err := db.strKeydir.Get(string(args[i]))
		if !errors.Is(err, constants.ErrKeyNotFound) {
			return 0
		}
	}
	for i := 0; i < len(args); i += 2 {
		_ = db.Set(args[i], args[i+1])
	}
	return 1
}

func (db *TinyDB) SetRange(key, value []byte, offset int) (res int, err error) {
	bytes, err := db.Get(key)
	str := string(bytes)
	if errors.Is(err, constants.ErrKeyNotFound) {
		str = ""
	}
	runes := []rune(str)
	if offset+len(string(value)) > len(runes) {
		runes = append(runes, make([]rune, offset+len(string(value))-len(runes))...)
	}
	newRunes := append(runes[:offset], []rune(string(value))...)
	newRunes = append(newRunes, runes[offset+len(value):]...)
	newString := string(newRunes)
	_ = db.Set(key, []byte(newString))
	return len(newString), nil
}

func (db *TinyDB) Incr(key []byte, incr int64) (res int64, err error) {
	bytes, err := db.Get(key)
	if errors.Is(err, constants.ErrKeyNotFound) {
		bytes = []byte("0")
	}
	res, err = strconv.ParseInt(string(bytes), 10, 64)
	if err != nil {
		return 0, err
	}
	res += incr
	_ = db.Set(key, []byte(fmt.Sprintf("%v", res)))
	return res, nil
}

func (db *TinyDB) IncrByFloat(key []byte, incr float64) (res float64, err error) {
	bytes, err := db.Get(key)
	if errors.Is(err, constants.ErrKeyNotFound) {
		bytes = []byte("0")
	}
	res, err = strconv.ParseFloat(string(bytes), 64)
	if err != nil {
		return 0, err
	}
	res += incr
	_ = db.Set(key, []byte(fmt.Sprintf("%v", res)))
	return res, nil
}

func (db *TinyDB) Append(key, value []byte) (res int, err error) {
	bytes, err := db.Get(key)
	if errors.Is(err, constants.ErrKeyNotFound) {
		bytes = []byte("")
	}
	bytes = append(bytes, value...)
	_ = db.Set(key, bytes)
	return len(string(bytes)), nil
}

func (db *TinyDB) Get(key []byte) ([]byte, error) {
	pos, err := db.strKeydir.Get(string(key))
	if err != nil {
		return nil, err
	}
	entry, err := db.ReadEntry(data.String, pos)
	if err != nil {
		return nil, err
	}
	return entry.Value, nil
}

func (db *TinyDB) GetRange(key []byte, start, end int) (string, error) {
	bytes, err := db.Get(key)
	if err != nil {
		return "", err
	}
	str := string(bytes)
	if start >= len(bytes) {
		start = len(bytes) - 1
	}
	if start < 0 {
		start = len(bytes) + start
	}
	if end >= len(bytes) {
		end = len(bytes) - 1
	}
	if end < 0 {
		end = len(bytes) + end
	}
	return str[start : end+1], nil
}

func (db *TinyDB) GetDel(key []byte) interface{} {
	res, err := db.Get(key)
	if errors.Is(err, constants.ErrKeyNotFound) {
		return nil
	}
	entry := data.NewEntry(key, []byte{}, data.Delete)
	_, _ = db.WriteEntry(entry, data.String)
	db.strKeydir.Del(string(key))
	return string(res)
}
