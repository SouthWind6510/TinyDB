package db

import "SouthWind6510/TinyDB/data"

func (db *TinyDB) Set(key, value []byte) (err error) {
	entry := data.NewEntry(key, value, data.Insert)
	pos, err := db.WriteEntry(entry, data.String)
	if err != nil {
		return err
	}
	db.strKeydir.Set(string(key), pos)
	return
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
