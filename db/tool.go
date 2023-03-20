package db

import (
	"encoding/binary"
)

func encodeSubKey(key, subKey []byte) []byte {
	len1, len2 := len(key), len(subKey)
	buf := make([]byte, 8+len1+len2)
	binary.LittleEndian.PutUint32(buf[:4], uint32(len1))
	binary.LittleEndian.PutUint32(buf[4:8], uint32(len2))
	copy(buf[8:8+len1], key)
	copy(buf[8+len1:8+len1+len2], subKey)
	return buf
}

func decodeSubKey(buf []byte) ([]byte, []byte) {
	len1 := binary.LittleEndian.Uint32(buf[:4])
	len2 := binary.LittleEndian.Uint32(buf[4:8])
	return buf[8 : 8+len1], buf[8+len1 : 8+len1+len2]
}

func encodeListKey(key []byte, index int) []byte {
	len1 := len(key)
	buf := make([]byte, 8+len1)
	binary.LittleEndian.PutUint32(buf[:4], uint32(len1))
	copy(buf[4:4+len1], key)
	binary.LittleEndian.PutUint32(buf[4+len1:8+len1], uint32(index))
	return buf
}

func decodeListKey(buf []byte) ([]byte, int) {
	len1 := binary.LittleEndian.Uint32(buf[:4])
	return buf[4 : 4+len1], int(binary.LittleEndian.Uint32(buf[4+len1 : 8+len1]))
}
