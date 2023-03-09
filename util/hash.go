package util

import "hash/crc32"

func GetCrc32(data []byte) uint32 {
	return crc32.ChecksumIEEE(data)
}
