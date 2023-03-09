package data

import (
	"SouthWind6510/TinyDB/util"
	"encoding/binary"
	"fmt"
	"time"
)

const HeaderSize = 29

type OptrType uint8

const (
	INSERT OptrType = iota
	UPDATE
	DELETE
)

type EntryHeader struct {
	CRC        uint32
	KeySize    uint32
	ValueSize  uint32
	Type       OptrType
	Timestamp  uint64
	ExpiryTime uint64
}

func (eh *EntryHeader) String() string {
	return fmt.Sprintf("{CRC: %v, KeySize: %v, ValueSize: %v, Type: %v, Timestamp: %v, ExpiryTime: %v}",
		eh.CRC, eh.KeySize, eh.ValueSize, eh.Type, eh.Timestamp, eh.ExpiryTime)
}

type Entry struct {
	Header *EntryHeader
	Key    []byte
	Value  []byte
}

func (e *Entry) String() string {
	return fmt.Sprintf("{Header: %+v, Key: %v, Value: %v}", e.Header, e.Key, e.Value)
}

func NewEntry(key, value []byte, opType OptrType) (entry *Entry) {
	entry = &Entry{
		Header: &EntryHeader{
			KeySize:   uint32(len(key)),
			ValueSize: uint32(len(value)),
			Type:      opType,
			Timestamp: uint64(time.Now().Unix()),
		},
		Key:   key,
		Value: value,
	}
	return
}

func (e *Entry) size() uint64 {
	return uint64(HeaderSize + e.Header.KeySize + e.Header.ValueSize)
}

func EncodeEntry(e *Entry) (buf []byte) {
	buf = make([]byte, e.size())
	binary.LittleEndian.PutUint32(buf[4:8], e.Header.KeySize)
	binary.LittleEndian.PutUint32(buf[8:12], e.Header.ValueSize)
	buf[12] = byte(e.Header.Type)
	binary.LittleEndian.PutUint64(buf[13:21], e.Header.Timestamp)
	binary.LittleEndian.PutUint64(buf[21:29], e.Header.ExpiryTime)
	copy(buf[HeaderSize:], e.Key)
	copy(buf[HeaderSize+e.Header.KeySize:], e.Value)
	e.Header.CRC = util.GetCrc32(buf[4:])
	binary.LittleEndian.PutUint32(buf[:4], e.Header.CRC)
	return
}

func decodeEntryHeader(buf []byte) (eh *EntryHeader) {
	eh = &EntryHeader{
		CRC:        binary.LittleEndian.Uint32(buf[:4]),
		KeySize:    binary.LittleEndian.Uint32(buf[4:8]),
		ValueSize:  binary.LittleEndian.Uint32(buf[8:12]),
		Type:       OptrType(buf[12]),
		Timestamp:  binary.LittleEndian.Uint64(buf[13:21]),
		ExpiryTime: binary.LittleEndian.Uint64(buf[21:29]),
	}
	return
}

func decodeEntry(buf []byte) (e *Entry) {
	e = &Entry{
		Header: decodeEntryHeader(buf[:29]),
	}
	e.Key = buf[HeaderSize : HeaderSize+e.Header.KeySize]
	e.Value = buf[HeaderSize+e.Header.KeySize : HeaderSize+e.Header.KeySize+e.Header.ValueSize]
	return
}
