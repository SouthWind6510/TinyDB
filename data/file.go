package data

import (
	"SouthWind6510/TinyDB/pkg/constants"
	"SouthWind6510/TinyDB/util"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"sync"

	"github.com/pkg/errors"
)

type DataType int8

const (
	String DataType = iota
	List
	Hash
	Set
	ZSet
)

var (
	Type2FileSufMap = map[DataType]string{
		String: ".str.log",
		List:   ".list.log",
		Hash:   ".hash.log",
		Set:    ".set.log",
		ZSet:   ".zset.log",
	}
	FileSuf2TypeMap = map[string]DataType{
		"str":  String,
		"list": List,
		"hash": Hash,
		"set":  Set,
		"zset": ZSet,
	}
)

type File struct {
	Fd       *os.File
	Fid      int16
	FileName string
	WriteAt  int64
	mu       sync.RWMutex
}

func NewFile(fd *os.File, fid int16, filename string, writeAt int64) *File {
	return &File{
		Fd:       fd,
		Fid:      fid,
		FileName: filename,
		WriteAt:  writeAt,
	}
}

func getFileName(path string, fid int16, fileType DataType) string {
	return filepath.Join(path, strconv.FormatUint(uint64(fid), 10)+Type2FileSufMap[fileType])
}

func OpenDataFile(path string, fid int16, fileType DataType, fileSize int64) (df *File, err error) {
	fileName := getFileName(path, fid, fileType)
	file, err := os.OpenFile(fileName, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return nil, errors.Wrap(err, fmt.Sprintf("filename: %v", fileName))
	}
	stat, err := os.Stat(fileName)
	if err != nil {
		return nil, errors.Wrap(err, fmt.Sprintf("filename: %v", fileName))
	}
	if stat.Size() < fileSize {
		err = file.Truncate(fileSize)
		if err != nil {
			return nil, errors.Wrap(err, fmt.Sprintf("filename: %v", fileName))
		}
	}
	return NewFile(file, fid, fileName, 0), nil
}

func (df *File) ReadEntry(offset int64) (entry *Entry, err error) {
	df.mu.RLock()
	defer df.mu.RUnlock()
	entry = &Entry{}
	hBuf := make([]byte, HeaderSize)
	_, err = df.Fd.ReadAt(hBuf, offset)
	if err != nil {
		return nil, errors.Wrap(err, fmt.Sprintf("filename: %v", df.FileName))
	}
	entry.Header = decodeEntryHeader(hBuf)
	if entry.Header.CRC == 0 {
		return nil, constants.ErrReadNullEntry
	}
	kvBuf := make([]byte, entry.Header.KeySize+entry.Header.ValueSize)
	_, err = df.Fd.ReadAt(kvBuf, offset+HeaderSize)
	if err != nil {
		return nil, errors.Wrap(err, fmt.Sprintf("filename: %v", df.FileName))
	}
	entry.Key = kvBuf[:entry.Header.KeySize]
	entry.Value = kvBuf[entry.Header.KeySize:]
	// 校验CRC
	if crc := util.GetCrc32(append(hBuf[4:], kvBuf...)); crc != entry.Header.CRC {
		return nil, errors.Wrap(constants.ErrInconsistentCRC, fmt.Sprintf("want crc: %v, got crc: %v", entry.Header.CRC, crc))
	}
	return
}

func (df *File) Write(buf []byte) (err error) {
	df.mu.Lock()
	defer df.mu.Unlock()
	n, err := df.Fd.WriteAt(buf, df.WriteAt)
	if err != nil {
		return
	}
	if n != len(buf) {
		return errors.Wrap(err, fmt.Sprintf("filename: %v, write at: %v", df.FileName, df.WriteAt))
	}
	df.WriteAt += int64(n)
	return
}

func (df *File) Sync() (err error) {
	err = df.Fd.Sync()
	if err != nil {
		return errors.Wrap(err, fmt.Sprintf("filename: %v", df.FileName))
	}
	return nil
}

func (df *File) Remove() (err error) {
	err = os.Remove(df.Fd.Name())
	if err != nil {
		return errors.Wrap(err, fmt.Sprintf("filename: %v", df.FileName))
	}
	return nil
}

func (df *File) Close() (err error) {
	if err = df.Fd.Close(); err != nil {
		return errors.Wrap(err, fmt.Sprintf("filename: %v", df.FileName))
	}
	return nil
}
