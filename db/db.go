package db

import (
	"SouthWind6510/TinyDB/data"
	"SouthWind6510/TinyDB/keydir"
	"SouthWind6510/TinyDB/pkg/constants"
	"io"
	"os"
	"strconv"
	"strings"
	"sync"

	"github.com/pkg/errors"
)

type TinyDB struct {
	activeFiles   map[data.DataType]*data.File
	archivedFiles map[data.DataType]map[int16]*data.File
	fileMap       map[data.DataType][]uint16
	opt           *Options
	mu            sync.RWMutex // 读写锁

	strKeydir *keydir.StrKeydir
}

func Open(opt *Options) (tinyDB *TinyDB, err error) {
	// 创建不存在的目录
	if _, err = os.Stat(opt.DBPath); os.IsNotExist(err) {
		if err := os.MkdirAll(opt.DBPath, os.ModePerm); err != nil {
			return nil, err
		}
	}
	tinyDB = &TinyDB{
		activeFiles:   make(map[data.DataType]*data.File),
		archivedFiles: make(map[data.DataType]map[int16]*data.File),
		opt:           opt,
		strKeydir:     keydir.NewStrKeydir(),
	}

	// 加载文件目录
	err = tinyDB.loadDataFiles()
	if err != nil {
		return nil, err
	}
	// 加载索引，更新WriteAt
	err = tinyDB.buildIndexes()
	if err != nil {
		return nil, err
	}
	// TODO 异步GC
	return
}

func (db *TinyDB) Close() {
	for _, activeFile := range db.activeFiles {
		_ = activeFile.Sync()
		_ = activeFile.Close()
	}
	for _, archivedFiles := range db.archivedFiles {
		for _, archivedFile := range archivedFiles {
			_ = archivedFile.Sync()
			_ = archivedFile.Close()
		}
	}
}

func (db *TinyDB) loadDataFiles() (err error) {
	db.mu.Lock()
	defer db.mu.Unlock()
	fileInfos, err := os.ReadDir(db.opt.DBPath)
	if err != nil {
		return err
	}
	// 将所有文件按照类型存入存档map中
	for _, fileInfo := range fileInfos {
		strs := strings.Split(fileInfo.Name(), ".")
		fid, _ := strconv.ParseInt(strs[0], 10, 64)
		fileType := data.FileSuf2TypeMap[strs[1]]
		dataFile, err := data.OpenDataFile(db.opt.DBPath, int16(fid), fileType, db.opt.FileSizeLimit)
		if err != nil {
			return err
		}
		if db.archivedFiles[fileType] == nil {
			db.archivedFiles[fileType] = make(map[int16]*data.File)
		}
		db.archivedFiles[fileType][int16(fid)] = dataFile
	}

	// fid最大的文件作为活跃文件
	for k, v := range db.archivedFiles {
		fid := int16(len(v)) - 1
		db.activeFiles[k] = v[fid]
		delete(v, fid)
	}
	return
}

func (db *TinyDB) buildIndexes() (err error) {
	db.mu.Lock()
	defer db.mu.Unlock()
	for dataType, activeFile := range db.activeFiles {
		offset := int64(0)
		// 读取活跃文件数据
		for {
			entry, err := activeFile.ReadEntry(offset)
			if err == io.EOF || errors.Is(err, constants.ErrReadNullEntry) {
				break
			} else if err != nil {
				return err
			}
			size := int64(data.HeaderSize + entry.Header.KeySize + entry.Header.ValueSize)
			pos := &keydir.EntryPos{Fid: activeFile.Fid, Offset: offset, Size: size}
			db.addIndex(dataType, entry, pos)
			offset += size
		}
		// 更新活跃文件WriteAt
		db.activeFiles[dataType].WriteAt = offset

		// 读取存档文件数据
		for _, archivedFile := range db.archivedFiles[dataType] {
			offset = int64(0)
			for {
				entry, err := archivedFile.ReadEntry(offset)
				if err == io.EOF || errors.Is(err, constants.ErrReadNullEntry) {
					break
				} else if err != nil {
					return err
				}
				size := int64(data.HeaderSize + entry.Header.KeySize + entry.Header.ValueSize)
				pos := &keydir.EntryPos{Fid: archivedFile.Fid, Offset: offset, Size: size}
				db.addIndex(dataType, entry, pos)
				offset += size
			}
		}
	}
	return nil
}

func (db *TinyDB) addIndex(dataType data.DataType, entry *data.Entry, pos *keydir.EntryPos) {
	switch dataType {
	case data.String:
		db.strKeydir.Put(string(entry.Key), pos)
	}
}

// initDataFile 第一次写dataType类型数据时需要初始化文件
func (db *TinyDB) initDataFile(dataType data.DataType) (err error) {
	if db.activeFiles[dataType] != nil {
		return
	}
	file, err := data.OpenDataFile(db.opt.DBPath, 0, dataType, db.opt.FileSizeLimit)
	if err != nil {
		return
	}
	db.activeFiles[dataType] = file
	return nil
}

func (db *TinyDB) WriteEntry(entry *data.Entry, dataType data.DataType) (pos *keydir.EntryPos, err error) {
	err = db.initDataFile(dataType)
	if err != nil {
		return nil, err
	}
	buf := data.EncodeEntry(entry)
	activeFile := db.activeFiles[dataType]
	if activeFile.WriteAt+int64(len(buf)) > db.opt.FileSizeLimit {
		// TODO 加锁？？？
		if err = activeFile.Sync(); err != nil {
			return nil, err
		}
		// 新建活跃文件
		newFile, err := data.OpenDataFile(db.opt.DBPath, activeFile.Fid+1, dataType, db.opt.FileSizeLimit)
		if err != nil {
			return nil, err
		}
		if db.archivedFiles[dataType] == nil {
			db.archivedFiles[dataType] = make(map[int16]*data.File)
		}
		db.archivedFiles[dataType][activeFile.Fid] = activeFile
		activeFile = newFile
	}
	pos = &keydir.EntryPos{Fid: activeFile.Fid, Offset: activeFile.WriteAt, Size: int64(len(buf))}
	if err = activeFile.Write(buf); err != nil {
		return nil, err
	}
	return
}

func (db *TinyDB) ReadEntry(dataType data.DataType, pos *keydir.EntryPos) (entry *data.Entry, err error) {
	var dataFile *data.File
	if db.activeFiles[dataType].Fid == pos.Fid {
		dataFile = db.activeFiles[dataType]
	} else {
		dataFile = db.archivedFiles[dataType][pos.Fid]
	}
	entry, err = dataFile.ReadEntry(pos.Offset)
	if err != nil {
		return nil, err
	}
	return
}
