package db

import (
	"SouthWind6510/TinyDB/data"
	"SouthWind6510/TinyDB/keydir"
	"SouthWind6510/TinyDB/pkg/constants"
	"SouthWind6510/TinyDB/pkg/logger"
	"io"
	"os"
	"sort"
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

	strKeydir  *keydir.StrKeydir
	listKeydir *keydir.ListKeydir
	hashKeydir *keydir.HashKeydir
	setKeydir  *keydir.SetKeydir
	zsetKeydir *keydir.ZSetKeydir
}

func Open(opt *Options) (tinyDB *TinyDB, err error) {
	logger.Log.Infof("Open TinyDB with options: %+v", opt)
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
		listKeydir:    keydir.NewListKeydir(),
		hashKeydir:    keydir.NewHashKeydir(),
		setKeydir:     keydir.NewSetKeydir(),
		zsetKeydir:    keydir.NewZSetKeydir(),
	}

	// 加载文件目录
	err = tinyDB.loadDataFiles()
	if err != nil {
		return nil, err
	}
	logger.Log.Infof("Load data files successful")
	// 加载索引，更新WriteAt
	err = tinyDB.buildIndexes()
	if err != nil {
		return nil, err
	}
	logger.Log.Infof("Build indexes successful")
	// TODO 异步GC
	return
}

func (db *TinyDB) Close() {
	for _, activeFile := range db.activeFiles {
		_ = activeFile.Sync()
		_ = activeFile.Close()
		if os.Getenv(constants.DebugEnv) == "1" {
			err := activeFile.Remove()
			if err != nil {
				logger.Log.Errorf("%+v", err)
			}
		}
	}
	for _, archivedFiles := range db.archivedFiles {
		for _, archivedFile := range archivedFiles {
			_ = archivedFile.Sync()
			_ = archivedFile.Close()
			if os.Getenv(constants.DebugEnv) == "1" {
				err := archivedFile.Remove()
				if err != nil {
					logger.Log.Errorf("%+v", err)
				}
			}
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

// buildIndexes 读取活跃文件和存档文件数据，构建索引
// 要按顺序读！！！
func (db *TinyDB) buildIndexes() (err error) {
	db.mu.Lock()
	defer db.mu.Unlock()
	for dataType, activeFile := range db.activeFiles {
		files := make([]*data.File, 0)
		files = append(files, activeFile)
		for _, archivedFile := range db.archivedFiles[dataType] {
			files = append(files, archivedFile)
		}
		// 按照fid从小到大读取
		sort.Slice(files, func(i, j int) bool {
			return files[i].Fid < files[j].Fid
		})
		for i := 0; i < len(files); i++ {
			offset := int64(0)
			// 读取活跃文件数据
			for {
				entry, err := files[i].ReadEntry(offset)
				if errors.Is(err, io.EOF) || errors.Is(err, constants.ErrReadNullEntry) {
					break
				} else if err != nil {
					return err
				}
				size := int64(data.HeaderSize + entry.Header.KeySize + entry.Header.ValueSize)
				pos := &keydir.EntryPos{Fid: activeFile.Fid, Offset: offset, Size: size}
				db.addIndex(dataType, entry, pos)
				offset += size
			}
			if i == len(files)-1 {
				// 更新活跃文件WriteAt
				files[i].WriteAt = offset
			}
		}
	}
	return nil
}

func (db *TinyDB) addIndex(dataType data.DataType, entry *data.Entry, pos *keydir.EntryPos) {
	switch dataType {
	case data.String:
		if entry.Header.Type == data.Insert {
			db.strKeydir.Set(string(entry.Key), pos)
		} else if entry.Header.Type == data.Delete {
			db.strKeydir.Del(string(entry.Key))
		}
	case data.List:
		if entry.Header.Type == data.InsertListMeta {
			db.listKeydir.Set(string(entry.Key), MetaIndex, pos)
		} else if entry.Header.Type == data.Insert {
			key, index := decodeListKey(entry.Key)
			db.listKeydir.Set(string(key), index, pos)
		} else if entry.Header.Type == data.Delete {
			key, index := decodeListKey(entry.Key)
			db.listKeydir.Del(string(key), index)
		}
	case data.Hash:
		if entry.Header.Type == data.Insert {
			key, field := decodeSubKey(entry.Key)
			db.hashKeydir.Set(string(key), string(field), pos)
		} else if entry.Header.Type == data.Delete {
			key, field := decodeSubKey(entry.Key)
			db.hashKeydir.Del(string(key), string(field))
		}
	case data.Set:
		if entry.Header.Type == data.Insert {
			key, field := decodeSubKey(entry.Key)
			db.setKeydir.Set(string(key), string(field))
		} else if entry.Header.Type == data.Delete {
			key, field := decodeSubKey(entry.Key)
			db.setKeydir.Del(string(key), string(field))
		}
	case data.ZSet:
		if entry.Header.Type == data.Insert {
			key, member := decodeSubKey(entry.Key)
			score, err := strconv.ParseFloat(string(entry.Value), 64)
			if err != nil {
				logger.Log.Errorf("zset score parse error: %v", err)
				return
			}
			db.zsetKeydir.Set(string(key), string(member), score)
		} else if entry.Header.Type == data.Delete {
			key, member := decodeSubKey(entry.Key)
			db.zsetKeydir.DeleteWithoutScore(string(key), string(member))
		}
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
	db.mu.Lock()
	defer db.mu.Unlock()
	err = db.initDataFile(dataType)
	if err != nil {
		return nil, err
	}
	buf := data.EncodeEntry(entry)
	activeFile := db.activeFiles[dataType]
	if activeFile.WriteAt+int64(len(buf)) > db.opt.FileSizeLimit {
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
		db.activeFiles[dataType] = newFile
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
