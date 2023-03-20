package keydir

import (
	"SouthWind6510/TinyDB/pkg/constants"
	"sync"
)

type hashFieldMap map[string]*EntryPos

type HashKeydir struct {
	mu     sync.RWMutex
	keydir map[string]hashFieldMap //key的field的位置
}

func NewHashKeydir() *HashKeydir {
	return &HashKeydir{
		keydir: make(map[string]hashFieldMap),
	}
}

func (i *HashKeydir) Set(key string, field string, pos *EntryPos) {
	i.mu.Lock()
	defer i.mu.Unlock()

	if i.keydir[key] == nil {
		i.keydir[key] = make(hashFieldMap)
	}
	i.keydir[key][field] = pos
}

func (i *HashKeydir) Get(key string, field string) (pos *EntryPos, err error) {
	i.mu.RLock()
	defer i.mu.RUnlock()

	if i.keydir[key] == nil || i.keydir[key][field] == nil {
		return nil, constants.ErrKeyNotFound
	}
	return i.keydir[key][field], nil
}

func (i *HashKeydir) Del(key string, field string) {
	i.mu.Lock()
	defer i.mu.Unlock()

	delete(i.keydir[key], field)
}

func (i *HashKeydir) GetFields(key string) (fields []string, err error) {
	i.mu.RLock()
	defer i.mu.RUnlock()

	if i.keydir[key] == nil {
		return nil, constants.ErrKeyNotFound
	}
	for field := range i.keydir[key] {
		fields = append(fields, field)
	}
	return
}

func (i *HashKeydir) GetFieldCount(key string) (int, error) {
	i.mu.RLock()
	defer i.mu.RUnlock()

	if i.keydir[key] == nil {
		return 0, constants.ErrKeyNotFound
	}
	return len(i.keydir[key]), nil
}
