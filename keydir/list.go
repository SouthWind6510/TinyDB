package keydir

import (
	"SouthWind6510/TinyDB/pkg/constants"
	"sync"
)

type listIndexMap map[int]*EntryPos

type ListKeydir struct {
	mu     sync.RWMutex
	keydir map[string]listIndexMap //key的index的位置
}

func NewListKeydir() *ListKeydir {
	return &ListKeydir{
		keydir: make(map[string]listIndexMap),
	}
}

// Set index为-1时表示listMeta
func (i *ListKeydir) Set(key string, index int, pos *EntryPos) {
	i.mu.Lock()
	defer i.mu.Unlock()

	if i.keydir[key] == nil {
		i.keydir[key] = make(listIndexMap)
	}
	i.keydir[key][index] = pos
}

// Get index为-1表示listMeta
func (i *ListKeydir) Get(key string, index int) (pos *EntryPos, err error) {
	i.mu.RLock()
	defer i.mu.RUnlock()

	if i.keydir[key] == nil || i.keydir[key][index] == nil {
		return nil, constants.ErrKeyNotFound
	}
	return i.keydir[key][index], nil
}

func (i *ListKeydir) Del(key string, index int) {
	i.mu.Lock()
	defer i.mu.Unlock()

	delete(i.keydir[key], index)
}
