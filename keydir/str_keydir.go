package keydir

import (
	"SouthWind6510/TinyDB/pkg/constants"
	"sync"
)

type StrKeydir struct {
	mu     sync.RWMutex
	keydir map[string]*EntryPos
}

func NewStrKeydir() *StrKeydir {
	return &StrKeydir{
		keydir: make(map[string]*EntryPos),
	}
}

func (i *StrKeydir) Set(key string, pos *EntryPos) {
	i.mu.Lock()
	defer i.mu.Unlock()

	i.keydir[key] = pos
}

func (i *StrKeydir) Get(key string) (pos *EntryPos, err error) {
	i.mu.RLock()
	defer i.mu.RUnlock()

	if _, ok := i.keydir[key]; !ok {
		return nil, constants.ErrKeyNotFound
	}
	return i.keydir[key], nil
}

func (i *StrKeydir) Del(key string) {
	i.mu.Lock()
	defer i.mu.Unlock()

	delete(i.keydir, key)
}
