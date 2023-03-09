package keydir

import (
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

func (i *StrKeydir) Put(key string, pos *EntryPos) {
	i.mu.Lock()
	defer i.mu.Unlock()
	i.keydir[key] = pos
}

func (i *StrKeydir) Update(key string, pos *EntryPos) {
	i.mu.Lock()
	defer i.mu.Unlock()
	i.keydir[key] = pos
}

func (i *StrKeydir) Get(key string) *EntryPos {
	i.mu.RLock()
	defer i.mu.RUnlock()
	return i.keydir[key]
}
