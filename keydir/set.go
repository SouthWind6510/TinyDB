package keydir

import (
	"SouthWind6510/TinyDB/pkg/constants"
	"sync"
)

type setFieldMap map[string]struct{}

type SetKeydir struct {
	mu     sync.RWMutex
	keydir map[string]setFieldMap //key的field是否存在
}

func NewSetKeydir() *SetKeydir {
	return &SetKeydir{
		keydir: make(map[string]setFieldMap),
	}
}

func (i *SetKeydir) Set(key string, field string) {
	i.mu.Lock()
	defer i.mu.Unlock()

	if i.keydir[key] == nil {
		i.keydir[key] = make(setFieldMap)
	}
	i.keydir[key][field] = struct{}{}
}

func (i *SetKeydir) Get(key string, field string) (err error) {
	i.mu.RLock()
	defer i.mu.RUnlock()

	if i.keydir[key] == nil {
		return constants.ErrKeyNotFound
	}
	if _, ok := i.keydir[key][field]; !ok {
		return constants.ErrKeyNotFound
	}
	return nil
}

func (i *SetKeydir) Del(key string, field string) {
	i.mu.Lock()
	defer i.mu.Unlock()

	delete(i.keydir[key], field)
}

func (i *SetKeydir) GetMemberCount(key string) (int, error) {
	i.mu.RLock()
	defer i.mu.RUnlock()

	if i.keydir[key] == nil {
		return 0, constants.ErrKeyNotFound
	}
	return len(i.keydir[key]), nil
}

func (i *SetKeydir) Pop(key string) (string, error) {
	i.mu.Lock()
	defer i.mu.Unlock()

	if i.keydir[key] == nil {
		return "", constants.ErrKeyNotFound
	}
	for field := range i.keydir[key] {
		delete(i.keydir[key], field)
		return field, nil
	}
	return "", constants.ErrKeyNotFound
}

func (i *SetKeydir) GetMembers(key string) ([]string, error) {
	i.mu.RLock()
	defer i.mu.RUnlock()

	if i.keydir[key] == nil {
		return nil, constants.ErrKeyNotFound
	}
	fields := make([]string, 0, len(i.keydir[key]))
	for field := range i.keydir[key] {
		fields = append(fields, field)
	}
	return fields, nil
}

func (i *SetKeydir) IsExists(key string, field string) bool {
	i.mu.RLock()
	defer i.mu.RUnlock()

	if i.keydir[key] == nil {
		return false
	}
	if _, ok := i.keydir[key][field]; !ok {
		return false
	}
	return true
}

func (i *SetKeydir) RandMember(key string) (string, error) {
	i.mu.RLock()
	defer i.mu.RUnlock()

	if i.keydir[key] == nil {
		return "", constants.ErrKeyNotFound
	}
	for field := range i.keydir[key] {
		return field, nil
	}
	return "", constants.ErrKeyNotFound
}

func (i *SetKeydir) RandMembers(key string, count int) (res []string, err error) {
	i.mu.RLock()
	defer i.mu.RUnlock()

	if i.keydir[key] == nil {
		return nil, constants.ErrKeyNotFound
	}
	if count > len(i.keydir[key]) {
		count = len(i.keydir[key])
	}
	res = make([]string, 0, count)
	for field := range i.keydir[key] {
		res = append(res, field)
		if len(res) == count {
			break
		}
	}
	return
}
