package keydir

import (
	"SouthWind6510/TinyDB/ds"
	"SouthWind6510/TinyDB/pkg/constants"
	"sync"

	"github.com/pkg/errors"
)

type ZSetKeydir struct {
	keydir map[string]*ds.SkipList
	mu     sync.RWMutex
}

func NewZSetKeydir() *ZSetKeydir {
	return &ZSetKeydir{
		keydir: make(map[string]*ds.SkipList),
	}
}

func (i *ZSetKeydir) GetScore(key string, member string) (score float64, err error) {
	if i.keydir[key] == nil {
		return 0, constants.ErrKeyNotFound
	}
	score, err = i.keydir[key].GetScore(member)
	if errors.Is(err, constants.ErrMemberNotExist) {
		return 0, err
	}
	if err != nil {
		return 0, err
	}
	return score, nil
}

func (i *ZSetKeydir) Set(key string, member string, score float64) {
	i.mu.Lock()
	if i.keydir[key] == nil {
		i.keydir[key] = ds.NewSkipList(2)
	}
	defer i.mu.Unlock()

	oldScore, err := i.keydir[key].GetScore(member)
	if err == nil {
		i.keydir[key].Delete(member, oldScore)
	}
	i.keydir[key].Insert(member, score)
}

func (i *ZSetKeydir) Del(key string, member string, score float64) {
	if i.keydir[key] == nil {
		return
	}
	i.keydir[key].Delete(member, score)
}

func (i *ZSetKeydir) GetMemberCount(key string) int64 {
	if i.keydir[key] == nil {
		return 0
	}
	return i.keydir[key].GetLength()
}

// Update 调用方需要确保key和member存在
func (i *ZSetKeydir) Update(key string, member string, score float64, updateScore float64) {
	i.keydir[key].Delete(member, score)
	i.keydir[key].Insert(member, updateScore)
}

func (i *ZSetKeydir) GetCountByScore(key string, min, max float64) int64 {
	if i.keydir[key] == nil {
		return 0
	}
	node1 := i.keydir[key].FirstInRange(min, max)
	node2 := i.keydir[key].LastInRange(min, max)
	if node1 == nil || node2 == nil {
		return 0
	}
	l := i.keydir[key].GetRank(node1.GetMember(), node1.GetScore())
	r := i.keydir[key].GetRank(node2.GetMember(), node2.GetScore())
	return r - l + 1
}

func (i *ZSetKeydir) GetMemberByRank(key string, rank int64) (member string, score float64, err error) {
	if i.keydir[key] == nil {
		return "", 0, constants.ErrKeyNotFound
	}
	if rank < 0 {
		rank = i.keydir[key].GetLength() + rank
	}
	node := i.keydir[key].GetElementByRank(rank + 1)
	if node == nil {
		return "", 0, constants.ErrMemberNotExist
	}
	return node.GetMember(), node.GetScore(), nil
}

func (i *ZSetKeydir) GetRangeByRank(key string, start, end int64, rev bool) (members []string, scores []float64, err error) {
	if i.keydir[key] == nil {
		return nil, nil, constants.ErrKeyNotFound
	}
	if start < 0 {
		start = i.keydir[key].GetLength() + start
	}
	if end < 0 {
		end = i.keydir[key].GetLength() + end
	}
	if start > end {
		return nil, nil, constants.ErrInvalidRange
	}

	nodes := i.keydir[key].GetRangeByRank(start+1, end+1)
	if rev {
		for i := 0; i < len(nodes)/2; i++ {
			nodes[i], nodes[len(nodes)-1-i] = nodes[len(nodes)-1-i], nodes[i]
		}
	}
	members = make([]string, len(nodes))
	scores = make([]float64, len(nodes))
	for i := 0; i < len(nodes); i++ {
		members[i] = nodes[i].GetMember()
		scores[i] = nodes[i].GetScore()
	}
	return members, scores, nil
}

func (i *ZSetKeydir) GetRangeByScore(key string, min, max float64, rev bool) (members []string, scores []float64, err error) {
	if i.keydir[key] == nil {
		return nil, nil, constants.ErrKeyNotFound
	}
	if min > max {
		return nil, nil, constants.ErrInvalidRange
	}

	nodes := i.keydir[key].GetRangeByScore(min, max)
	if rev {
		for i := 0; i < len(nodes)/2; i++ {
			nodes[i], nodes[len(nodes)-1-i] = nodes[len(nodes)-1-i], nodes[i]
		}
	}
	members = make([]string, len(nodes))
	scores = make([]float64, len(nodes))
	for i := 0; i < len(nodes); i++ {
		members[i] = nodes[i].GetMember()
		scores[i] = nodes[i].GetScore()
	}
	return members, scores, nil
}

func (i *ZSetKeydir) GetRank(key string, member string) (rank int64, score float64, err error) {
	if i.keydir[key] == nil {
		return 0, 0, constants.ErrKeyNotFound
	}
	score, err = i.keydir[key].GetScore(member)
	if err != nil {
		return 0, 0, err
	}
	rank = i.keydir[key].GetRank(member, score) - 1
	return rank, score, nil
}

func (i *ZSetKeydir) DeleteWithoutScore(key string, member string) bool {
	if i.keydir[key] == nil {
		return false
	}
	score, err := i.keydir[key].GetScore(member)
	if err != nil {
		return false
	}
	res := i.keydir[key].Delete(member, score)
	if i.keydir[key].GetLength() == 0 {
		delete(i.keydir, key)
	}
	return res
}

func (i *ZSetKeydir) DeleteRangeByScore(key string, min, max float64) []string {
	if i.keydir[key] == nil {
		return nil
	}
	return i.keydir[key].DeleteRangeByScore(min, max)
}

func (i *ZSetKeydir) DeleteRangeByRank(key string, start, end int64) []string {
	if i.keydir[key] == nil {
		return nil
	}
	return i.keydir[key].DeleteRangeByRank(start+1, end+1)
}
