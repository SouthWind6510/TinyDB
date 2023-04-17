package ds

import (
	"SouthWind6510/TinyDB/pkg/constants"
	"SouthWind6510/TinyDB/util"
	"math"
	"math/rand"
	"sync"
	"time"
)

const SkipListMaxLevel = 32

// 参考《Redis设计与实现》

type skipListLevel struct {
	forward *skipListNode
	span    int64
}

type skipListNode struct {
	member   string // 唯一标识
	score    float64
	level    []*skipListLevel
	backward *skipListNode
}

func NewSkipListNode(member string, score float64, level int, backward *skipListNode) (node *skipListNode) {
	node = &skipListNode{
		member:   member,
		score:    score,
		level:    make([]*skipListLevel, level),
		backward: backward,
	}
	for i := 0; i < level; i++ {
		node.level[i] = &skipListLevel{
			forward: nil,
			span:    0,
		}
	}
	return
}

func (node *skipListNode) GetMember() string {
	return node.member
}

func (node *skipListNode) GetScore() float64 {
	return node.score
}

type SkipList struct {
	mu       sync.RWMutex
	header   *skipListNode
	tail     *skipListNode
	length   int64
	level    int
	skipSpan int
	members  map[string]float64
}

// NewSkipList 创建一个跳表，skipSpan表示平均每隔多少个节点增加一级索引，默认2
func NewSkipList(skipSpan int) *SkipList {
	if skipSpan <= 0 {
		skipSpan = 2
	}
	return &SkipList{
		header:   NewSkipListNode("", 0, SkipListMaxLevel, nil),
		level:    1,
		skipSpan: skipSpan,
		members:  make(map[string]float64, 0),
	}
}

func (zsl *SkipList) GetLength() int64 {
	return zsl.length
}

// 生成随机层级
func (zsl *SkipList) RandLevel() int {
	// log(1/p)(n) = log2(n) / log2(1/p)
	// p = 1 / skipSpan, n = length
	maxl := int(math.Log2(float64(zsl.length))/math.Log2(float64(zsl.skipSpan))) + 1
	level := 1
	rand.Seed(time.Now().Unix())
	for level < maxl && rand.Intn(zsl.skipSpan) == 0 {
		level++
	}
	return level
}

func (zsl *SkipList) Insert(member string, score float64) *skipListNode {
	zsl.mu.Lock()
	defer zsl.mu.Unlock()

	// 1. 查找插入位置
	pre := make([]*skipListNode, zsl.level) // 前置节点
	rank := make([]int64, zsl.level)        // 前置节点的跨度
	cur := zsl.header
	for i := zsl.level - 1; i >= 0; i-- {
		if i != zsl.level-1 {
			rank[i] = rank[i+1]
		}
		next := cur.level[i].forward
		for next != nil && (next.score < score || (next.score == score && next.member < member)) {
			rank[i] += cur.level[i].span
			cur = next
			next = cur.level[i].forward
		}
		pre[i] = cur
	}
	// 2. 创建新节点
	level := zsl.RandLevel()
	node := NewSkipListNode(member, score, level, cur)
	// 3. 插入新节点
	for i := 0; i < util.MinInt(level, zsl.level); i++ {
		node.level[i].forward = pre[i].level[i].forward
		node.level[i].span = pre[i].level[i].span - (rank[0] - rank[i])
		pre[i].level[i].forward = node
		pre[i].level[i].span = rank[0] - rank[i] + 1
	}
	for i := zsl.level; i < level; i++ {
		zsl.header.level[i] = &skipListLevel{
			forward: node,
			span:    rank[0] + 1,
		}
	}
	// 4. 更新上层前置节点的跨度
	for i := level; i < zsl.level; i++ {
		pre[i].level[i].span++
	}
	// 5. 更新长度
	zsl.length++
	zsl.members[member] = score
	// 6. 更新尾节点
	if node.level[0].forward == nil {
		zsl.tail = node
	} else {
		node.level[0].forward.backward = node
	}
	// 7. 更新链表层数
	zsl.level = util.MaxInt(zsl.level, level)

	return node
}

func (zsl *SkipList) DeleteNode(node *skipListNode, pre []*skipListNode) bool {
	// 删除节点
	for i := 0; i < len(node.level); i++ {
		pre[i].level[i].forward = node.level[i].forward
		pre[i].level[i].span += node.level[i].span - 1
	}
	if node.level[0].forward != nil {
		node.level[0].forward.backward = node.backward
	} else {
		zsl.tail = node.backward
	}
	delete(zsl.members, node.member)
	// 2. 更新上层前置节点的跨度
	for i := len(node.level); i < zsl.level; i++ {
		pre[i].level[i].span--
	}
	// 3. 更新链表层数
	for zsl.level > 1 && zsl.header.level[zsl.level-1].forward == nil {
		zsl.level--
	}
	// 4. 更新长度
	zsl.length--
	return true
}

func (zsl *SkipList) Delete(member string, score float64) bool {
	zsl.mu.Lock()
	defer zsl.mu.Unlock()

	// 1. 查找删除位置
	pre := make([]*skipListNode, zsl.level) // 前置节点
	cur := zsl.header
	for i := zsl.level - 1; i >= 0; i-- {
		// TODO 提前break
		next := cur.level[i].forward
		for next != nil && (next.score < score || (next.score == score && next.member < member)) {
			cur = next
			next = cur.level[i].forward
		}
		pre[i] = cur
	}
	node := cur.level[0].forward
	// 2. 判断待删除的节点是否存在
	if node == nil || node.score != score || node.member != member {
		return false
	}
	return zsl.DeleteNode(node, pre)
}

func (zsl *SkipList) GetRank(member string, score float64) (rank int64) {
	zsl.mu.RLock()
	defer zsl.mu.RUnlock()

	cur := zsl.header
	for i := zsl.level - 1; i >= 0; i-- {
		next := cur.level[i].forward
		for next != nil && (next.score < score || (next.score == score && next.member <= member)) {
			rank += cur.level[i].span
			cur = next
			next = cur.level[i].forward
		}
		if cur != nil && cur.member == member && cur.score == score {
			return rank
		}
	}
	return 0
}

func (zsl *SkipList) GetScore(member string) (score float64, err error) {
	if score, ok := zsl.members[member]; ok {
		return score, nil
	}
	return 0, constants.ErrMemberNotExist
}

func (zsl *SkipList) IsInRange(min, max float64) bool {
	zsl.mu.RLock()
	defer zsl.mu.RUnlock()

	if zsl.length == 0 || min > max {
		return false
	}
	if zsl.header.level[0].forward.score > max || zsl.tail.score < min {
		return false
	}
	return true
}

func (zsl *SkipList) FirstInRange(min, max float64) *skipListNode {
	zsl.mu.RLock()
	defer zsl.mu.RUnlock()

	if !zsl.IsInRange(min, max) {
		return nil
	}
	cur := zsl.header
	for i := zsl.level - 1; i >= 0; i-- {
		next := cur.level[i].forward
		for next != nil && next.score < min {
			cur = next
			next = cur.level[i].forward
		}
	}
	cur = cur.level[0].forward
	if cur != nil && cur.score <= max {
		return cur
	}
	return nil
}

func (zsl *SkipList) LastInRange(min, max float64) *skipListNode {
	zsl.mu.RLock()
	defer zsl.mu.RUnlock()

	if !zsl.IsInRange(min, max) {
		return nil
	}
	cur := zsl.header
	for i := zsl.level - 1; i >= 0; i-- {
		next := cur.level[i].forward
		for next != nil && next.score <= max {
			cur = next
			next = cur.level[i].forward
		}
	}
	if cur != nil && cur.score >= min {
		return cur
	}
	return nil
}

func (zsl *SkipList) GetElementByRank(rank int64) *skipListNode {
	zsl.mu.RLock()
	defer zsl.mu.RUnlock()

	curRank := int64(0)
	cur := zsl.header
	for i := zsl.level - 1; i >= 0; i-- {
		next := cur.level[i].forward
		for next != nil && curRank+cur.level[i].span <= rank {
			curRank += cur.level[i].span
			cur = next
			next = cur.level[i].forward
		}
		if curRank == rank {
			return cur
		}
	}
	return nil
}

func (zsl *SkipList) GetRangeByRank(start, end int64) (nodes []*skipListNode) {
	zsl.mu.RLock()
	defer zsl.mu.RUnlock()

	if start <= 0 || end <= 0 || start > end {
		return nil
	}
	if start > zsl.length {
		return nil
	}
	if end > zsl.length {
		end = zsl.length
	}

	nodes = make([]*skipListNode, 0)
	cur := zsl.GetElementByRank(start)
	for cur != nil && start <= end {
		nodes = append(nodes, cur)
		cur = cur.level[0].forward
		start++
	}
	return
}

func (zsl *SkipList) GetRangeByScore(min, max float64) (nodes []*skipListNode) {
	zsl.mu.RLock()
	defer zsl.mu.RUnlock()

	if !zsl.IsInRange(min, max) {
		return nil
	}

	cur := zsl.FirstInRange(min, max)
	for cur != nil {
		if cur.score > max {
			break
		}
		nodes = append(nodes, cur)
		cur = cur.level[0].forward
	}
	return
}

func (zsl *SkipList) DeleteRangeByScore(min, max float64) (members []string) {
	if !zsl.IsInRange(min, max) {
		return nil
	}

	zsl.mu.Lock()
	defer zsl.mu.Unlock()

	members = make([]string, 0)
	pre := make([]*skipListNode, zsl.level) // 前置节点
	cur := zsl.header
	for i := zsl.level - 1; i >= 0; i-- {
		next := cur.level[i].forward
		for next != nil && next.score < min {
			cur = next
			next = cur.level[i].forward
		}
		pre[i] = cur
	}
	cur = cur.level[0].forward
	for cur != nil && cur.score <= max {
		members = append(members, cur.member)
		next := cur.level[0].forward
		zsl.DeleteNode(cur, pre)
		cur = next
	}
	return
}

func (zsl *SkipList) DeleteRangeByRank(start, end int64) (members []string) {
	zsl.mu.Lock()
	defer zsl.mu.Unlock()

	if start <= 0 || end <= 0 || start > end {
		return nil
	}
	if start >= zsl.length {
		return nil
	}
	if end >= zsl.length {
		end = zsl.length
	}

	members = make([]string, 0)
	rank := int64(0)
	pre := make([]*skipListNode, zsl.level) // 前置节点
	cur := zsl.header
	for i := zsl.level - 1; i >= 0; i-- {
		next := cur.level[i].forward
		for next != nil && cur.level[i].span+rank < start {
			rank += cur.level[i].span
			cur = next
			next = cur.level[i].forward
		}
		pre[i] = cur
	}
	cur = cur.level[0].forward
	for cur != nil && start <= end {
		members = append(members, cur.member)
		next := cur.level[0].forward
		zsl.DeleteNode(cur, pre)
		cur = next
		start++
	}
	return
}
