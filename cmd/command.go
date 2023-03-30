package main

import (
	"SouthWind6510/TinyDB/db"
	"SouthWind6510/TinyDB/pkg/constants"
	"SouthWind6510/TinyDB/pkg/logger"
	"SouthWind6510/TinyDB/util"
	"fmt"
	"math"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/pkg/errors"
	"github.com/tidwall/redcon"
)

type cmdHandler func(*Server, [][]byte) (interface{}, error)

var cmdHandlersMap = map[string]cmdHandler{
	"ping":   (*Server).Ping,
	"select": (*Server).Select,

	"set":         (*Server).Set,
	"mset":        (*Server).MSet,
	"setex":       (*Server).SetEX,
	"setnx":       (*Server).SetNX,
	"msetnx":      (*Server).MSetNX,
	"psetex":      (*Server).PSetEX,
	"setrange":    (*Server).SetRange,
	"incr":        (*Server).Incr,
	"incrby":      (*Server).IncrBy,
	"incrbyfloat": (*Server).IncrByFloat,
	"decr":        (*Server).Decr,
	"decrby":      (*Server).DecrBy,
	"append":      (*Server).Append,
	"get":         (*Server).Get,
	"mget":        (*Server).MGet,
	"getrange":    (*Server).GetRange,
	"getset":      (*Server).GetSet,
	"getdel":      (*Server).GetDel,
	"getex":       (*Server).GetEX,
	"lcs":         (*Server).LCS,
	"strlen":      (*Server).StrLen,
	"substr":      (*Server).SubStr,

	"lpush":  (*Server).LPush,
	"rpush":  (*Server).RPush,
	"lpop":   (*Server).LPop,
	"rpop":   (*Server).RPop,
	"lindex": (*Server).LIndex,
	"llen":   (*Server).LLen,
	"lrange": (*Server).LRange,
	"lset":   (*Server).LSet,

	"hset":    (*Server).HSet,
	"hget":    (*Server).HGet,
	"hgetall": (*Server).HGetAll,
	"hdel":    (*Server).HDel,
	"hexists": (*Server).HExists,
	"hlen":    (*Server).HLen,
	"hkeys":   (*Server).HKeys,
	"hvals":   (*Server).HVals,
	"hincrby": (*Server).HIncrBy,
	"hmget":   (*Server).HMGet,
	"hmset":   (*Server).HMSet,
	"hscan":   (*Server).HScan,
	"hsetnx":  (*Server).HSetNX,

	"sadd":        (*Server).SAdd,
	"srem":        (*Server).SRem,
	"spop":        (*Server).SPop,
	"scard":       (*Server).SCard,
	"smembers":    (*Server).SMembers,
	"sismember":   (*Server).SIsMember,
	"smismember":  (*Server).SMIsMember,
	"srandmember": (*Server).SRandMember,
	"sscan":       (*Server).SScan,

	"zadd":             (*Server).ZAdd,
	"zcard":            (*Server).ZCard,
	"zcount":           (*Server).ZCount,
	"zincrby":          (*Server).ZIncrBy,
	"zscore":           (*Server).ZScore,
	"zmscore":          (*Server).ZMScore,
	"zpopmax":          (*Server).ZPopMax,
	"zpopmin":          (*Server).ZPopMin,
	"zrandmember":      (*Server).ZRandMember,
	"zrange":           (*Server).ZRange,
	"zrangebyscore":    (*Server).ZRangeByScore,
	"zrank":            (*Server).ZRank,
	"zrem":             (*Server).ZRem,
	"zremrangebyrank":  (*Server).ZRemRangeByRank,
	"zremrangebyscore": (*Server).ZRemRangeByScore,
	"zscan":            (*Server).ZScan,
}

func execCommand(conn redcon.Conn, cmd redcon.Command) {
	args := ""
	for _, arg := range cmd.Args {
		args += string(arg) + " "
	}
	logger.Log.Infof("start handler: %v", args)
	command := strings.ToLower(string(cmd.Args[0]))
	if handler, ok := cmdHandlersMap[command]; !ok {
		conn.WriteError(fmt.Sprintf("unsupported command: %v", command))
	} else {
		result, err := handler(conn.Context().(*Server), cmd.Args[1:])
		if err != nil {
			if errors.Is(err, constants.ErrKeyNotFound) {
				conn.WriteNull()
			} else {
				logger.Log.Errorf("exec command err: %+v", err)
				conn.WriteError(err.Error())
			}
		} else {
			logger.Log.Infof("result: %v", result)
			conn.WriteAny(result)
		}
	}
}

// ======== redis协议相关命令 ========

func (s *Server) Ping(args [][]byte) (res interface{}, err error) {
	return constants.ResultPong, nil
}

func (s *Server) Select(args [][]byte) (res interface{}, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	n, err := strconv.ParseInt(string(args[0]), 10, 0)
	if s.dbs[n] == nil {
		opt := db.DefaultOptions(filepath.Join(s.opt.path, strconv.Itoa(int(n))))
		s.dbs[n], err = db.Open(opt)
		if err != nil {
			return nil, err
		}
	}
	s.curDB = s.dbs[n]
	return constants.ResultOk, nil
}

// ======== String相关命令 ========

func (s *Server) Set(args [][]byte) (res interface{}, err error) {
	if len(args) < 2 {
		return nil, constants.ErrWrongNumberArgs
	}
	err = s.curDB.Set(args[0], args[1])
	if err == nil {
		res = constants.ResultOk
	}
	return
}

func (s *Server) MSet(args [][]byte) (res interface{}, err error) {
	if len(args) < 2 || len(args)%2 != 0 {
		return nil, constants.ErrWrongNumberArgs
	}
	for i := 0; i < len(args); i += 2 {
		err = s.curDB.Set(args[i], args[i+1])
		continue
	}
	res = constants.ResultOk
	return
}

func (s *Server) SetEX(args [][]byte) (res interface{}, err error) {
	return nil, constants.ErrUnsupportedCommand
}

func (s *Server) SetNX(args [][]byte) (res interface{}, err error) {
	if len(args) != 2 {
		return nil, constants.ErrWrongNumberArgs
	}
	return s.curDB.SetNX(args[0], args[1]), nil
}

func (s *Server) MSetNX(args [][]byte) (res interface{}, err error) {
	if len(args) < 2 || len(args)%2 != 0 {
		return nil, constants.ErrWrongNumberArgs
	}
	return s.curDB.MSetNX(args...), nil
}

func (s *Server) PSetEX(args [][]byte) (res interface{}, err error) {
	return nil, constants.ErrUnsupportedCommand
}

func (s *Server) SetRange(args [][]byte) (res interface{}, err error) {
	if len(args) != 3 {
		return nil, constants.ErrWrongNumberArgs
	}
	offset, err := strconv.Atoi(string(args[1]))
	if err != nil {
		return
	}
	return s.curDB.SetRange(args[0], args[2], offset)
}

func (s *Server) Incr(args [][]byte) (res interface{}, err error) {
	if len(args) != 1 {
		return nil, constants.ErrWrongNumberArgs
	}
	return s.curDB.Incr(args[0], 1)
}

func (s *Server) IncrBy(args [][]byte) (res interface{}, err error) {
	if len(args) != 2 {
		return nil, constants.ErrWrongNumberArgs
	}
	incr, err := strconv.ParseInt(string(args[1]), 10, 64)
	if err != nil {
		return nil, err
	}
	return s.curDB.Incr(args[0], incr)
}

func (s *Server) IncrByFloat(args [][]byte) (res interface{}, err error) {
	if len(args) != 2 {
		return nil, constants.ErrWrongNumberArgs
	}
	incr, err := strconv.ParseFloat(string(args[1]), 64)
	if err != nil {
		return nil, err
	}
	return s.curDB.IncrByFloat(args[0], incr)
}

func (s *Server) Decr(args [][]byte) (res interface{}, err error) {
	if len(args) != 1 {
		return nil, constants.ErrWrongNumberArgs
	}
	return s.curDB.Incr(args[0], -1)
}

func (s *Server) DecrBy(args [][]byte) (res interface{}, err error) {
	if len(args) != 2 {
		return nil, constants.ErrWrongNumberArgs
	}
	incr, err := strconv.ParseInt(string(args[1]), 10, 64)
	if err != nil {
		return nil, err
	}
	return s.curDB.Incr(args[0], -incr)
}

func (s *Server) Append(args [][]byte) (res interface{}, err error) {
	if len(args) != 2 {
		return nil, constants.ErrWrongNumberArgs
	}
	return s.curDB.Append(args[0], args[1])
}

func (s *Server) Get(args [][]byte) (res interface{}, err error) {
	if len(args) < 1 {
		return nil, constants.ErrWrongNumberArgs
	}
	return s.curDB.Get(args[0])
}

func (s *Server) MGet(args [][]byte) (interface{}, error) {
	if len(args) < 1 {
		return nil, constants.ErrWrongNumberArgs
	}
	res := make([]interface{}, len(args))
	for i, key := range args {
		if value, err := s.curDB.Get(key); err != nil {
			res[i] = nil
		} else {
			res[i] = string(value)
		}
	}
	return res, nil
}

func (s *Server) GetRange(args [][]byte) (res interface{}, err error) {
	if len(args) != 3 {
		return nil, constants.ErrWrongNumberArgs
	}
	start, err := strconv.Atoi(string(args[1]))
	if err != nil {
		return
	}
	end, err := strconv.Atoi(string(args[2]))
	if err != nil {
		return
	}
	return s.curDB.GetRange(args[0], start, end)
}

func (s *Server) GetSet(args [][]byte) (res interface{}, err error) {
	if len(args) != 2 {
		return nil, constants.ErrWrongNumberArgs
	}
	old, err := s.curDB.Get(args[0])
	_ = s.curDB.Set(args[0], args[1])
	if err != nil {
		return nil, nil
	}
	return old, nil
}

func (s *Server) GetDel(args [][]byte) (res interface{}, err error) {
	if len(args) != 1 {
		return nil, constants.ErrWrongNumberArgs
	}
	return s.curDB.GetDel(args[0]), nil
}

func (s *Server) GetEX(args [][]byte) (res interface{}, err error) {
	return nil, constants.ErrUnsupportedCommand
}

func (s *Server) LCS(args [][]byte) (res interface{}, err error) {
	return nil, constants.ErrUnsupportedCommand
}

func (s *Server) StrLen(args [][]byte) (res interface{}, err error) {
	if len(args) != 1 {
		return nil, constants.ErrWrongNumberArgs
	}
	bytes, err := s.curDB.Get(args[0])
	if err != nil {
		return 0, nil
	}
	return len(string(bytes)), nil
}

func (s *Server) SubStr(args [][]byte) (res interface{}, err error) {
	return s.GetRange(args)
}

// ======== List相关命令 ========

func (s *Server) LPush(args [][]byte) (res interface{}, err error) {
	if len(args) < 2 {
		return nil, constants.ErrWrongNumberArgs
	}
	return s.curDB.LPush(args[0], true, args[1:]...)
}

func (s *Server) RPush(args [][]byte) (res interface{}, err error) {
	if len(args) < 2 {
		return nil, constants.ErrWrongNumberArgs
	}
	return s.curDB.LPush(args[0], false, args[1:]...)
}

func (s *Server) LPop(args [][]byte) (res interface{}, err error) {
	if len(args) < 1 {
		return nil, constants.ErrWrongNumberArgs
	}
	count := 1
	if len(args) > 1 {
		count, err = strconv.Atoi(string(args[1]))
		if err != nil {
			return nil, err
		}
	}
	return s.curDB.LPop(args[0], count, true)
}

func (s *Server) RPop(args [][]byte) (res interface{}, err error) {
	if len(args) < 1 {
		return nil, constants.ErrWrongNumberArgs
	}
	count := 1
	if len(args) > 1 {
		count, err = strconv.Atoi(string(args[1]))
		if err != nil {
			return nil, err
		}
	}
	return s.curDB.LPop(args[0], count, false)
}

func (s *Server) LIndex(args [][]byte) (res interface{}, err error) {
	if len(args) < 2 {
		return nil, constants.ErrWrongNumberArgs
	}
	offset, err := strconv.Atoi(string(args[1]))
	if err != nil {
		return nil, err
	}
	return s.curDB.LIndex(args[0], offset)
}

func (s *Server) LLen(args [][]byte) (res interface{}, err error) {
	if len(args) < 1 {
		return nil, constants.ErrWrongNumberArgs
	}
	return s.curDB.LLen(args[0])
}

func (s *Server) LRange(args [][]byte) (res interface{}, err error) {
	if len(args) < 3 {
		return nil, constants.ErrWrongNumberArgs
	}
	sOffset, err := strconv.Atoi(string(args[1]))
	if err != nil {
		return nil, err
	}
	eOffset, err := strconv.Atoi(string(args[2]))
	if err != nil {
		return nil, err
	}
	return s.curDB.LRange(args[0], sOffset, eOffset)
}

func (s *Server) LSet(args [][]byte) (res interface{}, err error) {
	if len(args) < 3 {
		return nil, constants.ErrWrongNumberArgs
	}
	offset, err := strconv.Atoi(string(args[1]))
	if err != nil {
		return nil, err
	}
	err = s.curDB.LSet(args[0], offset, args[2])
	if err != nil {
		return nil, err
	}
	return constants.ResultOk, nil
}

// TODO
func (s *Server) LTrim(args [][]byte) (res interface{}, err error) {
	return
}

// TODO
func (s *Server) LInsert(args [][]byte) (res interface{}, err error) {
	return
}

// TODO
func (s *Server) LRem(args [][]byte) (res interface{}, err error) {
	return
}

// TODO
func (s *Server) LMove(args [][]byte) (res interface{}, err error) {
	return
}

// TODO
func (s *Server) LPos(args [][]byte) (res interface{}, err error) {
	return
}

// ======== Hash相关命令 ========

func (s *Server) HSet(args [][]byte) (res interface{}, err error) {
	if len(args) < 3 {
		return nil, constants.ErrWrongNumberArgs
	}
	return s.curDB.HSet(args[0], args[1:]...)
}

func (s *Server) HGet(args [][]byte) (res interface{}, err error) {
	if len(args) < 2 {
		return nil, constants.ErrWrongNumberArgs
	}
	return s.curDB.HGet(args[0], args[1])
}

func (s *Server) HGetAll(args [][]byte) (res interface{}, err error) {
	if len(args) < 1 {
		return nil, constants.ErrWrongNumberArgs
	}
	return s.curDB.HGetAll(args[0])
}

func (s *Server) HDel(args [][]byte) (res interface{}, err error) {
	if len(args) < 2 {
		return nil, constants.ErrWrongNumberArgs
	}
	return s.curDB.HDel(args[0], args[1:]...)
}

func (s *Server) HExists(args [][]byte) (res interface{}, err error) {
	if len(args) < 2 {
		return nil, constants.ErrWrongNumberArgs
	}
	return s.curDB.HExists(args[0], args[1])
}

func (s *Server) HLen(args [][]byte) (res interface{}, err error) {
	if len(args) < 1 {
		return nil, constants.ErrWrongNumberArgs
	}
	return s.curDB.HLen(args[0])
}

func (s *Server) HKeys(args [][]byte) (res interface{}, err error) {
	if len(args) < 1 {
		return nil, constants.ErrWrongNumberArgs
	}
	return s.curDB.HKeys(args[0])
}

func (s *Server) HVals(args [][]byte) (res interface{}, err error) {
	if len(args) < 1 {
		return nil, constants.ErrWrongNumberArgs
	}
	return s.curDB.HVals(args[0])
}

func (s *Server) HIncrBy(args [][]byte) (res interface{}, err error) {
	if len(args) < 3 {
		return nil, constants.ErrWrongNumberArgs
	}
	incr, err := strconv.Atoi(string(args[2]))
	if err != nil {
		return nil, err
	}
	return s.curDB.HIncrBy(args[0], args[1], incr)
}

func (s *Server) HMGet(args [][]byte) (res interface{}, err error) {
	if len(args) < 2 {
		return nil, constants.ErrWrongNumberArgs
	}
	return s.curDB.HMGet(args[0], args[1:]...)
}

func (s *Server) HMSet(args [][]byte) (res interface{}, err error) {
	if len(args) < 3 {
		return nil, constants.ErrWrongNumberArgs
	}
	err = s.curDB.HMSet(args[0], args[1:]...)
	if err != nil {
		return nil, err
	}
	return constants.ResultOk, nil
}

func (s *Server) HSetNX(args [][]byte) (res interface{}, err error) {
	if len(args) < 3 {
		return nil, constants.ErrWrongNumberArgs
	}
	return s.curDB.HSetNX(args[0], args[1], args[2])
}

// TODO
func (s *Server) HScan(args [][]byte) (res interface{}, err error) {
	return nil, constants.ErrUnsupportedCommand
}

// ======== Set相关命令 ========

func (s *Server) SAdd(args [][]byte) (res interface{}, err error) {
	if len(args) < 2 {
		return nil, constants.ErrWrongNumberArgs
	}
	return s.curDB.SAdd(args[0], args[1:]...)
}

func (s *Server) SRem(args [][]byte) (res interface{}, err error) {
	if len(args) < 2 {
		return nil, constants.ErrWrongNumberArgs
	}
	return s.curDB.SRem(args[0], args[1:]...)
}

func (s *Server) SPop(args [][]byte) (res interface{}, err error) {
	if len(args) < 1 {
		return nil, constants.ErrWrongNumberArgs
	}
	count := 1
	if len(args) == 2 {
		count, err = strconv.Atoi(string(args[1]))
		if err != nil {
			return nil, err
		}
	}
	return s.curDB.SPop(args[0], count)
}

func (s *Server) SCard(args [][]byte) (res interface{}, err error) {
	if len(args) < 1 {
		return nil, constants.ErrWrongNumberArgs
	}
	return s.curDB.SCard(args[0])
}

func (s *Server) SMembers(args [][]byte) (res interface{}, err error) {
	if len(args) < 1 {
		return nil, constants.ErrWrongNumberArgs
	}
	return s.curDB.SMembers(args[0])
}

func (s *Server) SIsMember(args [][]byte) (res interface{}, err error) {
	if len(args) < 2 {
		return nil, constants.ErrWrongNumberArgs
	}
	return s.curDB.SIsMember(args[0], args[1])
}

func (s *Server) SMIsMember(args [][]byte) (res interface{}, err error) {
	if len(args) < 3 {
		return nil, constants.ErrWrongNumberArgs
	}
	return s.curDB.SMIsMember(args[0], args[1:]...)
}

func (s *Server) SRandMember(args [][]byte) (res interface{}, err error) {
	if len(args) < 1 {
		return nil, constants.ErrWrongNumberArgs
	}
	count := 1
	if len(args) == 2 {
		count, err = strconv.Atoi(string(args[1]))
		if err != nil {
			return nil, err
		}
	}
	return s.curDB.SRandMember(args[0], count)
}

// TODO
func (s *Server) SScan(args [][]byte) (res interface{}, err error) {
	return nil, constants.ErrUnsupportedCommand
}

// ======== ZSet相关命令 ========

func (s *Server) ZAdd(args [][]byte) (res interface{}, err error) {
	var opt1, opt2, opt3, opt4 string
	index := 1
loop:
	for index = 1; index < util.MinInt(5, len(args)); index++ {
		switch strings.ToLower(string(args[index])) {
		case "nx":
			opt1 = "nx"
		case "xx":
			opt1 = "xx"
		case "gt":
			opt2 = "gt"
		case "lt":
			opt2 = "lt"
		case "ch":
			opt3 = "ch"
		case "incr":
			opt4 = "incr"
		default:
			break loop
		}
	}
	if len(args[index:]) == 0 {
		return nil, constants.ErrWrongNumberArgs
	}
	return s.curDB.ZAdd(args[0], opt1, opt2, opt3, opt4, args[index:]...)
}

func (s *Server) ZCard(args [][]byte) (res interface{}, err error) {
	if len(args) < 1 {
		return nil, constants.ErrWrongNumberArgs
	}
	return s.curDB.ZCard(args[0])
}

func (s *Server) ZCount(args [][]byte) (res interface{}, err error) {
	if len(args) < 3 {
		return nil, constants.ErrWrongNumberArgs
	}
	var min, max float64
	if string(args[1]) == "-inf" {
		min = math.MinInt64
	} else {
		min, err = strconv.ParseFloat(string(args[1]), 64)
		if err != nil {
			return nil, err
		}
	}
	if string(args[2]) == "+inf" {
		max = math.MaxInt64
	} else {
		max, err = strconv.ParseFloat(string(args[2]), 64)
		if err != nil {
			return nil, err
		}
	}
	return s.curDB.ZCount(args[0], min, max)
}

func (s *Server) ZIncrBy(args [][]byte) (res interface{}, err error) {
	if len(args) < 3 {
		return nil, constants.ErrWrongNumberArgs
	}
	incr, err := strconv.ParseFloat(string(args[1]), 64)
	if err != nil {
		return nil, err
	}
	return s.curDB.ZIncrBy(args[0], incr, args[2])
}

func (s *Server) ZScore(args [][]byte) (res interface{}, err error) {
	if len(args) < 2 {
		return nil, constants.ErrWrongNumberArgs
	}
	return s.curDB.ZMScore(args[0], args[1])
}

func (s *Server) ZMScore(args [][]byte) (res interface{}, err error) {
	if len(args) < 2 {
		return nil, constants.ErrWrongNumberArgs
	}
	return s.curDB.ZMScore(args[0], args[1:]...)
}

func (s *Server) ZPopMax(args [][]byte) (res interface{}, err error) {
	if len(args) < 1 {
		return nil, constants.ErrWrongNumberArgs
	}
	count := 1
	if len(args) == 2 {
		count, err = strconv.Atoi(string(args[1]))
		if err != nil {
			return nil, err
		}
	}
	return s.curDB.ZPop(args[0], false, count)
}

func (s *Server) ZPopMin(args [][]byte) (res interface{}, err error) {
	if len(args) < 1 {
		return nil, constants.ErrWrongNumberArgs
	}
	count := 1
	if len(args) == 2 {
		count, err = strconv.Atoi(string(args[1]))
		if err != nil {
			return nil, err
		}
	}
	return s.curDB.ZPop(args[0], true, count)
}

func (s *Server) ZRandMember(args [][]byte) (res interface{}, err error) {
	if len(args) < 1 {
		return nil, constants.ErrWrongNumberArgs
	}
	withscores := false
	count := 1
	for i := 1; i < len(args); i++ {
		if strings.ToLower(string(args[i])) == "withscores" {
			withscores = true
		} else {
			count, err = strconv.Atoi(string(args[1]))
			if err != nil {
				return nil, err
			}
		}
	}
	return s.curDB.ZRandMember(args[0], count, withscores)
}

// ZRange key start stop [BYSCORE] [REV] [WITHSCORES]
func (s *Server) ZRange(args [][]byte) (res interface{}, err error) {
	if len(args) < 3 {
		return nil, constants.ErrWrongNumberArgs
	}
	var start, end float64
	if string(args[1]) == "-inf" {
		start = math.MinInt64
	} else {
		start, err = strconv.ParseFloat(string(args[1]), 64)
		if err != nil {
			return nil, err
		}
	}
	if string(args[2]) == "+inf" {
		end = math.MaxInt64
	} else {
		end, err = strconv.ParseFloat(string(args[2]), 64)
		if err != nil {
			return nil, err
		}
	}
	byScore, rev, withScores := false, false, 0
	for i := 3; i < len(args); i++ {
		switch strings.ToLower(string(args[i])) {
		case "byscore":
			byScore = true
		case "rev":
			rev = true
		case "withscores":
			withScores = 1
		}
	}
	return s.curDB.ZRange(args[0], start, end, byScore, rev, withScores)
}

// ZRangeByScore As of Redis version 6.2.0, this command is regarded as deprecated.
// It can be replaced by ZRANGE with the BYSCORE argument when migrating or writing new code.
func (s *Server) ZRangeByScore(args [][]byte) (res interface{}, err error) {
	if len(args) < 3 {
		return nil, constants.ErrWrongNumberArgs
	}
	return s.ZRange(args)
}

func (s *Server) ZRank(args [][]byte) (res interface{}, err error) {
	if len(args) < 2 {
		return nil, constants.ErrWrongNumberArgs
	}
	withscore := false
	if len(args) == 3 && strings.ToLower(string(args[2])) == "withscore" {
		withscore = true
	}
	return s.curDB.ZRank(args[0], args[1], withscore)
}

func (s *Server) ZRem(args [][]byte) (res interface{}, err error) {
	if len(args) < 2 {
		return nil, constants.ErrWrongNumberArgs
	}
	return s.curDB.ZRem(args[0], args[1:]...)
}

func (s *Server) ZRemRangeByRank(args [][]byte) (res interface{}, err error) {
	if len(args) < 3 {
		return nil, constants.ErrWrongNumberArgs
	}
	start, err := strconv.Atoi(string(args[1]))
	if err != nil {
		return nil, err
	}
	stop, err := strconv.Atoi(string(args[2]))
	if err != nil {
		return nil, err
	}
	return s.curDB.ZRemRange(args[0], float64(start), float64(stop), false)
}

func (s *Server) ZRemRangeByScore(args [][]byte) (res interface{}, err error) {
	if len(args) < 3 {
		return nil, constants.ErrWrongNumberArgs
	}
	min, err := strconv.ParseFloat(string(args[1]), 64)
	if err != nil {
		return nil, err
	}
	max, err := strconv.ParseFloat(string(args[2]), 64)
	if err != nil {
		return nil, err
	}
	return s.curDB.ZRemRange(args[0], min, max, true)
}

func (s *Server) ZScan(args [][]byte) (res interface{}, err error) {
	return nil, constants.ErrUnsupportedCommand
}
