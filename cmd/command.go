package main

import (
	"SouthWind6510/TinyDB/db"
	"SouthWind6510/TinyDB/pkg/constants"
	"SouthWind6510/TinyDB/pkg/logger"
	"fmt"
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

	"set": (*Server).Set,
	"get": (*Server).Get,

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

func (s *Server) Get(args [][]byte) (res interface{}, err error) {
	if len(args) < 1 {
		return nil, constants.ErrWrongNumberArgs
	}
	return s.curDB.Get(args[0])
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
