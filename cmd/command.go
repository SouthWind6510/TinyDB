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

// ======== redis协议相关操作 ========

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

// ======== String相关操作 ========

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
	res, err = s.curDB.Get(args[0])
	return
}
