package main

import (
	"SouthWind6510/TinyDB/db"
	"SouthWind6510/TinyDB/pkg/constants"
	"SouthWind6510/TinyDB/pkg/logger"
	"fmt"
	"path/filepath"
	"sync"
	"time"

	"github.com/tidwall/redcon"
)

type ServerOptions struct {
	path  string
	host  string
	port  string
	dbNum int
}

type Server struct {
	opt   ServerOptions
	dbs   []*db.TinyDB
	curDB *db.TinyDB
	mu    sync.RWMutex
}

func main() {
	start := time.Now()
	// open数据库
	opt := db.DefaultOptions(filepath.Join(constants.DefaultPath, "0"))
	curDB, err := db.Open(opt)
	if err != nil {
		logger.Log.Errorf("open db err: %+v", err)
		return
	}
	logger.Log.Infof("open db success, time cost: %v", time.Since(start))
	// 启动服务监听
	svr := &Server{dbs: []*db.TinyDB{curDB}, curDB: curDB}
	err = redcon.ListenAndServe(fmt.Sprintf(constants.ServerHost+":"+constants.ServerPort),
		execCommand,
		func(conn redcon.Conn) bool {
			// Use this function to accept or deny the connection.
			logger.Log.Printf("accept: %s", conn.RemoteAddr())
			conn.SetContext(svr)
			return true
		},
		func(conn redcon.Conn, err error) {
			// This is called when the connection has been closed
			logger.Log.Printf("closed: %s, err: %v", conn.RemoteAddr(), err)
		},
	)
	if err != nil {
		panic(err)
	}
}
