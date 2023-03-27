# TinyDB
> 一款基于Bitcask模型建立的KV式数据库
1. 支持Redis协议，实现了大部分常用命令，详见「支持的命令」，与Redis协议使用方式不同的命令会特殊说明；
2. Bitcask模型使用文件末尾追加的方式写入，写性能很高；
3. 支持String、List、Hash、Set、ZSet五种数据结构。

## 快速使用
### 1. 构建应用并启动服务
```bash
make
./tinydb-server
```
### 2. 命令行使用
使用redis-cli连接服务
```bash
redis-cli -p 6388
```
### 3. 个人应用中使用
在你的应用可以使用支持Redis协议的库来连接服务，比如go-redis、redis-py，并不局限于Go应用。
## 支持的命令
### String
SET

GET

### List
LPUSH

RPUSH

LPOP

RPOP

LRANGE

LINDEX

LSET

LLEN

### Hash
HSET

HGET

HGETALL

HDEL

HLEN

HSETNX

HKEYS

HVALS

HINCRBY

HEXISTS

HMGET

HMSET

### Set
SADD

SREM

SPOP

SSCAN

SMEMBERS

SISMEMBER

SMISMEMBER

SRANDMEMBER

### ZSet
ZADD

ZCARD

ZCOUNT

ZINCRBY

ZSCORE

ZMSCORE

ZPOPMAX

ZPOPMIN

ZRANDMEMBER

ZRANGE
> ZRange key start stop [BYSCORE] [REV] [WITHSCORES]，默认ByRank

ZRANGEBYSCORE

ZRANK

ZREM

ZREMBYRANK

ZREMBYSCORE