package driver

import (
	"github.com/go-redis/redis"
	"strconv"
	"time"
)

var PoolKey = "P:H"

type Redis struct {
	R *redis.Client
}

func (r *Redis) SetHeartBeat(node string) {
	r.R.HSet(PoolKey, node, time.Now().Unix())
}

func (r *Redis) GetNodeList() []string {
	ex := int(time.Now().Unix() - 30)
	var c *redis.StringStringMapCmd
	if c = r.R.HGetAll(PoolKey); c.Err() != nil {
		return nil
	}
	var rs []string
	for k, v := range c.Val() {
		var vv int
		var err error
		if vv, err = strconv.Atoi(v); err != nil {
			r.R.HDel(PoolKey, k)
			continue
		}
		if vv < ex {
			r.R.HDel(PoolKey, k)
			continue
		}
		rs = append(rs,k)
	}
	return rs
}