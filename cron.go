package main

import (
	"github.com/viile/gocron/driver"
	"github.com/viile/gocron/tw"
	"hash/crc32"
	"log"
	"sort"
	"sync"
	"time"
)

type CalcFunc func(f string) bool
type Hash func(data []byte) uint32
type CronFunc func(CalcFunc) bool

type Cron struct {
	// job 返回true 下次执行延迟时间
	Ts time.Duration
	// job 返回false 下次执行延迟时间
	Fs time.Duration
	// job
	F  CronFunc
}

type CronManager struct {
	R driver.Driver
	// TimeWheel
	T tw.TimeWheel
	// node 名称
	NO string
	// node hash
	H uint32
	// calc func
	calc CalcFunc
	// hash func
	hash Hash
	// cron map
	CronMap *sync.Map
}

func NewCronManager(n string, r driver.Driver, t tw.TimeWheel) *CronManager {
	c := &CronManager{
		R:       r,
		T:       t,
		NO:      n,
		hash:    crc32.ChecksumIEEE,
		CronMap: &sync.Map{},
	}
	c.H = c.hash([]byte(c.NO))
	c.calc = func(f string) bool {
		return false
	}
	// register node
	c.R.SetHeartBeat(c.NO)
	// calc node pool
	c.calcPool()

	return c
}

func (c *CronManager) AddCron(flag string, cr Cron) {
	c.CronMap.Store(flag, cr)
}

func (c *CronManager) handle(flag string) {
	var cr Cron
	if r, ok := c.CronMap.Load(flag); !ok {
		return
	} else {
		cr = r.(Cron)
		if cr.F(c.calc) {
			c.T.AddTask(c.NO, 1, cr.Ts, func() error {
				c.handle(flag)
				return nil
			})
		} else {
			c.T.AddTask(c.NO, 1, cr.Fs, func() error {
				c.handle(flag)
				return nil
			})
		}
	}
}

func (c *CronManager) calcPool() {
	nodes := c.R.GetNodeList()
	var ks []uint32
	for _, v := range nodes {
		ks = append(ks, c.hash([]byte(v)))
	}
	sort.Slice(ks, func(i, j int) bool { return ks[i] < ks[j] })

	var left uint32
	for _, v := range ks {
		if v == c.H {
			break
		}
		left = v
	}
	if left == 0 {
		for _, v := range ks {
			left = v
		}
	}

	c.calc = func(f string) bool {
		h := c.hash([]byte(f))
		log.Println(f, h, left, c.H)
		if left < c.H {
			if left < h && h <= c.H {
				return true
			}
		} else {
			if h > left || h <= c.H {
				return true
			}
		}

		return false
	}
}

func (c *CronManager) Run() {
	// 设置心跳时间
	c.T.AddTask(c.NO, -1, time.Second*3, func() error {
		c.R.SetHeartBeat(c.NO)
		return nil
	})
	// 设置同步时间
	c.T.AddTask(c.NO, -1, time.Second*15, func() error {
		c.calcPool()
		return nil
	})
	c.CronMap.Range(func(key, value interface{}) bool {
		c.handle(key.(string))

		return true
	})

}
