package main

import (
	"github.com/go-redis/redis"
	"github.com/viile/gocron/driver"
	"github.com/viile/gocron/tw"
	"math/rand"
	"strconv"
	"sync"
	"time"
)

var data *sync.Map = &sync.Map{}
var rs = rand.New(rand.NewSource(time.Now().UnixNano()))
func main() {
	r := &driver.Redis{
		R: redis.NewClient(
			&redis.Options{
				Addr:         "127.0.0.1:6379",
			}),
	}

	cron := Cron{
		Ts: time.Second * 1,
		Fs: time.Second * 2,
		F: func(calcFunc CalcFunc) bool {
			return Loop(calcFunc)
		},
	}

	// timewheel
	t := tw.NewTimeWheel(time.Second, 180)
	t.Start()

	cm1 := NewCronManager("1", r,t)
	cm2 := NewCronManager("2", r,t)

	cm1.AddCron("handle1", cron)
	cm2.AddCron("handle6", cron)

	cm1.Run()
	cm2.Run()

	time.Sleep(time.Second * 10000)
}

func Loop(calcFunc CalcFunc) bool {
	r := rs.Intn(10000)
	if calcFunc(strconv.Itoa(r)) {
		if v, ok := data.Load(r); ok {
			data.Store(r, v.(int) + 1)
			return true
		} else {
			data.Store(r, 1)
		}
	}

	return false
}
