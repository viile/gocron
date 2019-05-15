package tw

import (
	"time"
)

// Job callback function
type Job func() error

type TimeWheel interface {
	Start()
	AddTask(key interface{}, times int, interval time.Duration, job Job) error
}
