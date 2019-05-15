package tw

import (
	"container/list"
	"errors"
	"sync"
	"time"
)

// time wheel struct
type timeWheel struct {
	interval       time.Duration
	ticker         *time.Ticker
	slots          []*list.List
	currentPos     int
	currentLvl     int
	slotNum        int
	addTaskChannel chan *task
	stopChannel    chan bool
	taskRecord     *sync.Map
}

type task struct {
	interval time.Duration
	job      Job
	key      interface{}
	circle   int
	times    int
}

// New create a empty time wheel
func NewTimeWheel(interval time.Duration, slotNum int) *timeWheel {
	if interval <= 0 || slotNum <= 0 {
		return nil
	}
	tw := &timeWheel{
		interval:       interval,
		slots:          make([]*list.List, slotNum),
		currentPos:     0,
		currentLvl:     0,
		slotNum:        slotNum,
		addTaskChannel: make(chan *task),
		stopChannel:    make(chan bool),
		taskRecord:     &sync.Map{},
	}

	tw.init()

	return tw
}

// Start start the time wheel
func (tw *timeWheel) Start() {
	tw.ticker = time.NewTicker(tw.interval)
	go tw.start()
}

// Stop stop the time wheel
func (tw *timeWheel) Stop() {
	tw.stopChannel <- true
}

func (tw *timeWheel) start() {
	for {
		select {
		case <-tw.ticker.C:
			tw.tickHandler()
		case task := <-tw.addTaskChannel:
			tw.addTask(task)
		case <-tw.stopChannel:
			tw.ticker.Stop()
			return
		}
	}
}

// AddTask add new task to the time wheel
func (tw *timeWheel) AddTask(key interface{}, times int, interval time.Duration, job Job) error {
	if interval <= 0 || times == 0 || job == nil {
		return errors.New("illegal task params")
	}

	tw.addTaskChannel <- &task{key: key, interval: interval, job: job, times: times}
	return nil
}

// time wheel initialize
func (tw *timeWheel) init() {
	for i := 0; i < tw.slotNum; i++ {
		tw.slots[i] = list.New()
	}
}

//
func (tw *timeWheel) tickHandler() {
	l := tw.slots[tw.currentPos]
	tw.scanAddRunTask(l)
	if tw.currentPos == tw.slotNum-1 {
		tw.currentPos = 0
	} else {
		tw.currentPos++
	}
}

// add task
func (tw *timeWheel) addTask(task *task) {
	pos, circle := tw.getPositionAndCircle(task.interval)
	task.circle = circle

	tw.slots[pos].PushBack(task)

	//record the task
	tw.taskRecord.Store(task.key, task)
	//log.Println(task)
}

// scan task list and run the task
func (tw *timeWheel) scanAddRunTask(l *list.List) {
	if l == nil {
		return
	}

	for item := l.Front(); item != nil; {
		task := item.Value.(*task)

		if task.circle > 0 {
			task.circle--
			item = item.Next()
			continue
		}

		go task.job()
		next := item.Next()
		l.Remove(item)
		item = next

		if task.times == -1 {
			tw.addTask(task)
		} else if task.times > 1 {
			task.times--
			tw.addTask(task)
		} else {
			tw.taskRecord.Delete(task.key)
		}

	}
}

// get the task position
func (tw *timeWheel) getPositionAndCircle(d time.Duration) (pos int, circle int) {
	delaySeconds := int(d.Seconds())
	intervalSeconds := int(tw.interval.Seconds())

	circle = int(delaySeconds / intervalSeconds / tw.slotNum)
	pos = int(tw.currentPos+delaySeconds/intervalSeconds) % tw.slotNum

	//log.Println(delaySeconds,intervalSeconds,pos,circle)
	return
}
