package impl

import (
	"sync"
	"time"
)

type Watchdog struct {
	l       sync.Mutex
	resetCh chan bool
	deadCh  chan bool
}

func NewWatchdog() *Watchdog {
	return &Watchdog{
		deadCh: make(chan bool, 10),
	}
}

func (wd *Watchdog) GetChannel() chan bool {
	return wd.deadCh
}

func (wd *Watchdog) Start(timeout time.Duration) {
	// wd.l.Lock()
	// defer wd.l.Unlock()

	if wd.resetCh != nil {
		panic("Watchdog already in use")
	}
	
	wd.resetCh = make(chan bool, 10)

	go func() {
		going := true
		for going {

			select {
			case c := <-wd.resetCh:
				if c {
					wd.deadCh <- false
					going = false
				}
			case <-time.After(timeout):
				wd.deadCh <- true
				going = false
			}
		}

		// wd.l.Lock()
		// defer wd.l.Unlock()

		wd.resetCh = nil
	}()
}

func (wd *Watchdog) Update() {
	wd.sendUpdate(false)
}

func (wd *Watchdog) Stop() {
	wd.sendUpdate(true)
}

func (wd *Watchdog) sendUpdate(value bool) {
	// wd.l.Lock()
	// defer wd.l.Unlock()
	wd.resetCh <- value
}
