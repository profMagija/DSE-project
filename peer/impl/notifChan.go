package impl

import (
	"sync"
	"time"

	"golang.org/x/xerrors"
)

type NotifChan struct {
	cmap sync.Map
}

func (n *NotifChan) CreateChan(id string) {
	ch := make(chan interface{}, 1)
	n.cmap.Store(id, ch)
}

func (n *NotifChan) getChanOpt(id string) (chan interface{}, bool) {
	chi, ok := n.cmap.Load(id)
	if ok {
		return chi.(chan interface{}), ok
	} else {
		return nil, ok
	}
}

func (n *NotifChan) getChan(id string) chan interface{} {
	ch, ok := n.getChanOpt(id)
	if !ok {
		panic(xerrors.Errorf("wait channel removed"))
	}
	return ch
}

func (n *NotifChan) WaitChan(id string) interface{} {
	ch := n.getChan(id)
	return <-ch
}

func (n *NotifChan) WaitChanTimeout(id string, timeout time.Duration) (v interface{}, success bool) {
	ch := n.getChan(id)
	if timeout == 0 {
		v = <-ch
		success = true
	} else {
		select {
		case v = <-ch:
			success = true
		case <-time.After(timeout):
			v = nil
			success = false
		}
	}
	n.cmap.Delete(id)
	return
}

func (n *NotifChan) WaitChanNonblocking(id string) (v interface{}, ok bool) {
	ch := n.getChan(id)
	select {
	case v = <- ch:
		ok = true
	default:
		v = nil
		ok = false
	}
	n.cmap.Delete(id)
	return
}

func (n *NotifChan) WriteChan(id string, v interface{}) {
	ch, ok := n.getChanOpt(id)
	if ok {
		ch <- v
	}
}
