package eventstore

import "sync"

type Event struct {
	Sequence  uint64
	Data      []byte
	completed chan struct{}
}

var eventPool = sync.Pool{
	New: func() interface{} {
		return NewEvent()
	},
}

func NewEvent() *Event {
	return &Event{
		completed: make(chan struct{}, 1),
	}
}

func (event *Event) Ack() {
	event.completed <- struct{}{}
}

func (event *Event) reset() {
	close(event.completed)
	event.completed = make(chan struct{}, 1)
}

func (event *Event) Release() {
	eventPool.Put(event)
}
