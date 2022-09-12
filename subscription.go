package eventstore

import (
	"fmt"
	"sync"
	"time"

	"github.com/cockroachdb/pebble"
)

type StoreHandler func(*Event)

type Subscription struct {
	store        *Store
	durableName  string
	lastSequence uint64
	iterator     *pebble.Iterator
	cf           *ColumnFamily
	newTriggered chan struct{}
	watchFn      StoreHandler
	isClosed     bool
	mutex        sync.Mutex
}

type SubOpt func(s *Subscription)

func DurableName(durable string) SubOpt {
	return func(s *Subscription) {
		s.durableName = durable
	}
}

func StartAtSequence(seq uint64) SubOpt {
	return func(s *Subscription) {
		s.lastSequence = seq
	}
}

func NewSubscription(store *Store, cf *ColumnFamily, fn StoreHandler, opts ...SubOpt) *Subscription {
	s := &Subscription{
		store:        store,
		cf:           cf,
		durableName:  "",
		newTriggered: make(chan struct{}, 1),
		isClosed:     false,
		watchFn:      fn,
	}

	for _, o := range opts {
		o(s)
	}

	// Load last sequence from store for durable name
	if len(s.durableName) != 0 && s.lastSequence == 0 {
		s.lastSequence, _ = store.GetDurableState(s.durableName)
	}

	return s
}

func (sub *Subscription) Close() {

	close(sub.newTriggered)

	sub.mutex.Lock()
	sub.isClosed = true
	sub.mutex.Unlock()

}

func (sub *Subscription) Watch() {

	sub.pull()

	for _ = range sub.newTriggered {
		sub.pull()
	}

	//	sub.iterator.Close()
}

func (sub *Subscription) Trigger() error {

	if sub.isClosed {
		return nil
	}

	select {
	case sub.newTriggered <- struct{}{}:
	default:
		return nil
	}

	return nil
}

func (sub *Subscription) pull() {

	sub.mutex.Lock()
	defer sub.mutex.Unlock()

	if sub.isClosed {
		return
	}

	cur, _ := sub.cf.List([]byte(""), Uint64ToBytes(sub.lastSequence), &ListOptions{
		WithoutRawPrefix: true,
	})

	for !sub.isClosed {

		if cur.EOF() {
			break
		}

		// Getting sequence number
		key := cur.GetKey()
		seq := BytesToUint64(key)

		// If we get record which is the same with last seq, find next one
		if seq == sub.lastSequence {

			// Get next one
			cur.Next()
			continue
		}

		sub.lastSequence = seq

		// Invoke data handler
		value := cur.GetData()
		sub.handle(seq, value)

		// Get next one
		cur.Next()
	}

	cur.Close()
}

func (sub *Subscription) handle(seq uint64, data []byte) {

	event := eventPool.Get().(*Event)
	event.Sequence = seq
	event.Data = data

	defer eventPool.Put(event)
	defer event.reset()

	sub.watchFn(event)

	for {
		if sub.isClosed {
			return
		}

		select {
		case <-event.completed:
			sub.updateDurableOffset()
			return
		default:
		}
	}
}

func (sub *Subscription) updateDurableOffset() {

	for !sub.isClosed {

		// Update offset for this durable name
		err := sub.store.UpdateDurableState(nil, sub.durableName, sub.lastSequence)
		if err == nil {
			return
		}

		fmt.Println(err)

		// Retry in second
		<-time.After(time.Second)
	}
}
