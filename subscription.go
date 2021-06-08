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

func NewSubscription(store *Store, durableName string, startAt uint64, cf *ColumnFamily, fn StoreHandler) *Subscription {
	return &Subscription{
		store:        store,
		durableName:  durableName,
		lastSequence: startAt,
		cf:           cf,
		newTriggered: make(chan struct{}, 1),
		isClosed:     false,
		watchFn:      fn,
	}
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

	iter := sub.cf.Db.NewIter(nil)
	iter.SeekGE(Uint64ToBytes(sub.lastSequence))

	for !sub.isClosed {

		if !iter.Valid() {
			break
		}

		// Getting sequence number
		key := iter.Key()
		seq := BytesToUint64(key)

		// If we get record which is the same with last seq, find next one
		if seq == sub.lastSequence {

			// Get next one
			iter.Next()
			continue
		}

		sub.lastSequence = seq

		// Invoke data handler
		value := iter.Value()
		sub.handle(seq, value)

		// Get next one
		iter.Next()
	}

	iter.Close()
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
		err := sub.store.UpdateDurableState(sub.durableName, sub.lastSequence)
		if err == nil {
			return
		}

		fmt.Println(err)

		// Retry in second
		<-time.After(time.Second)
	}
}
