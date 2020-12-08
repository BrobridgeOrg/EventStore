package eventstore

import (
	"fmt"
	"time"

	"github.com/tecbot/gorocksdb"
)

type StoreHandler func(*Event)

type Subscription struct {
	store        *Store
	durableName  string
	lastSequence uint64
	iterator     *gorocksdb.Iterator
	newTriggered chan struct{}
	watchFn      StoreHandler
	isClosed     bool
}

func NewSubscription(store *Store, durableName string, startAt uint64, iterator *gorocksdb.Iterator, fn StoreHandler) *Subscription {
	return &Subscription{
		store:        store,
		durableName:  durableName,
		lastSequence: startAt,
		iterator:     iterator,
		newTriggered: make(chan struct{}, 1),
		isClosed:     false,
		watchFn:      fn,
	}
}

func (sub *Subscription) Close() {
	close(sub.newTriggered)
	sub.isClosed = true
}

func (sub *Subscription) Watch() {

	sub.pull()

	for _ = range sub.newTriggered {
		sub.pull()
	}

	sub.iterator.Close()
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

	if sub.isClosed {
		return
	}

	sub.iterator.Seek(Uint64ToBytes(sub.lastSequence))

	for !sub.isClosed {

		if !sub.iterator.Valid() {
			break
		}

		// Getting sequence number
		key := sub.iterator.Key()
		seq := BytesToUint64(key.Data())
		key.Free()

		// If we get record which is the same with last seq, find next one
		if seq == sub.lastSequence {

			// Get next one
			sub.iterator.Next()
			continue
		}

		sub.lastSequence = seq

		// Invoke data handler
		value := sub.iterator.Value()
		sub.handle(seq, value.Data())
		value.Free()

		// Get next one
		sub.iterator.Next()
	}
}

func (sub *Subscription) handle(seq uint64, data []byte) {

	event := eventPool.Get().(*Event)
	event.Sequence = seq
	event.Data = data

	defer event.reset()
	defer eventPool.Put(event)

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
