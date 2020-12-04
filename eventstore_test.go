package eventstore

import (
	"fmt"
	"os"
	"sync/atomic"
)

var testEventstore *EventStore
var testCounter int32

func createTestEventStore(name string) {

	err := os.RemoveAll("./" + name)
	if err != nil {
		panic(err)
	}

	options := NewOptions()
	options.DatabasePath = "./" + name

	eventstore, err := CreateEventStore(options)
	if err != nil {
		panic(err)
	}

	testEventstore = eventstore
}

func createTestStore() *Store {

	counter := atomic.AddInt32(&testCounter, 1)
	name := "bench-" + fmt.Sprintf("%d", counter)

	// Create a new store for benchmark
	store, err := testEventstore.GetStore(name)
	if err != nil {
		panic(err)
	}

	return store
}
