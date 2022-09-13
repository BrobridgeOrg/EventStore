package eventstore

import (
	"fmt"
	"os"
	"sync/atomic"
)

var testEventstore *EventStore
var testCounter int32

func createTestEventStore(name string, enabledSnapshot bool) {

	err := os.RemoveAll("./" + name)
	if err != nil {
		panic(err)
	}

	options := NewOptions()
	options.DatabasePath = "./" + name
	options.EnabledSnapshot = enabledSnapshot

	eventstore, err := CreateEventStore(options)
	if err != nil {
		panic(err)
	}

	testEventstore = eventstore
}

func closeTestEventStore() {
	testEventstore.Close()
}

func createTestStore(opts ...StoreOpt) *Store {

	counter := atomic.AddInt32(&testCounter, 1)
	name := "bench-" + fmt.Sprintf("%d", counter)

	// Create a new store for benchmark
	store, err := testEventstore.GetStore(name, opts...)
	if err != nil {
		panic(err)
	}

	return store
}
