package eventstore

import (
	"strconv"
	"sync"
	"testing"
)

func init() {
	createTestEventStore("testing")
}

func TestWrite(t *testing.T) {

	store := createTestStore()
	defer store.Close()

	if _, err := store.Write([]byte("Benchmark")); err != nil {
		t.Error(err)
	}
}

func TestSubscription(t *testing.T) {

	store := createTestStore()
	defer store.Close()

	var wg sync.WaitGroup

	// Subscription to store
	_, err := store.Subscribe(store.name, 0, func(event *Event) {
		//		t.Logf("%d %s", seq, string(data))
		event.Ack()
		wg.Done()
	})
	if err != nil {
		panic(err)
	}

	wg.Add(10)
	go func() {
		for i := 0; i < 10; i++ {
			if _, err := store.Write([]byte("Benchmark" + strconv.Itoa(i))); err != nil {
				t.Error(err)
			}
		}
	}()

	wg.Wait()
}

func TestSubscriptionWithDurableName(t *testing.T) {

	store := createTestStore()
	defer store.Close()

	var wg sync.WaitGroup
	var lastSeq uint64 = 0
	storeName := store.name

	// Subscribe to store
	sub, err := store.Subscribe(storeName, lastSeq, func(event *Event) {
		//		t.Logf("%d %s", seq, string(data))

		lastSeq++
		if lastSeq != event.Sequence {
			t.Fail()
		}

		event.Ack()
		wg.Done()
	})
	if err != nil {
		panic(err)
	}

	wg.Add(10)
	go func() {
		for i := 0; i < 10; i++ {
			if _, err := store.Write([]byte("Benchmark" + strconv.Itoa(i))); err != nil {
				t.Error(err)
			}
		}
	}()

	wg.Wait()

	// Close current subscription
	sub.Close()

	// Subscribe to store
	_, err = store.Subscribe(storeName, lastSeq, func(event *Event) {

		lastSeq++
		if lastSeq != event.Sequence {
			t.Fail()
		}

		event.Ack()
		wg.Done()
	})
	if err != nil {
		t.Error(err)
	}

	wg.Add(10)
	go func() {
		for i := 0; i < 10; i++ {
			if _, err := store.Write([]byte("Benchmark" + strconv.Itoa(i))); err != nil {
				t.Error(err)
			}
		}
	}()

	wg.Wait()
}
