package eventstore

import (
	"strconv"
	"sync"
	"testing"
)

func TestWrite(t *testing.T) {

	createTestEventStore("testing", false)
	defer closeTestEventStore()

	store := createTestStore()

	if _, err := store.Write([]byte("Benchmark")); err != nil {
		t.Error(err)
	}
}

func TestSubscription(t *testing.T) {

	createTestEventStore("testing", false)
	defer closeTestEventStore()

	store := createTestStore()

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

func TestSubscriptionOffset(t *testing.T) {

	createTestEventStore("testing", false)
	defer closeTestEventStore()

	store := createTestStore()

	var wg sync.WaitGroup

	// Subscription to store
	sub, err := store.Subscribe(store.name, 0, func(event *Event) {
		//		t.Logf("%d %s", seq, string(data))
		event.Ack()

		wg.Done()
	})
	if err != nil {
		panic(err)
	}

	wg.Add(50)
	go func() {
		for i := 1; i <= 50; i++ {
			if _, err := store.Write([]byte("Benchmark" + strconv.Itoa(i))); err != nil {
				t.Error(err)
			}
		}
	}()

	wg.Wait()
	sub.Close()

	// Open store again
	createTestEventStore("testing", false)
	lastSeq, err := store.GetDurableState(store.name)
	if err != nil {
		t.Error(err)
	}

	if lastSeq != 50 {
		t.Fail()
	}

	// Write 50 records againg
	wg.Add(50)
	for i := 51; i <= 100; i++ {
		if _, err := store.Write([]byte("Benchmark" + strconv.Itoa(i))); err != nil {
			t.Error(err)
		}
	}

	// Subscription to store
	_, err = store.Subscribe(store.name, 50, func(event *Event) {
		//		t.Logf("%d %s", event.Sequence, string(event.Data))
		event.Ack()

		wg.Done()
	})
	if err != nil {
		panic(err)
	}

	wg.Wait()

	// Getting the last sequence number
	lastSeq, err = store.GetDurableState(store.name)
	if err != nil {
		t.Error(err)
	}

	if lastSeq != 100 {
		t.Fail()
	}
}

func TestSubscriptionWithDurableName(t *testing.T) {

	createTestEventStore("testing", false)
	defer closeTestEventStore()

	store := createTestStore()

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
