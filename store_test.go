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

func TestFetch(t *testing.T) {

	createTestEventStore("testing", false)
	defer closeTestEventStore()

	store := createTestStore()

	for i := 0; i < 5000; i++ {
		if _, err := store.Write([]byte("Benchmark" + strconv.Itoa(i))); err != nil {
			t.Error(err)
		}
	}

	var lastSeq uint64 = 0
	events, err := store.Fetch(0, 0, 5000)
	if err != nil {
		panic(err)
	}

	for _, event := range events {
		lastSeq++
		if lastSeq != event.Sequence {
			t.Error(lastSeq, event.Sequence)
			t.Fail()
		}
	}
}

func TestRealtimeFetch(t *testing.T) {

	createTestEventStore("testing", false)
	defer closeTestEventStore()

	store := createTestStore()

	go func() {
		for i := 0; i < 10000; i++ {
			if _, err := store.Write([]byte("Benchmark" + strconv.Itoa(i))); err != nil {
				t.Error(err)
			}
		}
	}()

	var lastSeq uint64 = 0
	var offset uint64 = 0
	for i := uint64(0); i < 10000; {

		if i != 0 {
			offset = 1
		}

		events, err := store.Fetch(i, offset, 100)
		if err != nil {
			panic(err)
		}

		for _, event := range events {
			lastSeq++
			if lastSeq != event.Sequence {
				t.Fail()
			}
		}

		i += uint64(len(events))
	}
}

func TestFetchWithCount(t *testing.T) {

	createTestEventStore("testing", false)
	defer closeTestEventStore()

	store := createTestStore()

	for i := 0; i < 10; i++ {
		if _, err := store.Write([]byte("Benchmark" + strconv.Itoa(i))); err != nil {
			t.Error(err)
		}
	}

	events, err := store.Fetch(0, 1, 2)
	if err != nil {
		panic(err)
	}

	if len(events) != 2 {
		t.Fail()
	}
}

func TestFetchWithOffset(t *testing.T) {

	createTestEventStore("testing", false)
	defer closeTestEventStore()

	store := createTestStore()

	for i := 0; i < 10; i++ {
		if _, err := store.Write([]byte("Benchmark" + strconv.Itoa(i))); err != nil {
			t.Error(err)
		}
	}

	events, err := store.Fetch(0, 1, 10)
	if err != nil {
		panic(err)
	}

	var lastSeq uint64 = 1
	for _, event := range events {
		lastSeq++
		if lastSeq != event.Sequence {
			t.Fail()
		}
	}
}

func TestSubscription(t *testing.T) {

	createTestEventStore("testing", false)
	defer closeTestEventStore()

	store := createTestStore()

	var wg sync.WaitGroup

	// Subscription to store
	_, err := store.Subscribe(store.name, 0, func(event *Event) {
		//		t.Logf("%d %s", event.Sequence, string(event.Data))
		event.Ack()
		wg.Done()
	})
	if err != nil {
		panic(err)
	}

	wg.Add(1000)
	go func() {
		for i := 0; i < 1000; i++ {
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
	_, err := store.Subscribe(store.name, 0, func(event *Event) {
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

	// Release current store
	storeName := store.name
	store.Close()

	// Re-open store
	store, err = testEventstore.GetStore(storeName)
	if err != nil {
		panic(err)
	}

	// Open store again
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
		//t.Logf("%d %s", event.Sequence, string(event.Data))
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

	wg.Add(100)
	go func() {
		for i := 0; i < 100; i++ {
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

	wg.Add(100)
	go func() {
		for i := 0; i < 100; i++ {
			if _, err := store.Write([]byte("Benchmark" + strconv.Itoa(i))); err != nil {
				t.Error(err)
			}
		}
	}()

	wg.Wait()
}
