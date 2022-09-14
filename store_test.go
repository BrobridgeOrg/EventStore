package eventstore

import (
	"fmt"
	"strconv"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestStoreWrite(t *testing.T) {

	createTestEventStore("testing", false)
	defer closeTestEventStore()

	store := createTestStore()
	value := []byte("test_value")

	seq, err := store.Write(value, 0)
	if err != nil {
		t.Error(err)
	}

	assert.Equal(t, uint64(1), seq)

	v, err := store.Get(seq)
	if err != nil {
		t.Error(err)
	}

	assert.Equal(t, value, v)
	assert.Equal(t, uint64(1), store.State().Count())
	assert.Equal(t, seq, store.State().LastSeq())
}

func TestStoreWriteWithRev(t *testing.T) {

	createTestEventStore("testing", false)
	defer closeTestEventStore()

	store := createTestStore()

	for i := 0; i < 10; i++ {
		if _, err := store.Write([]byte("Benchmark"+strconv.Itoa(i)), uint64(i+1)); err != nil {
			t.Error(err)
		}
	}

	assert.Equal(t, uint64(10), store.State().Count())
	assert.Equal(t, uint64(10), store.State().LastSeq())
	assert.Equal(t, uint64(10), store.State().Rev())

	// duplicated rev should be ignored
	if _, err := store.Write([]byte("Benchmark10"), uint64(10)); err != nil {
		t.Error(err)
	}

	assert.Equal(t, uint64(10), store.State().Count())
	assert.Equal(t, uint64(10), store.State().LastSeq())
	assert.Equal(t, uint64(10), store.State().Rev())
}

func TestStoreWriteUnderPressureWithRev(t *testing.T) {

	createTestEventStore("testing", false)
	defer closeTestEventStore()

	store := createTestStore()

	totalCount := 20000

	for i := 0; i < totalCount; i++ {
		if _, err := store.Write([]byte("Benchmark"+strconv.Itoa(i)), uint64(i+1)); err != nil {
			t.Error(err)
		}
	}

	assert.Equal(t, uint64(totalCount), store.State().Count())
	assert.Equal(t, uint64(totalCount), store.State().LastSeq())
	assert.Equal(t, uint64(totalCount), store.State().Rev())
}

func TestStoreWriteReopen(t *testing.T) {

	createTestEventStore("testing", false)
	defer closeTestEventStore()

	store := createTestStore()
	value := []byte("test_value")

	assert.Equal(t, uint64(0), store.State().LastSeq())

	seq, err := store.Write(value, 0)
	if err != nil {
		t.Error(err)
	}

	assert.Equal(t, uint64(1), seq)
	assert.Equal(t, seq, store.State().LastSeq())
	assert.Equal(t, uint64(1), store.State().Count())

	v, err := store.Get(seq)
	if err != nil {
		t.Error(err)
	}

	assert.Equal(t, value, v)

	// Release current store and re-open store
	storeName := store.name
	store.Close()

	store, err = testEventstore.GetStore(storeName)
	if err != nil {
		panic(err)
	}

	assert.Equal(t, seq, store.State().LastSeq())
	assert.Equal(t, uint64(1), store.State().Count())

	nseq, err := store.Write(value, 0)
	if err != nil {
		t.Error(err)
	}

	assert.Equal(t, nseq, seq+1)
	assert.Equal(t, nseq, store.State().LastSeq())
	assert.Equal(t, uint64(2), store.State().Count())

	v, err = store.Get(nseq)
	if err != nil {
		t.Error(err)
	}

	assert.Equal(t, value, v)
}

func TestStoreDelete(t *testing.T) {

	createTestEventStore("testing", false)
	defer closeTestEventStore()

	store := createTestStore()

	seq, err := store.Write([]byte("Benchmark"), 0)
	if err != nil {
		t.Error(err)
	}
	if err := store.Delete(seq); err != nil {
		t.Error(err)
	}

	assert.Equal(t, seq, store.State().LastSeq())
	assert.Equal(t, uint64(0), store.State().Count())
}

func TestStoreFetch(t *testing.T) {

	createTestEventStore("testing", false)
	defer closeTestEventStore()

	store := createTestStore()
	totalCount := 5000

	for i := 0; i < totalCount; i++ {
		if _, err := store.Write([]byte(fmt.Sprintf("%d", i+1)), 0); err != nil {
			t.Error(err)
		}
	}

	assert.Equal(t, uint64(totalCount), store.State().Count())
	assert.Equal(t, uint64(totalCount), store.State().LastSeq())

	var lastSeq uint64 = 0
	events, err := store.Fetch(0, 0, totalCount)
	if err != nil {
		panic(err)
	}

	for _, event := range events {
		lastSeq++
		assert.Equal(t, lastSeq, event.Sequence)
		assert.Equal(t, fmt.Sprintf("%d", lastSeq), string(event.Data))

		event.Release()
	}
}

func TestStoreRealtimeFetch(t *testing.T) {

	createTestEventStore("testing", false)
	defer closeTestEventStore()

	store := createTestStore()

	totalCount := 10000

	go func() {
		for i := 0; i < totalCount; i++ {
			if _, err := store.Write([]byte(fmt.Sprintf("%d", i+1)), 0); err != nil {
				t.Error(err)
			}
		}
	}()

	var lastSeq uint64 = 0
	var offset uint64 = 0
	for i := uint64(0); i < uint64(totalCount); {

		if i != 0 {
			offset = 1
		}

		events, err := store.Fetch(i, offset, 100)
		if err != nil {
			panic(err)
		}

		for _, event := range events {
			lastSeq++
			assert.Equal(t, lastSeq, event.Sequence)
			assert.Equal(t, fmt.Sprintf("%d", lastSeq), string(event.Data))

			event.Release()
		}

		i += uint64(len(events))
	}

	assert.Equal(t, uint64(totalCount), store.State().Count())
}

func TestStoreFetchWithCount(t *testing.T) {

	createTestEventStore("testing", false)
	defer closeTestEventStore()

	store := createTestStore()

	for i := 0; i < 10; i++ {
		if _, err := store.Write([]byte("Benchmark"+strconv.Itoa(i)), 0); err != nil {
			t.Error(err)
		}
	}

	assert.Equal(t, uint64(10), store.State().Count())
	assert.Equal(t, uint64(10), store.State().LastSeq())

	events, err := store.Fetch(0, 1, 2)
	if err != nil {
		panic(err)
	}

	if len(events) != 2 {
		t.Fail()
	}
}

func TestStoreFetchWithOffset(t *testing.T) {

	createTestEventStore("testing", false)
	defer closeTestEventStore()

	store := createTestStore()

	for i := 0; i < 10; i++ {
		if _, err := store.Write([]byte("Benchmark"+strconv.Itoa(i)), 0); err != nil {
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

		event.Release()
	}
}

func TestStoreSubscription(t *testing.T) {

	createTestEventStore("testing", false)
	defer closeTestEventStore()

	store := createTestStore()

	var wg sync.WaitGroup

	// Subscription to store
	_, err := store.Subscribe(func(event *Event) {
		//t.Logf("%d %s", event.Sequence, string(event.Data))
		event.Ack()
		wg.Done()
	}, DurableName(store.name))
	if err != nil {
		panic(err)
	}

	wg.Add(1000)
	go func() {
		for i := 0; i < 1000; i++ {
			if _, err := store.Write([]byte("Benchmark"+strconv.Itoa(i)), 0); err != nil {
				t.Error(err)
			}
		}
	}()

	wg.Wait()
}

func TestStoreSubscriptionOffset(t *testing.T) {

	createTestEventStore("testing", false)
	defer closeTestEventStore()

	store := createTestStore()

	var wg sync.WaitGroup

	// Subscription to store
	_, err := store.Subscribe(func(event *Event) {
		//		t.Logf("%d %s", seq, string(data))
		event.Ack()

		wg.Done()
	}, DurableName(store.name))
	if err != nil {
		panic(err)
	}

	wg.Add(50)
	go func() {
		for i := 1; i <= 50; i++ {
			if _, err := store.Write([]byte(fmt.Sprintf("%d", i)), 0); err != nil {
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
		if _, err := store.Write([]byte(fmt.Sprintf("%d", i)), 0); err != nil {
			t.Error(err)
		}
	}

	// Subscription to store
	_, err = store.Subscribe(func(event *Event) {
		//t.Logf("%d %s", event.Sequence, string(event.Data))
		event.Ack()

		wg.Done()
	}, DurableName(store.name), StartAtSequence(50))
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

func TestStoreSubscriptionWithDurableName(t *testing.T) {

	createTestEventStore("testing", false)
	defer closeTestEventStore()

	store := createTestStore()

	var wg sync.WaitGroup
	var lastSeq uint64 = 0
	msgCount := 100

	// Subscribe to store
	sub, err := store.Subscribe(func(event *Event) {

		lastSeq++
		assert.Equal(t, lastSeq, event.Sequence)
		assert.Equal(t, fmt.Sprintf("%d", lastSeq), string(event.Data))

		event.Ack()
		wg.Done()
	}, DurableName(store.name))
	if err != nil {
		panic(err)
	}

	wg.Add(msgCount - 30)
	go func() {
		for i := 0; i < msgCount-30; i++ {
			if _, err := store.Write([]byte(fmt.Sprintf("%d", i+1)), 0); err != nil {
				t.Error(err)
			}
		}
	}()

	wg.Wait()

	assert.Equal(t, lastSeq, uint64(msgCount-30))
	assert.Equal(t, lastSeq, store.State().LastSeq())
	assert.Equal(t, lastSeq, store.State().Count())

	// Close current subscription
	sub.Close()

	durableSeq, err := store.GetDurableState(store.name)
	if err != nil {
		t.Error(err)
	}

	assert.Equal(t, lastSeq, durableSeq)
	assert.Equal(t, durableSeq, store.State().LastSeq())

	// Write more messages
	wg.Add(30)
	for i := 0; i < 30; i++ {
		if _, err := store.Write([]byte(fmt.Sprintf("%d", i+1)), 0); err != nil {
			t.Error(err)
		}
	}

	// Subscribe to store
	_, err = store.Subscribe(func(event *Event) {

		lastSeq++
		assert.Equal(t, lastSeq, event.Sequence)
		assert.Equal(t, fmt.Sprintf("%d", lastSeq-uint64(msgCount-30)), string(event.Data))

		event.Ack()
		wg.Done()
	}, DurableName(store.name))
	if err != nil {
		t.Error(err)
	}

	wg.Wait()

	assert.Equal(t, lastSeq, uint64(msgCount))
}
