package eventstore

import (
	"sync"
	"testing"
)

func BenchmarkWrite(b *testing.B) {

	createTestEventStore("bench", false)
	defer closeTestEventStore()

	store := createTestStore()

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if _, err := store.Write([]byte("Benchmark")); err != nil {
			panic(err)
		}
	}
}

func BenchmarkEventThroughput(b *testing.B) {

	createTestEventStore("bench", false)
	defer closeTestEventStore()

	store := createTestStore()

	var wg sync.WaitGroup

	// Subscription to store
	_, err := store.Subscribe(func(event *Event) {
		event.Ack()
		wg.Done()
	}, DurableName(store.name))
	if err != nil {
		panic(err)
	}

	wg.Add(b.N)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := store.Write([]byte("Benchmark")); err != nil {
			panic(err)
		}
	}

	wg.Wait()
}

func BenchmarkSnapshotPerformance(b *testing.B) {

	createTestEventStore("bench", true)
	defer closeTestEventStore()

	store := createTestStore()

	var wg sync.WaitGroup

	// Setup snapshot handler
	testEventstore.SetSnapshotHandler(func(request *SnapshotRequest) error {

		err := request.Upsert([]byte("testing"), []byte("testing_key"), []byte("value"), func(origin []byte, newValue []byte) []byte {

			// Replace old data directly
			return newValue
		})

		if err != nil {
			b.Error(err)
		}

		wg.Done()

		return nil
	})

	wg.Add(b.N)
	b.ResetTimer()

	go func() {
		for i := 0; i < b.N; i++ {
			if _, err := store.Write([]byte("Benchmark")); err != nil {
				panic(err)
			}
		}
	}()

	wg.Wait()
}
