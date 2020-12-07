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
	_, err := store.Subscribe(store.name, 0, func(event *Event) {
		event.Ack()
		wg.Done()
	})
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

func BenchmarkSnapshotThroughput(b *testing.B) {

	createTestEventStore("bench", true)
	defer closeTestEventStore()

	store := createTestStore()

	var wg sync.WaitGroup

	// Setup snapshot handler
	testEventstore.SetSnapshotHandler(func(request *SnapshotRequest) error {

		err := request.Upsert([]byte("testing"), []byte("testing_key"), func(origin []byte) ([]byte, error) {

			// Replace old data directly
			return origin, nil
		})

		if err != nil {
			b.Error(err)
		}

		wg.Done()

		return nil
	})

	wg.Add(b.N)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := store.Write([]byte("Benchmark")); err != nil {
			panic(err)
		}
	}

	wg.Wait()
}
