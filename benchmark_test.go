package eventstore

import (
	"sync"
	"testing"
)

func init() {
	createTestEventStore("bench")
}

func BenchmarkWrite(b *testing.B) {

	store := createTestStore()
	defer store.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := store.Write([]byte("Benchmark")); err != nil {
			panic(err)
		}
	}
}

func BenchmarkThroughput(b *testing.B) {

	store := createTestStore()
	defer store.Close()

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
