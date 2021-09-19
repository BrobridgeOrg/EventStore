package eventstore

import (
	"bytes"
	"strconv"
	"sync"
	"testing"
)

func TestSnapshotWrite(t *testing.T) {

	createTestEventStore("testing", true)
	defer closeTestEventStore()

	var wg sync.WaitGroup
	store := createTestStore()

	// Setup snapshot handler
	testEventstore.SetSnapshotHandler(func(request *SnapshotRequest) error {

		err := request.Upsert([]byte("testing"), []byte("testing_key"), []byte("value"), func(origin []byte, newValue []byte) []byte {

			return newValue
		})

		if err != nil {
			t.Error(err)
		}

		data, err := request.Get([]byte("testing"), []byte("testing_key"))
		if err != nil {
			t.Error(err)
		}

		if bytes.Compare(data, []byte("value")) != 0 {
			t.Fail()
		}

		wg.Done()

		return nil
	})

	// Write data to store
	wg.Add(1)
	if _, err := store.Write([]byte("Snapshot Testing")); err != nil {
		t.Error(err)
	}

	wg.Wait()
}

func TestSnapshotDelete(t *testing.T) {

	createTestEventStore("testing", true)
	defer closeTestEventStore()

	var wg sync.WaitGroup
	store := createTestStore()

	// Setup snapshot handler
	testEventstore.SetSnapshotHandler(func(request *SnapshotRequest) error {

		err := request.Upsert([]byte("testing"), []byte("testing_key"), []byte("value"), func(origin []byte, newValue []byte) []byte {

			// Replace old data directly
			return newValue
		})

		if err != nil {
			t.Error(err)
		}

		// Delete snapshot by key
		err = request.Delete([]byte("testing"), []byte("testing_key"))
		if err != nil {
			t.Error(err)
		}

		wg.Done()

		return nil
	})

	// Write data to store
	wg.Add(1)
	if _, err := store.Write([]byte("Snapshot Testing")); err != nil {
		t.Error(err)
	}

	wg.Wait()
}

func TestSnapshotViewFetch(t *testing.T) {

	createTestEventStore("testing", true)
	defer closeTestEventStore()

	var wg sync.WaitGroup
	store := createTestStore()

	// Setup snapshot handler
	snapshotCounter := uint64(0)
	testEventstore.SetSnapshotHandler(func(request *SnapshotRequest) error {

		snapshotCounter++
		key := Uint64ToBytes(snapshotCounter)

		err := request.Upsert([]byte("testing"), key, []byte("value"), func(origin []byte, newValue []byte) []byte {

			return newValue
		})
		if err != nil {
			t.Error(err)
		}

		wg.Done()

		return nil
	})

	for i := 0; i < 50000; i++ {
		if _, err := store.Write([]byte("original value " + strconv.Itoa(i))); err != nil {
			t.Error(err)
		}

		wg.Add(1)
	}

	wg.Wait()

	// Create a new snapshot view
	view := NewSnapshotView(store)
	defer view.Release()
	err := view.Initialize()
	if err != nil {
		panic(err)
	}

	for targetKey := uint64(0); targetKey < snapshotCounter; {

		findKey := Uint64ToBytes(targetKey)
		offset := uint64(0)
		if targetKey > 0 {
			offset = 1
		}

		records, err := view.Fetch([]byte("testing"), findKey, offset, 1000)
		if err != nil {
			panic(err)
		}

		for _, record := range records {
			targetKey++

			key := Uint64ToBytes(targetKey)

			if bytes.Compare(record.Data, []byte("value")) != 0 {
				t.Fail()
			}

			if bytes.Compare(key, record.Key) != 0 {
				t.Fail()
			}

			record.Release()
		}
	}
}
