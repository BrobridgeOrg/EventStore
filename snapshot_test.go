package eventstore

import (
	"bytes"
	"encoding/binary"
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
	snapshotCounter := 0
	testEventstore.SetSnapshotHandler(func(request *SnapshotRequest) error {

		snapshotCounter++
		key := make([]byte, 4)
		binary.BigEndian.PutUint32(key, uint32(snapshotCounter))

		err := request.Upsert([]byte("testing"), key, []byte("value"), func(origin []byte, newValue []byte) []byte {

			return newValue
		})
		if err != nil {
			t.Error(err)
		}

		wg.Done()

		return nil
	})

	wg.Add(1000)
	for i := 0; i < 1000; i++ {
		if _, err := store.Write([]byte("original value " + strconv.Itoa(i))); err != nil {
			t.Error(err)
		}
	}
	wg.Wait()

	// Create a new snapshot view
	view := NewSnapshotView(store)
	defer view.Release()
	err := view.Initialize()
	if err != nil {
		panic(err)
	}

	records, err := view.Fetch([]byte("testing"), []byte(""), 0, 1000)
	if err != nil {
		panic(err)
	}

	targetKey := 0
	for _, record := range records {
		targetKey++

		key := make([]byte, 4)
		binary.BigEndian.PutUint32(key, uint32(targetKey))

		if bytes.Compare(record.Data, []byte("value")) != 0 {
			t.Fail()
		}

		if bytes.Compare(key, record.Key) != 0 {
			t.Fail()
		}
	}
}
