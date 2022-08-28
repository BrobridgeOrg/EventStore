package eventstore

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSnapshotWrite(t *testing.T) {

	createTestEventStore("testing", true)
	defer closeTestEventStore()

	var wg sync.WaitGroup
	store := createTestStore()

	// Setup snapshot handler
	testEventstore.SetSnapshotHandler(func(request *SnapshotRequest) error {

		_, err := request.Get([]byte("testing"), []byte("testing_key"))
		assert.Equal(t, err, ErrRecordNotFound)

		err = request.Upsert([]byte("testing"), []byte("testing_key"), []byte("value"), func(origin []byte, newValue []byte) []byte {
			return newValue
		})

		if err != nil {
			t.Error(err)
		}

		// Getting record after upsert
		data, err := request.Get([]byte("testing"), []byte("testing_key"))
		if err != nil {
			t.Error(err)
		}

		assert.Equal(t, data, []byte("value"))

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

func TestSnapshotUpdate(t *testing.T) {

	createTestEventStore("testing", true)
	defer closeTestEventStore()

	var wg sync.WaitGroup
	store := createTestStore()

	// Setup snapshot handler
	testEventstore.SetSnapshotHandler(func(request *SnapshotRequest) error {

		err := request.Upsert([]byte("testing"), []byte("testing_key"), request.Data, func(origin []byte, newValue []byte) []byte {
			return Uint64ToBytes(BytesToUint64(origin) | BytesToUint64(newValue))
		})

		if err != nil {
			t.Error(err)
		}

		wg.Done()

		return nil
	})

	// Write to store
	totalCount := 8
	wg.Add(totalCount)
	for i := 0; i < totalCount; i++ {

		//		data := Uint64ToBytes(uint64(i))
		input := make([]byte, 8)
		input[i] = 1

		if _, err := store.Write(input); err != nil {
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
	data, err := view.Get([]byte("testing"), []byte("testing_key"))
	if err != nil {
		panic(err)
	}

	assert.Equal(t, []byte{1, 1, 1, 1, 1, 1, 1, 1}, data)
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

		// Check if record exists
		_, err = request.Get([]byte("testing"), []byte("testing_key"))
		assert.Equal(t, err, ErrRecordNotFound)

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
		primaryKey := request.Data

		err := request.Upsert([]byte("testing"), primaryKey, request.Data, func(origin []byte, newValue []byte) []byte {
			return newValue
		})
		if err != nil {
			t.Error(err)
		}

		wg.Done()

		return nil
	})

	// Write to strore
	totalCount := 200000
	wg.Add(totalCount)
	for i := 0; i < totalCount/20000; i++ {
		go func(base uint64) {
			for i := base; i < base+20000; i++ {
				data := Uint64ToBytes(i + 1)
				if _, err := store.Write(data); err != nil {
					t.Error(err)
				}
			}
		}(uint64(i * 20000))
	}

	wg.Wait()

	// Create a new snapshot view
	view := NewSnapshotView(store)
	defer view.Release()
	err := view.Initialize()
	if err != nil {
		panic(err)
	}

	targetKey := uint64(0)
	for targetKey < snapshotCounter {

		findKey := Uint64ToBytes(targetKey)
		offset := uint64(0)
		if targetKey > 0 {
			offset = 1
		}

		records, err := view.Fetch([]byte("testing"), findKey, offset, 1000)
		if err != nil {
			t.Error(err)
		}

		for _, record := range records {
			targetKey++

			key := Uint64ToBytes(targetKey)

			assert.Equal(t, key, record.Data)
			assert.Equal(t, key, record.Key)

			record.Release()
		}
	}

	assert.Equal(t, uint64(totalCount), snapshotCounter)
	assert.Equal(t, uint64(totalCount), targetKey)
}
