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

	assert.Equal(t, uint64(1), store.GetSnapshotLastSequence())
}

func TestSnapshotUpdate(t *testing.T) {

	createTestEventStore("testing", true)
	defer closeTestEventStore()

	var wg sync.WaitGroup
	store := createTestStore()

	// Setup snapshot handler
	testEventstore.SetSnapshotHandler(func(request *SnapshotRequest) error {

		err := request.Upsert([]byte("testing"), []byte("testing_key"), request.Data, func(origin []byte, newValue []byte) []byte {

			if len(origin) == 0 {
				return newValue
			}

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

func TestSnapshotUpdateReopen(t *testing.T) {

	createTestEventStore("testing", true)
	defer closeTestEventStore()

	var wg sync.WaitGroup
	store := createTestStore()

	// Setup snapshot handler
	testEventstore.SetSnapshotHandler(func(request *SnapshotRequest) error {

		err := request.Upsert([]byte("testing"), []byte("testing_key"), request.Data, func(origin []byte, newValue []byte) []byte {

			if len(origin) == 0 {
				return newValue
			}

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

		input := make([]byte, 8)
		input[i] = 1

		if _, err := store.Write(input); err != nil {
			t.Error(err)
		}
	}

	wg.Wait()

	// Release current store
	storeName := store.name
	store.Close()

	// Re-open store
	store, err := testEventstore.GetStore(storeName)
	if err != nil {
		panic(err)
	}

	// Create a new snapshot view
	view := NewSnapshotView(store)
	defer view.Release()
	err = view.Initialize()
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
	pipelineCount := 10
	quotaPerPipeline := totalCount / pipelineCount
	wg.Add(totalCount)
	for i := 0; i < pipelineCount; i++ {

		go func(base uint64) {
			end := base + uint64(quotaPerPipeline)
			for i := base; i < end; i++ {
				data := Uint64ToBytes(i + 1)
				if _, err := store.Write(data); err != nil {
					t.Error(err)
				}
			}
		}(uint64(quotaPerPipeline * i))
	}

	wg.Wait()

	assert.Equal(t, uint64(totalCount), snapshotCounter)

	// Release current store
	storeName := store.name
	store.Close()

	// Re-open store
	store, err := testEventstore.GetStore(storeName)
	if err != nil {
		panic(err)
	}

	// Create a new snapshot view
	view := NewSnapshotView(store)
	defer view.Release()
	err = view.Initialize()
	if err != nil {
		panic(err)
	}

	targetKey := uint64(0)
	offset := uint64(0)
	for {
		findKey := Uint64ToBytes(targetKey)

		if targetKey > uint64(0) {
			offset = 1
		}

		records, err := view.Fetch([]byte("testing"), findKey, offset, 1000)
		if err != nil {
			t.Error(err)
		}

		if len(records) == 0 {
			break
		}

		for _, record := range records {
			targetKey++

			key := Uint64ToBytes(targetKey)

			assert.Equal(t, key, record.Key)
			assert.Equal(t, key, record.Data)

			record.Release()
		}
	}

	assert.Equal(t, uint64(totalCount), targetKey)
	assert.Equal(t, uint64(totalCount), store.GetSnapshotLastSequence())
}
