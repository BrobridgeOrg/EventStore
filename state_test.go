package eventstore

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestStateSetStateBytes(t *testing.T) {

	createTestEventStore("testing", false)
	defer closeTestEventStore()

	store := createTestStore()
	class := []byte("mystate")
	group := []byte("mygroup")
	key := []byte("mykey")
	val := []byte("test_value")

	if err := store.SetStateBytes(class, group, key, val); err != nil {
		t.Error(err)
		return
	}

	value, err := store.GetStateBytes(class, group, key)
	if err != nil {
		t.Error(err)
		return
	}

	assert.Equal(t, val, value)
}

func TestStateSetStateInt64(t *testing.T) {

	createTestEventStore("testing", false)
	defer closeTestEventStore()

	store := createTestStore()
	class := []byte("mystate")
	group := []byte("mygroup")
	key := []byte("mykey")
	val := int64(9999999)

	if err := store.SetStateInt64(class, group, key, val); err != nil {
		t.Error(err)
		return
	}

	value, err := store.GetStateInt64(class, group, key)
	if err != nil {
		t.Error(err)
		return
	}

	assert.Equal(t, val, value)
}

func TestStateSetStateUint64(t *testing.T) {

	createTestEventStore("testing", false)
	defer closeTestEventStore()

	store := createTestStore()
	class := []byte("mystate")
	group := []byte("mygroup")
	key := []byte("mykey")
	val := uint64(9999999)

	if err := store.SetStateUint64(class, group, key, val); err != nil {
		t.Error(err)
		return
	}

	value, err := store.GetStateUint64(class, group, key)
	if err != nil {
		t.Error(err)
		return
	}

	assert.Equal(t, val, value)
}

func TestStateSetStateFloat64(t *testing.T) {

	createTestEventStore("testing", false)
	defer closeTestEventStore()

	store := createTestStore()
	class := []byte("mystate")
	group := []byte("mygroup")
	key := []byte("mykey")
	val := float64(9999.999)

	if err := store.SetStateFloat64(class, group, key, val); err != nil {
		t.Error(err)
	}

	value, err := store.GetStateFloat64(class, group, key)
	if err != nil {
		t.Error(err)
		return
	}

	assert.Equal(t, val, value)
}

func TestStateSetStateString(t *testing.T) {

	createTestEventStore("testing", false)
	defer closeTestEventStore()

	store := createTestStore()
	class := []byte("mystate")
	group := []byte("mygroup")
	key := []byte("mykey")
	val := "test_value"

	if err := store.SetStateString(class, group, key, val); err != nil {
		t.Error(err)
		return
	}

	value, err := store.GetStateString(class, group, key)
	if err != nil {
		t.Error(err)
		return
	}

	assert.Equal(t, val, value)
}

func TestStateDeleteState(t *testing.T) {

	createTestEventStore("testing", false)
	defer closeTestEventStore()

	store := createTestStore()
	class := []byte("mystate")
	group := []byte("mygroup")
	key := []byte("mykey")
	val := []byte("test_value")

	if err := store.SetStateBytes(class, group, key, val); err != nil {
		t.Error(err)
	}

	value, err := store.GetStateBytes(class, group, key)
	if err != nil {
		t.Error(err)
		return
	}

	assert.Equal(t, val, value)

	err = store.DeleteState(class, group, key)
	if err != nil {
		t.Error(err)
		return
	}

	_, err = store.GetStateBytes(class, group, key)
	assert.Equal(t, ErrStateEntryNotFound, err)
}

func TestStateListStates(t *testing.T) {

	createTestEventStore("testing", false)
	defer closeTestEventStore()

	store := createTestStore()
	class := []byte("mystate")
	group := []byte("mygroup")

	for i := 1; i <= 10; i++ {
		key := Int64ToBytes(int64(i))

		err := store.SetStateInt64(class, group, key, int64(i))
		if err != nil {
			t.Error(err)
			return
		}
	}

	var counter int64 = 0
	targetKey := Int64ToBytes(int64(1))
	cur, err := store.ListStates(class, group, targetKey)
	if err != nil {
		t.Error(err)
		return
	}

	for !cur.EOF() {

		counter++

		value := cur.GetData()

		key := append(bytes.Join([][]byte{
			class,
			group,
			Int64ToBytes(int64(counter)),
		}, []byte(".")))

		assert.Equal(t, key, cur.GetKey())
		assert.Equal(t, counter, BytesToInt64(value))

		cur.Next()
	}

	assert.Equal(t, int64(10), counter)
}
