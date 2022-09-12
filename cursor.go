package eventstore

import (
	"github.com/cockroachdb/pebble"
)

type Cursor struct {
	prefix []byte
	isRaw  bool
	iter   *pebble.Iterator
}

func (cur *Cursor) EOF() bool {
	return !cur.iter.Valid()
}

func (cur *Cursor) Next() bool {
	return cur.iter.Next()
}

func (cur *Cursor) Close() error {
	return cur.iter.Close()
}

func (cur *Cursor) GetKey() []byte {
	if !cur.isRaw {
		return cur.iter.Key()[len(cur.prefix):]
	}

	return cur.iter.Key()
}

func (cur *Cursor) GetData() []byte {
	return cur.iter.Value()
}
