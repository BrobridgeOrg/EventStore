package eventstore

import "sync"

var recordPool = sync.Pool{
	New: func() interface{} {
		return &Record{}
	},
}

type Record struct {
	Key  []byte
	Data []byte
}

func NewRecord() *Record {
	return recordPool.Get().(*Record)
}

func (r *Record) Release() {
	recordPool.Put(r)
}
