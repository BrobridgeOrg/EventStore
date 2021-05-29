package eventstore

type Record struct {
	Key  []byte
	Data []byte
}

func NewRecord() *Record {
	return &Record{}
}
