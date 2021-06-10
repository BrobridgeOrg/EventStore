package eventstore

import "io"

type Merger struct {
	newValue     []byte
	oldValue     []byte
	mergeHandler func(origin []byte, newValue []byte) []byte
}

func (m *Merger) MergeNewer(value []byte) error {
	m.newValue = value
	return nil
}

func (m *Merger) SetHandler(fn func(origin []byte, newValue []byte) []byte) {
	m.mergeHandler = fn
}

func (m *Merger) MergeOlder(value []byte) error {
	m.oldValue = value
	return nil
}

func (m *Merger) Finish(includesBase bool) ([]byte, io.Closer, error) {
	return m.mergeHandler(m.oldValue, m.newValue), nil, nil
}
