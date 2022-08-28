package eventstore

import (
	"io"
)

type Merger struct {
	key          []byte
	newValue     []byte
	oldValue     []byte
	mergeHandler func(origin []byte, newValue []byte) []byte
	done         func([]byte)
}

func (m *Merger) MergeNewer(value []byte) error {
	m.newValue = value
	return nil
}

func (m *Merger) SetHandler(fn func(origin []byte, newValue []byte) []byte) {
	m.mergeHandler = fn
}

func (m *Merger) MergeOlder(value []byte) error {

	if m.oldValue == nil {
		m.oldValue = value
		return nil
	}

	m.oldValue = m.mergeHandler(value, m.oldValue)

	return nil
}

func (m *Merger) Finish(includesBase bool) ([]byte, io.Closer, error) {

	if m.done != nil {
		m.done(m.key)
	}

	if m.mergeHandler == nil {
		return m.newValue, nil, nil
	}

	return m.mergeHandler(m.oldValue, m.newValue), nil, nil
}
