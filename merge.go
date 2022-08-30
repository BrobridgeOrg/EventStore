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

func NewMerger() *Merger {
	return &Merger{
		mergeHandler: func(origin []byte, newValue []byte) []byte {
			return newValue
		},
	}
}

func (m *Merger) MergeNewer(value []byte) error {
	m.newValue = value
	return nil
}

func (m *Merger) MergeOlder(value []byte) error {

	if len(m.oldValue) == 0 {
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

	return m.mergeHandler(m.oldValue, m.newValue), nil, nil
}
