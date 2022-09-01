package eventstore

import (
	"fmt"
	"io"
)

type Merger struct {
	newValue     []byte
	oldValue     []byte
	mergeHandler func(origin []byte, newValue []byte) []byte
}

func NewMerger() *Merger {
	return &Merger{
		mergeHandler: func(origin []byte, newValue []byte) []byte {
			return newValue
		},
	}
}

func (m *Merger) MergeNewer(value []byte) error {
	fmt.Println("MergeNewer", m.newValue, value)
	m.newValue = value
	return nil
}

func (m *Merger) MergeOlder(value []byte) error {

	fmt.Println("MergeOlder", value, m.oldValue)

	if len(m.oldValue) == 0 {
		m.oldValue = value
		return nil
	}

	m.oldValue = m.mergeHandler(value, m.oldValue)

	fmt.Println("============================================================> MergeOlder", m.oldValue)

	return nil
}

func (m *Merger) Finish(includesBase bool) ([]byte, io.Closer, error) {

	fmt.Println("Finish", m.oldValue, m.newValue)

	return m.mergeHandler(m.oldValue, m.newValue), nil, nil
}
