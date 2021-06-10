package eventstore

import (
	"path/filepath"

	"github.com/cockroachdb/pebble"
)

type ColumnFamily struct {
	Store *Store
	Db    *pebble.DB
	Name  string
	Merge func([]byte, []byte) []byte
}

func NewColumnFamily(store *Store, name string) *ColumnFamily {
	return &ColumnFamily{
		Store: store,
		Name:  name,
		Merge: func(oldValue []byte, newValue []byte) []byte {
			return newValue
		},
	}
}

func (cf *ColumnFamily) Open() error {

	opts := &pebble.Options{
		Merger: &pebble.Merger{
			Merge: func(key []byte, value []byte) (pebble.ValueMerger, error) {
				m := &Merger{}
				m.SetHandler(cf.Merge)
				return m, m.MergeNewer(value)
			},
		},
	}

	dbPath := filepath.Join(cf.Store.dbPath, cf.Name)
	db, err := pebble.Open(dbPath, opts)
	if err != nil {
		return err
	}

	cf.Db = db

	return nil
}

func (cf *ColumnFamily) Close() error {
	return cf.Db.Close()
}
