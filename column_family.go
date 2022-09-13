package eventstore

import (
	"bytes"
	"errors"
	"io"

	"github.com/cockroachdb/pebble"
)

var (
	ErrInvalidKey = errors.New("EventStore: invalid key")
)

type ListOptions struct {
	Prefix           []byte
	WithoutRawPrefix bool
}

type ColumnFamily struct {
	Store  *Store
	Name   string
	Symbol []byte
	Prefix []byte
}

func NewColumnFamily(store *Store, name string, symbol []byte) *ColumnFamily {
	cf := &ColumnFamily{
		Store:  store,
		Name:   name,
		Symbol: symbol,
	}

	cf.Prefix = append(symbol, []byte(":")...)

	return cf
}

func (cf *ColumnFamily) genRawKey(key []byte) ([]byte, error) {

	if len(key) == 0 {
		return nil, ErrInvalidKey
	}

	return append(cf.Prefix, key...), nil
}

func (cf *ColumnFamily) Get(b *pebble.Batch, key []byte) ([]byte, io.Closer, error) {

	rk, err := cf.genRawKey(key)
	if err != nil {
		return nil, nil, err
	}

	if b != nil {
		return b.Get(rk)
	}

	return cf.Store.db.Get(rk)
}

func (cf *ColumnFamily) Delete(b *pebble.Batch, key []byte) error {

	rk, err := cf.genRawKey(key)
	if err != nil {
		return err
	}

	if b != nil {
		return b.Delete(rk, pebble.NoSync)
	}

	return cf.Store.db.Delete(rk, pebble.NoSync)
}

func (cf *ColumnFamily) Write(b *pebble.Batch, key []byte, data []byte) error {

	rk, err := cf.genRawKey(key)
	if err != nil {
		return err
	}

	// Batch
	if b != nil {
		return b.Set(rk, data, pebble.NoSync)
	}

	return cf.Store.db.Set(rk, data, pebble.NoSync)
}

func (cf *ColumnFamily) list(rawPrefix []byte, targetPrimaryKey []byte, opts *ListOptions) (*Cursor, error) {

	iterOpts := &pebble.IterOptions{}

	rawPrefix = append(cf.Prefix, rawPrefix...)
	prefix := []byte("")
	if opts != nil && len(opts.Prefix) > 0 {

		prefix = append(cf.Prefix, prefix...)

		// Configuring upper bound
		upperBound := make([]byte, len(prefix))
		copy(upperBound, prefix)
		upperBound[len(upperBound)-1] = byte(int(upperBound[len(upperBound)-1]) + 1)

		fullUpperBound := bytes.Join([][]byte{
			rawPrefix,
			upperBound,
		}, []byte(""))

		iterOpts.UpperBound = fullUpperBound
	} else if len(rawPrefix) > 0 {

		// Configuring upper bound
		upperBound := make([]byte, len(rawPrefix))
		copy(upperBound, rawPrefix)
		upperBound[len(upperBound)-1] = byte(int(upperBound[len(upperBound)-1]) + 1)

		fullUpperBound := bytes.Join([][]byte{
			upperBound,
		}, []byte(""))

		iterOpts.UpperBound = fullUpperBound
	}

	targetKey := bytes.Join([][]byte{
		rawPrefix,
		prefix,
		targetPrimaryKey,
	}, []byte(""))

	iter := cf.Store.db.NewIter(iterOpts)

	iter.SeekGE(targetKey)

	cur := &Cursor{
		prefix: rawPrefix,
		isRaw:  !opts.WithoutRawPrefix,
		iter:   iter,
	}

	return cur, nil
}

func (cf *ColumnFamily) List(rawPrefix []byte, targetPrimaryKey []byte, opts *ListOptions) (*Cursor, error) {
	return cf.list(rawPrefix, targetPrimaryKey, opts)
}
