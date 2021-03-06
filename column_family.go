package eventstore

import (
	"path/filepath"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/pebble"
)

type ColumnFamily struct {
	Store *Store
	Db    *pebble.DB
	Name  string
	Merge func([]byte, []byte) []byte

	closed chan struct{}

	isScheduled uint32
	timer       *time.Timer
}

func NewColumnFamily(store *Store, name string) *ColumnFamily {
	cf := &ColumnFamily{
		Store: store,
		Name:  name,
		Merge: func(oldValue []byte, newValue []byte) []byte {
			return newValue
		},
		closed:      make(chan struct{}),
		isScheduled: 0,
		timer:       time.NewTimer(time.Second * 10),
	}

	cf.timer.Stop()

	return cf
}

func (cf *ColumnFamily) sync() {

	cf.timer.Reset(time.Second * 10)

	for {

		select {
		case <-cf.timer.C:
			cf.Db.LogData(nil, pebble.Sync)

			cf.timer.Stop()
			cf.timer.Reset(time.Second * 10)

			atomic.StoreUint32(&cf.isScheduled, 0)
		case <-cf.closed:
			cf.timer.Stop()
			close(cf.closed)
			return
		}
	}
}

func (cf *ColumnFamily) requestSync() {

	if atomic.LoadUint32(&cf.isScheduled) != 0 {
		return
	}

	atomic.StoreUint32(&cf.isScheduled, 1)

	cf.timer.Stop()
	cf.timer.Reset(time.Millisecond * 100)
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
		//		DisableWAL:    true,
		MaxOpenFiles:  -1,
		LBaseMaxBytes: 512 << 20,
	}

	// Initialize 4 levels
	opts.Levels = make([]pebble.LevelOptions, 4)
	for i := range opts.Levels {
		l := &opts.Levels[i]

		// Level 0
		l.Compression = pebble.SnappyCompression
		l.BlockSize = 32 << 10      // 32MB
		l.TargetFileSize = 64 << 20 // 64MB

		if i > 0 {
			l.Compression = pebble.SnappyCompression
			l.TargetFileSize = opts.Levels[i-1].TargetFileSize * 2
		}

		opts.Levels[i].EnsureDefaults()
	}

	opts.EnsureDefaults()

	dbPath := filepath.Join(cf.Store.dbPath, cf.Name)
	db, err := pebble.Open(dbPath, opts)
	if err != nil {
		return err
	}

	cf.Db = db

	go cf.sync()

	return nil
}

func (cf *ColumnFamily) Close() error {
	cf.closed <- struct{}{}
	cf.Db.LogData(nil, pebble.Sync)
	return cf.Db.Close()
}

func (cf *ColumnFamily) Write(key []byte, data []byte) error {

	err := cf.Db.Set(key, data, pebble.NoSync)
	if err != nil {
		return err
	}

	cf.requestSync()

	return nil
}
