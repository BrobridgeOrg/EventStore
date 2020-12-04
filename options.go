package eventstore

import "github.com/tecbot/gorocksdb"

type Options struct {
	DatabasePath   string
	RocksdbOptions *gorocksdb.Options
}

func NewOptions() *Options {

	// Well, I am not really sure what i am writing right here. hope it won't get any troubles. :-S
	options := gorocksdb.NewDefaultOptions()
	options.SetCreateIfMissing(true)
	options.SetEnablePipelinedWrite(true)
	options.SetAllowConcurrentMemtableWrites(true)
	options.SetOptimizeFiltersForHits(true)
	options.SetNumLevels(4)

	blockBasedTableOptions := gorocksdb.NewDefaultBlockBasedTableOptions()
	blockBasedTableOptions.SetBlockSizeDeviation(5)
	blockBasedTableOptions.SetBlockSize(32 * 1024)
	blockBasedTableOptions.SetCacheIndexAndFilterBlocks(true)
	blockBasedTableOptions.SetCacheIndexAndFilterBlocksWithHighPriority(true)
	blockBasedTableOptions.SetPinL0FilterAndIndexBlocksInCache(true)
	//	blockBasedTableOptions.SetIndexType(gorocksdb.KHashSearchIndexType)
	options.SetBlockBasedTableFactory(blockBasedTableOptions)

	env := gorocksdb.NewDefaultEnv()
	env.SetBackgroundThreads(4)
	options.SetMaxBackgroundCompactions(4)
	options.SetEnv(env)

	return &Options{
		RocksdbOptions: options,
	}
}
