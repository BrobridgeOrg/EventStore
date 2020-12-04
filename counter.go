package eventstore

import (
	"fmt"
	"sync/atomic"
)

type Counter uint64

func (counter *Counter) SetCount(val uint64) {
	atomic.StoreUint64((*uint64)(counter), val)
}

func (counter *Counter) Count() uint64 {
	return atomic.LoadUint64((*uint64)(counter))
}

func (counter *Counter) Increase(delta uint64) uint64 {
	return atomic.AddUint64((*uint64)(counter), delta)
}

func (counter *Counter) String() string {
	return fmt.Sprint(counter.Count())
}
