# EventStore

[![GoDoc](https://godoc.org/github.com/BrobridgeOrg/EventSource?status.svg)](http://godoc.org/github.com/BrobridgeOrg/EventSource)

High performance event store which is used by Brobridge Gravity.

## Benchmark

Here is benchmark results:

```shell
$ go test -v -bench Benchmark -run Benchmark
goos: darwin
goarch: amd64
pkg: github.com/BrobridgeOrg/EventStore
cpu: Intel(R) Core(TM) i9-9880H CPU @ 2.30GHz
BenchmarkWrite-16                  	  382582	      3211 ns/op
BenchmarkEventThroughput-16        	  164775	      7871 ns/op
BenchmarkSnapshotPerformance-16    	   57495	     18151 ns/op
PASS
ok  	github.com/BrobridgeOrg/EventStore	36.917s
```

## License
Licensed under the MIT License

## Authors
Copyright(c) 2020 Fred Chien <fred@brobridge.com>
