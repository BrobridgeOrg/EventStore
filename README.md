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
BenchmarkWrite
BenchmarkWrite-16                  	  208004	      5060 ns/op
BenchmarkEventThroughput
BenchmarkEventThroughput-16        	  119781	      9529 ns/op
BenchmarkSnapshotPerformance
BenchmarkSnapshotPerformance-16    	   62036	     17749 ns/op
PASS
ok  	github.com/BrobridgeOrg/EventStore	20.779s
```

## License
Licensed under the MIT License

## Authors
Copyright(c) 2020 Fred Chien <fred@brobridge.com>
