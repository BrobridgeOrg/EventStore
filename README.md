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
BenchmarkWrite-16                  	  348991	      2885 ns/op
BenchmarkEventThroughput-16        	  177333	      6030 ns/op
BenchmarkSnapshotPerformance-16    	   82579	     15176 ns/op
PASS
ok  	github.com/BrobridgeOrg/EventStore	31.418s
```

## License
Licensed under the MIT License

## Authors
Copyright(c) 2020 Fred Chien <fred@brobridge.com>
