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
BenchmarkWrite
BenchmarkWrite-16                  	  514297	      2336 ns/op
BenchmarkEventThroughput
BenchmarkEventThroughput-16        	  223045	      5077 ns/op
BenchmarkSnapshotPerformance
BenchmarkSnapshotPerformance-16    	  108236	     11125 ns/op
PASS
ok  	github.com/BrobridgeOrg/EventStore	15.152s
```

## License
Licensed under the MIT License

## Authors
Copyright(c) 2020 Fred Chien <fred@brobridge.com>
