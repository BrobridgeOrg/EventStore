package eventstore

type Options struct {
	DatabasePath    string
	EnabledSnapshot bool
	SnapshotOptions *SnapshotOptions
}

func NewOptions() *Options {

	return &Options{
		EnabledSnapshot: false,
		SnapshotOptions: NewSnapshotOptions(),
	}
}
