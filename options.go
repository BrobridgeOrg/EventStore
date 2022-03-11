package eventstore

type Options struct {
	DatabasePath     string
	BypassEventStore bool
	EnabledSnapshot  bool
	SnapshotOptions  *SnapshotOptions
}

func NewOptions() *Options {

	return &Options{
		BypassEventStore: false,
		EnabledSnapshot:  false,
		SnapshotOptions:  NewSnapshotOptions(),
	}
}
