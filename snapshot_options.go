package eventstore

type SnapshotOptions struct {
	WorkerCount int32
	BufferSize  int
}

func NewSnapshotOptions() *SnapshotOptions {
	return &SnapshotOptions{
		WorkerCount: 8,
		BufferSize:  102400,
	}
}
