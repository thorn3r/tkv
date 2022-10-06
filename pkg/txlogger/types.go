package txlogger

type TxLogger interface {
	WritePut(key, value string)
	WriteDelete(key string)
	Err() <-chan error

	ReadEvents() (<-chan Event, <-chan error)
	Run()
}

type EventType byte

const (
	_                     = iota
	EventDelete EventType = iota
	EventPut
)

type Event struct {
	Sequence  uint64
	EventType EventType
	Key       string
	Value     string
}
