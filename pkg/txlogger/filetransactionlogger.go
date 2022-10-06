package txlogger

import (
	"bufio"
	"fmt"
	"os"

	klog "k8s.io/klog/v2"
)

type FileTxLogger struct {
	lastSequence uint64
	file         *os.File
	events       chan<- Event
	errors       <-chan error
}

func NewFileTxLogger(filename string) (TxLogger, error) {
	file, err := os.OpenFile(filename, os.O_RDWR|os.O_APPEND|os.O_CREATE, 0755)
	if err != nil {
		return nil, fmt.Errorf("cannot open transaction log file: %v", err)
	}

	return &FileTxLogger{file: file}, nil
}

func (l *FileTxLogger) WritePut(key, value string) {
	l.events <- Event{EventType: EventPut, Key: key, Value: value}
}

func (l *FileTxLogger) WriteDelete(key string) {
	l.events <- Event{EventType: EventDelete, Key: key, Value: "nil"}
}

func (l *FileTxLogger) Err() <-chan error {
	return l.errors
}

func (l *FileTxLogger) Run() {
	events := make(chan Event, 16)
	l.events = events

	errors := make(chan error, 1)
	l.errors = errors

	go func() {
		for e := range events {
			l.lastSequence++
			_, err := fmt.Fprintf(
				l.file,
				"%d\t%d\t%s\t%s\n",
				l.lastSequence,
				e.EventType,
				e.Key,
				e.Value,
			)
			if err != nil {
				errors <- err
				return
			}
		}
	}()
}

func (l *FileTxLogger) Close() {

}

func (l *FileTxLogger) ReadEvents() (<-chan Event, <-chan error) {
	outEvent := make(chan Event)
	outError := make(chan error)
	scanner := bufio.NewScanner(l.file)

	go func() {
		var e Event
		defer close(outEvent)
		defer close(outError)

		for scanner.Scan() {
			line := scanner.Text()

			if _, err := fmt.Sscanf(
				line,
				"%d\t%d\t%s\t%v\n",
				&e.Sequence,
				&e.EventType,
				&e.Key,
				&e.Value,
			); err != nil {
				outError <- fmt.Errorf("transaction parse error: %v", err)
				//return
			}
			klog.Infof("scanned seq %d type %d key %s val %s", e.Sequence, e.EventType, e.Key, e.Value)

			if l.lastSequence >= e.Sequence {
				outError <- fmt.Errorf("transaction numbers out of sequence")
				return
			}

			l.lastSequence = e.Sequence
			outEvent <- e
		}

		if err := scanner.Err(); err != nil {
			outError <- fmt.Errorf("transaction log read failure: %v", err)
			return
		}
	}()

	return outEvent, outError
}

func (l *FileTxLogger) compact() {

}
