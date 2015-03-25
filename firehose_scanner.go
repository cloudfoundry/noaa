package noaa

import (
	"github.com/cloudfoundry/noaa/events"
	"io"
	"sync"
)

type FirehoseScanner struct {
	outputChan chan *events.Envelope
	consumer   *Consumer
	err        error
	event      *events.Envelope
	syncRoot   *sync.RWMutex
	onceErr    *sync.Once
}

func newFirehoseScanner(appGuid, authToken string, consumer *Consumer) *FirehoseScanner {
	outputChan := make(chan *events.Envelope)
	scanner := &FirehoseScanner{
		outputChan: outputChan,
		consumer:   consumer,
		syncRoot:   &sync.RWMutex{},
		onceErr:    &sync.Once{},
	}

	go scanner.startScanning(appGuid, authToken, outputChan)
	return scanner
}

func (s *FirehoseScanner) Scan() (*events.Envelope, error) {
	event, ok := <-s.outputChan
	if ok {
		return event, nil
	}
	return nil, s.setError(io.EOF)
}

func (s *FirehoseScanner) Close() {
	s.setError(io.EOF)
}

func (s *FirehoseScanner) setError(err error) error {
	s.onceErr.Do(func() {
		s.err = err
		close(s.outputChan)
	})
	return s.err
}

func (s *FirehoseScanner) startScanning(subscriptionId, authToken string, outputChan chan<- *events.Envelope) {
	err := s.consumer.FirehoseWithoutReconnect(subscriptionId, authToken, outputChan)
	s.setError(err)
	s.Close()
}
