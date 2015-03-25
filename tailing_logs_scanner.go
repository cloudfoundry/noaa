package noaa

import (
	"github.com/cloudfoundry/noaa/events"
	"io"
	"sync"
)

type TailingLogsScanner struct {
	outputChan chan *events.LogMessage
	consumer   *Consumer
	err        error
	event      *events.LogMessage
	syncRoot   *sync.RWMutex
	onceErr    *sync.Once
}

func newTailingLogsScanner(appGuid, authToken string, consumer *Consumer) *TailingLogsScanner {
	outputChan := make(chan *events.LogMessage)
	scanner := &TailingLogsScanner{
		outputChan: outputChan,
		consumer:   consumer,
		syncRoot:   &sync.RWMutex{},
		onceErr:    &sync.Once{},
	}

	go scanner.startTailing(appGuid, authToken, outputChan)
	return scanner
}

func (s *TailingLogsScanner) Scan() (*events.LogMessage, error) {
	event, ok := <-s.outputChan
	if ok {
		return event, nil
	}
	return nil, s.setError(io.EOF)
}

func (s *TailingLogsScanner) Close() {
	s.setError(io.EOF)
}

func (s *TailingLogsScanner) setError(err error) error {
	s.onceErr.Do(func() {
		s.err = err
		close(s.outputChan)
	})
	return s.err
}

func (s *TailingLogsScanner) startTailing(appGuid, authToken string, outputChan chan<- *events.LogMessage) {
	err := s.consumer.TailingLogsWithoutReconnect(appGuid, authToken, outputChan)
	s.setError(err)
	s.Close()
}
