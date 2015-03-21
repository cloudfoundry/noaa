package noaa

import (
	"crypto/tls"
)

type StreamScanner struct {
	consumer *Consumer
}

func NewStreamScannerFactory(trafficControllerUrl string, tlsConfig *tls.Config) *StreamScanner {
	return &StreamScanner{
		consumer: NewConsumer(trafficControllerUrl, tlsConfig, nil),
	}
}

func (s *StreamScanner) TailLogs(appGuid, authToken string) *TailingLogsScanner {
	return newTailingLogsScanner(appGuid, authToken, s.consumer)
}
