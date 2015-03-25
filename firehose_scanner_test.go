package noaa_test

import (
	"github.com/cloudfoundry/noaa"

	"github.com/cloudfoundry/loggregatorlib/loggertesthelper"
	"github.com/cloudfoundry/loggregatorlib/server/handlers"
	"github.com/cloudfoundry/noaa/events"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"net/http"
	"net/http/httptest"
	"time"
)

var _ = Describe("FirehoseScanner", func() {
	var (
		fakeHandler          *FakeHandler
		testServer           *httptest.Server
		trafficControllerUrl string
		subscriptionId       string
	)

	var startFakeTrafficController = func() {
		fakeHandler = &FakeHandler{
			InputChan: make(chan []byte, 10),
			GenerateHandler: func(input chan []byte) http.Handler {
				return handlers.NewWebsocketHandler(input, 100*time.Millisecond, loggertesthelper.Logger())
			},
		}

		testServer = httptest.NewServer(fakeHandler)
		trafficControllerUrl = "ws://" + testServer.Listener.Addr().String()
		subscriptionId = "some-id"
	}

	BeforeEach(func() {
		startFakeTrafficController()
	})

	It("should return data until the connection is closed", func(done Done) {
		count := 50
		defer close(done)

		scanner := noaa.NewStreamScannerFactory(trafficControllerUrl, nil)
		firehose := scanner.Firehose("some-id", "some-token")

		go putDataOntoTheLine(count, fakeHandler)

		resultsChan := make(chan string, 5)
		errChan := make(chan error, 5)
		for i := 0; i < 5; i++ {
			go func() {
				var event *events.Envelope
				var err error
				for event, err = firehose.Scan(); err == nil; event, err = firehose.Scan() {
					resultsChan <- string(event.GetLogMessage().GetMessage())
				}
				errChan <- err
			}()
		}

		expectedResults := buildExpectedSlice(count)
		results := fetchResults(count, resultsChan)
		Expect(results).To(ConsistOf(expectedResults))
		firehose.Close()

		for i := 0; i < 5; i++ {
			Eventually(errChan).Should(Receive())
		}
	})
})
