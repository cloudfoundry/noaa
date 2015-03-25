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
	"strconv"
	"time"
)

var _ = Describe("TailingLogsScanner", func() {
	var (
		fakeHandler          *FakeHandler
		testServer           *httptest.Server
		trafficControllerUrl string
		appGuid              string
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
		appGuid = "appGuidp-guid"
	}

	BeforeEach(func() {
		startFakeTrafficController()
	})

	It("should return data until the connection is closed", func(done Done) {
		count := 50
		defer close(done)

		scanner := noaa.NewStreamScannerFactory(trafficControllerUrl, nil)
		tailer := scanner.TailLogs("some-guid", "some-token")

		go putDataOntoTheLine(count, fakeHandler)

		resultsChan := make(chan string, 5)
		errChan := make(chan error, 5)
		for i := 0; i < 5; i++ {
			go func() {
				var event *events.LogMessage
				var err error
				for event, err = tailer.Scan(); err == nil; event, err = tailer.Scan() {
					resultsChan <- string(event.GetMessage())
				}
				errChan <- err
			}()
		}

		expectedResults := buildExpectedSlice(count)
		results := fetchResults(count, resultsChan)
		Expect(results).To(ConsistOf(expectedResults))
		tailer.Close()

		for i := 0; i < 5; i++ {
			Eventually(errChan).Should(Receive())
		}
	})
})

func buildExpectedSlice(count int) []string {
	expectedResults := make([]string, 0)
	for i := 0; i < count; i++ {
		expectedResults = append(expectedResults, strconv.Itoa(i))
	}
	return expectedResults
}

func putDataOntoTheLine(count int, fakeHandler *FakeHandler) {
	for i := 0; i < count; i++ {
		fakeHandler.InputChan <- marshalMessage(createMessage(strconv.Itoa(i), 0))
	}
}

func fetchResults(count int, resultsChan chan string) []string {
	results := make([]string, 0)
	for i := 0; i < count; i++ {
		msg := <-resultsChan
		results = append(results, msg)
	}
	return results
}
