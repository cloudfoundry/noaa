package consumer_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"time"

	"github.com/cloudfoundry/noaa/v2/consumer"
	"github.com/cloudfoundry/noaa/v2/consumer/internal"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

type nullHandler chan struct{}

func (h nullHandler) ServeHTTP(http.ResponseWriter, *http.Request) {
	<-h
}

const (
	appGuid     = "fakeAppGuid"
	authTkn     = "fakeAuthToken"
	testTimeout = 1 * time.Second
)

var (
	cnsmr       *consumer.Consumer
	testServer  *httptest.Server
	fakeHandler nullHandler
)

var _ = AfterSuite(func() {
	if testServer != nil {
		testServer.Close()
	}
})

var _ = Describe("Timeout", func() {

	BeforeEach(func() {
		internal.Timeout = testTimeout

		fakeHandler = make(nullHandler, 1)
		testServer = httptest.NewServer(fakeHandler)
	})

	AfterEach(func() {
		cnsmr.Close()
	})

	Describe("TailingLogsWithoutReconnect", func() {
		It("times out due to handshake timeout", func() {
			defer close(fakeHandler)
			cnsmr = consumer.New(strings.Replace(testServer.URL, "http", "ws", 1), nil, nil)

			_, errCh := cnsmr.TailingLogsWithoutReconnect(appGuid, authTkn)
			var err error
			Eventually(errCh, 2*testTimeout).Should(Receive(&err))
			Expect(err.Error()).To(ContainSubstring("i/o timeout"))
		})
	})

	Describe("Stream", func() {
		It("times out due to handshake timeout", func() {
			defer close(fakeHandler)

			cnsmr = consumer.New(strings.Replace(testServer.URL, "http", "ws", 1), nil, nil)

			_, errCh := cnsmr.Stream(appGuid, authTkn)
			var err error
			Eventually(errCh, 2*testTimeout).Should(Receive(&err))
			Expect(err.Error()).To(ContainSubstring("i/o timeout"))
		})
	})
})
