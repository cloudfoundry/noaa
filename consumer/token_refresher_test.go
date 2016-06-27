package consumer_test

import (
	"errors"

	"github.com/cloudfoundry/noaa/consumer"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Consumer fetching tokens from UAA", func() {
	It("uses the token refresher to obtain a token before making async requests", func() {
		refresher := newMockTokenRefresher()
		c := consumer.New("fakeTrafficControllerURL", nil, nil)

		c.RefreshTokenFrom(refresher)

		var called bool
		c.TailingLogs("some-fake-app-guid", "")
		Eventually(refresher.RefreshAuthTokenCalled).Should(Receive(&called))
		Expect(called).To(BeTrue())

		called = false
		c.TailingLogsWithoutReconnect("some-fake-app-guid", "")
		Eventually(refresher.RefreshAuthTokenCalled).Should(Receive(&called))
		Expect(called).To(BeTrue())

		called = false
		c.StreamWithoutReconnect("some-fake-app-guid", "")
		Eventually(refresher.RefreshAuthTokenCalled).Should(Receive(&called))
		Expect(called).To(BeTrue())

		called = false
		c.Stream("some-fake-app-guid", "")
		Eventually(refresher.RefreshAuthTokenCalled).Should(Receive(&called))
		Expect(called).To(BeTrue())

		called = false
		c.FirehoseWithoutReconnect("some-fake-app-guid", "")
		Eventually(refresher.RefreshAuthTokenCalled).Should(Receive(&called))
		Expect(called).To(BeTrue())

		called = false
		c.Firehose("some-fake-app-guid", "")
		Eventually(refresher.RefreshAuthTokenCalled).Should(Receive(&called))
		Expect(called).To(BeTrue())
	})

	It("uses the token refresher to obtain a token before making sync requests", func() {
		refresher := newMockTokenRefresher()
		refresher.RefreshAuthTokenOutput.Token <- "some-example-token"
		refresher.RefreshAuthTokenOutput.AuthError <- nil

		c := consumer.New("fakeTrafficControllerURL", nil, nil)

		c.RefreshTokenFrom(refresher)

		var called bool
		c.RecentLogs("some-fake-app-guid", "")

		Eventually(refresher.RefreshAuthTokenCalled).Should(Receive(&called))
		Expect(called).To(BeTrue())

		called = false
		refresher.RefreshAuthTokenOutput.Token <- "some-example-token"
		refresher.RefreshAuthTokenOutput.AuthError <- nil
		c.ContainerMetrics("some-fake-app-guid", "")
		Eventually(refresher.RefreshAuthTokenCalled).Should(Receive(&called))
		Expect(called).To(BeTrue())
	})

	It("does not use the token refresher if an auth token has been passed in", func() {
		refresher := newMockTokenRefresher()

		c := consumer.New("fakeTrafficControllerURL", nil, nil)

		c.RefreshTokenFrom(refresher)

		c.TailingLogs("some-fake-app-guid", "someToken")
		Consistently(refresher.RefreshAuthTokenCalled).ShouldNot(Receive())
	})

	It("returns any error when fetching the token from the refresher", func() {
		errMsg := "Fetching authToken failed"
		refresher := newMockTokenRefresher()
		refresher.RefreshAuthTokenOutput.Token <- ""
		refresher.RefreshAuthTokenOutput.AuthError <- errors.New(errMsg)

		c := consumer.New("fakeTrafficControllerURL", nil, nil)

		c.RefreshTokenFrom(refresher)

		var err error
		_, errChan := c.TailingLogs("some-fake-app-guid", "")
		Eventually(errChan).Should(Receive(&err))
		Expect(err).To(MatchError(errMsg))
	})

	It("returns a helpful error message if the token refresher has not been set", func() {
		c := consumer.New("fakeTrafficControllerURL", nil, nil)

		var err error
		_, errChan := c.TailingLogs("some-fake-app-guid", "")
		Eventually(errChan).Should(Receive(&err))
		Expect(err).To(MatchError("no token refresher has been set"))
	})
})
