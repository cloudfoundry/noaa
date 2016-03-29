package consumer_test

import (
	"crypto/tls"
	"net/http"
	"net/http/httptest"
	"net/url"
	"time"

	"github.com/cloudfoundry/loggregatorlib/loggertesthelper"
	"github.com/cloudfoundry/loggregatorlib/server/handlers"
	"github.com/cloudfoundry/noaa/consumer"
	"github.com/cloudfoundry/noaa/test_helpers"
	"github.com/cloudfoundry/sonde-go/events"
	"github.com/gogo/protobuf/proto"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Consumer", func() {

	var (
		cnsmr                *consumer.Consumer
		trafficControllerURL string
		testServer           *httptest.Server
		fakeHandler          *test_helpers.FakeHandler
		tlsSettings          *tls.Config
		proxy                func(*http.Request) (*url.URL, error)

		appGuid        string
		authToken      string
		messagesToSend chan []byte
	)

	BeforeEach(func() {
		messagesToSend = make(chan []byte, 256)
	})

	JustBeforeEach(func() {
		cnsmr = consumer.New(trafficControllerURL, tlsSettings, proxy)
	})

	AfterEach(func() {
		if testServer != nil {
			testServer.Close()
		}
	})

	Describe("SetOnConnectCallback", func() {
		BeforeEach(func() {
			testServer = httptest.NewServer(handlers.NewWebsocketHandler(messagesToSend, 100*time.Millisecond, loggertesthelper.Logger()))
			trafficControllerURL = "ws://" + testServer.Listener.Addr().String()
			close(messagesToSend)
		})

		It("sets a callback and calls it when connecting", func() {
			called := false
			cb := func() { called = true }

			cnsmr.SetOnConnectCallback(cb)

			cnsmr.TailingLogsWithoutReconnect(appGuid, authToken)

			Eventually(func() bool { return called }).Should(BeTrue())
		})

		Context("when the connection fails", func() {
			BeforeEach(func() {
				trafficControllerURL = "!!!bad-url"
			})

			It("does not call the callback", func() {
				called := false
				cb := func() { called = true }

				cnsmr.SetOnConnectCallback(cb)
				cnsmr.TailingLogsWithoutReconnect(appGuid, authToken)

				Consistently(func() bool { return called }).Should(BeFalse())
			})
		})

		Context("when authorization fails", func() {
			var (
				failure test_helpers.AuthFailure
			)

			BeforeEach(func() {
				failure = test_helpers.AuthFailure{Message: "Helpful message"}
				testServer = httptest.NewServer(failure)
				trafficControllerURL = "ws://" + testServer.Listener.Addr().String()
			})

			It("does not call the callback", func() {
				called := false
				cb := func() { called = true }

				cnsmr.SetOnConnectCallback(cb)
				cnsmr.TailingLogsWithoutReconnect(appGuid, authToken)

				Consistently(func() bool { return called }).Should(BeFalse())
			})

		})

	})

	var startFakeTrafficController = func() {
		fakeHandler = &test_helpers.FakeHandler{
			InputChan: make(chan []byte, 10),
			GenerateHandler: func(input chan []byte) http.Handler {
				return handlers.NewWebsocketHandler(input, 100*time.Millisecond, loggertesthelper.Logger())
			},
		}

		testServer = httptest.NewServer(fakeHandler)
		trafficControllerURL = "ws://" + testServer.Listener.Addr().String()
		appGuid = "app-guid"
	}

	Describe("Debug Printing", func() {
		var debugPrinter *test_helpers.FakeDebugPrinter

		BeforeEach(func() {
			startFakeTrafficController()
		})

		JustBeforeEach(func() {
			debugPrinter = &test_helpers.FakeDebugPrinter{}
			cnsmr.SetDebugPrinter(debugPrinter)
		})

		It("includes websocket handshake", func() {
			fakeHandler.Close()

			cnsmr.TailingLogsWithoutReconnect(appGuid, authToken)

			Eventually(func() int { return len(debugPrinter.Messages) }).Should(BeNumerically(">=", 1))
			Expect(debugPrinter.Messages[0].Body).To(ContainSubstring("Sec-WebSocket-Version: 13"))
		})

		It("does not include messages sent or received", func() {
			fakeHandler.InputChan <- marshalMessage(createMessage("hello", 0))

			fakeHandler.Close()
			cnsmr.TailingLogsWithoutReconnect(appGuid, authToken)

			Eventually(func() int { return len(debugPrinter.Messages) }).Should(BeNumerically(">=", 1))
			Expect(debugPrinter.Messages[0].Body).ToNot(ContainSubstring("hello"))
		})
	})

	Describe("TailingLogsWithoutReconnect", func() {
		var (
			logMessages <-chan *events.LogMessage
			errors      <-chan error
		)

		BeforeEach(func() {
			startFakeTrafficController()
		})

		JustBeforeEach(func() {
			logMessages, errors = cnsmr.TailingLogsWithoutReconnect(appGuid, authToken)
		})

		AfterEach(func() {
			cnsmr.Close()
			Eventually(logMessages).Should(BeClosed())
		})

		Context("when there is no TLS Config or consumerProxyFunc setting", func() {
			Context("when the connection can be established", func() {
				It("returns a read only LogMessage chan and error chan", func() {
					fakeHandler.InputChan <- marshalMessage(createMessage("hello", 0))

					var message *events.LogMessage
					Eventually(logMessages).Should(Receive(&message))
					Expect(message.GetMessage()).To(Equal([]byte("hello")))
					Consistently(errors).ShouldNot(Receive())
				})

				It("receives messages on the incoming channel", func(done Done) {
					fakeHandler.InputChan <- marshalMessage(createMessage("hello", 0))

					message := <-logMessages

					Expect(message.GetMessage()).To(Equal([]byte("hello")))
					fakeHandler.Close()

					close(done)
				})

				It("does not include metrics", func(done Done) {
					fakeHandler.InputChan <- marshalMessage(createContainerMetric(int32(1), int64(2)))
					fakeHandler.InputChan <- marshalMessage(createMessage("hello", 0))

					message := <-logMessages

					Expect(message.GetMessage()).To(Equal([]byte("hello")))
					fakeHandler.Close()

					close(done)
				})

				Context("with a specific app", func() {
					BeforeEach(func() {
						appGuid = "the-app-guid"
					})

					It("sends messages for a specific app", func() {
						fakeHandler.Close()

						Eventually(fakeHandler.GetLastURL).Should(ContainSubstring("/apps/the-app-guid/stream"))
					})
				})

				Context("with an access token", func() {
					BeforeEach(func() {
						authToken = "auth-token"
					})

					It("sends an Authorization header with an access token", func() {
						fakeHandler.Close()

						Eventually(fakeHandler.GetAuthHeader).Should(Equal("auth-token"))
					})
				})

				Context("when remote connection dies unexpectedly", func() {
					It("receives a message on the error channel", func(done Done) {
						fakeHandler.Close()

						var err error
						Eventually(errors).Should(Receive(&err))
						Expect(err.Error()).To(ContainSubstring("websocket: close 1000"))

						close(done)
					})
				})

				Context("when the message fails to parse", func() {
					It("skips that message but continues to read messages", func(done Done) {
						fakeHandler.InputChan <- []byte{0}
						fakeHandler.InputChan <- marshalMessage(createMessage("hello", 0))
						fakeHandler.Close()

						message := <-logMessages

						Expect(message.GetMessage()).To(Equal([]byte("hello")))

						close(done)
					})
				})
			})

			Context("when the connection cannot be established", func() {
				BeforeEach(func() {
					trafficControllerURL = "!!!bad-url"
				})

				It("receives an error on errChan", func(done Done) {
					var err error
					Eventually(errors).Should(Receive(&err))
					Expect(err.Error()).To(ContainSubstring("Please ask your Cloud Foundry Operator"))

					close(done)
				})
			})

			Context("when the authorization fails", func() {
				var failure test_helpers.AuthFailure

				BeforeEach(func() {
					failure = test_helpers.AuthFailure{Message: "Helpful message"}
					testServer = httptest.NewServer(failure)
					trafficControllerURL = "ws://" + testServer.Listener.Addr().String()
				})

				It("it returns a helpful error message", func() {
					var err error
					Eventually(errors).Should(Receive(&err))
					Expect(err.Error()).To(ContainSubstring("You are not authorized. Helpful message"))
				})
			})
		})

		Context("when SSL settings are passed in", func() {
			BeforeEach(func() {
				testServer = httptest.NewTLSServer(handlers.NewWebsocketHandler(messagesToSend, 100*time.Millisecond, loggertesthelper.Logger()))
				trafficControllerURL = "wss://" + testServer.Listener.Addr().String()

				tlsSettings = &tls.Config{InsecureSkipVerify: true}
			})

			It("connects using those settings", func() {
				Consistently(errors).ShouldNot(Receive())

				close(messagesToSend)
				Eventually(errors).Should(Receive())
			})
		})

		Context("when error source is not NOAA", func() {
			It("does not pass on the error", func(done Done) {
				fakeHandler.InputChan <- marshalMessage(createError("foreign error"))

				Consistently(errors).Should(BeEmpty())
				fakeHandler.Close()

				close(done)
			})

			It("continues to process log messages", func() {
				fakeHandler.InputChan <- marshalMessage(createError("foreign error"))
				fakeHandler.InputChan <- marshalMessage(createMessage("hello", 0))

				fakeHandler.Close()

				Eventually(logMessages).Should(Receive())
			})
		})
	})

})

func createMessage(message string, timestamp int64) *events.Envelope {
	if timestamp == 0 {
		timestamp = time.Now().UnixNano()
	}

	logMessage := createLogMessage(message, timestamp)

	return &events.Envelope{
		LogMessage: logMessage,
		EventType:  events.Envelope_LogMessage.Enum(),
		Origin:     proto.String("fake-origin-1"),
		Timestamp:  proto.Int64(timestamp),
	}
}

func createContainerMetric(instanceIndex int32, timestamp int64) *events.Envelope {
	if timestamp == 0 {
		timestamp = time.Now().UnixNano()
	}

	cm := &events.ContainerMetric{
		ApplicationId: proto.String("appId"),
		InstanceIndex: proto.Int32(instanceIndex),
		CpuPercentage: proto.Float64(1),
		MemoryBytes:   proto.Uint64(2),
		DiskBytes:     proto.Uint64(3),
	}

	return &events.Envelope{
		ContainerMetric: cm,
		EventType:       events.Envelope_ContainerMetric.Enum(),
		Origin:          proto.String("fake-origin-1"),
		Timestamp:       proto.Int64(timestamp),
	}
}

func createError(message string) *events.Envelope {
	timestamp := time.Now().UnixNano()

	err := &events.Error{
		Message: &message,
		Source:  proto.String("foreign"),
		Code:    proto.Int32(42),
	}

	return &events.Envelope{
		Error:     err,
		EventType: events.Envelope_Error.Enum(),
		Origin:    proto.String("fake-origin-1"),
		Timestamp: proto.Int64(timestamp),
	}
}

func createLogMessage(message string, timestamp int64) *events.LogMessage {
	return &events.LogMessage{
		Message:     []byte(message),
		MessageType: events.LogMessage_OUT.Enum(),
		AppId:       proto.String("my-app-guid"),
		SourceType:  proto.String("DEA"),
		Timestamp:   proto.Int64(timestamp),
	}
}

func marshalMessage(message *events.Envelope) []byte {
	data, err := proto.Marshal(message)
	if err != nil {
		println(err.Error())
	}

	return data
}
