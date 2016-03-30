package consumer_test

import (
	"crypto/tls"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"time"

	"github.com/cloudfoundry/loggregatorlib/loggertesthelper"
	"github.com/cloudfoundry/loggregatorlib/server/handlers"
	"github.com/cloudfoundry/noaa"
	"github.com/cloudfoundry/noaa/consumer"
	"github.com/cloudfoundry/noaa/errors"
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
		cnsmr.Close()
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
			called := make(chan bool)
			cb := func() { called <- true }

			cnsmr.SetOnConnectCallback(cb)

			cnsmr.TailingLogsWithoutReconnect(appGuid, authToken)

			Eventually(called).Should(Receive())
		})

		Context("when the connection fails", func() {
			BeforeEach(func() {
				trafficControllerURL = "!!!bad-url"
			})

			It("does not call the callback", func() {
				called := make(chan bool)
				cb := func() { called <- true }

				cnsmr.SetOnConnectCallback(cb)
				cnsmr.TailingLogsWithoutReconnect(appGuid, authToken)

				Consistently(called).ShouldNot(Receive())
			})
		})

		Context("when authorization fails", func() {
			var (
				failure test_helpers.AuthFailureHandler
			)

			BeforeEach(func() {
				failure = test_helpers.AuthFailureHandler{Message: "Helpful message"}
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
		var debugPrinter *mockDebugPrinter

		BeforeEach(func() {
			startFakeTrafficController()
		})

		JustBeforeEach(func() {
			debugPrinter = newMockDebugPrinter()
			cnsmr.SetDebugPrinter(debugPrinter)
		})

		It("includes websocket handshake", func() {
			fakeHandler.Close()

			cnsmr.TailingLogsWithoutReconnect(appGuid, authToken)

			var body string
			Eventually(debugPrinter.PrintInput.Dump).Should(Receive(&body))
			Expect(body).To(ContainSubstring("Sec-WebSocket-Version: 13"))
		})

		It("does not include messages sent or received", func() {
			fakeHandler.InputChan <- marshalMessage(createMessage("hello", 0))

			fakeHandler.Close()
			cnsmr.TailingLogsWithoutReconnect(appGuid, authToken)

			var body string
			Eventually(debugPrinter.PrintInput.Dump).Should(Receive(&body))
			Expect(body).ToNot(ContainSubstring("hello"))
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
				var failure test_helpers.AuthFailureHandler

				BeforeEach(func() {
					failure = test_helpers.AuthFailureHandler{Message: "Helpful message"}
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

	Describe("TailingLogs", func() {
		var (
			logMessages <-chan *events.LogMessage
			errors      <-chan error
			retries     uint
		)

		BeforeEach(func() {
			retries = 5
			startFakeTrafficController()
		})

		JustBeforeEach(func() {
			logMessages, errors = cnsmr.TailingLogs(appGuid, authToken)
		})

		It("resets the attempt counter after a successful connection", func(done Done) {
			defer close(done)

			fakeHandler.InputChan <- marshalMessage(createMessage("message 1", 0))
			Eventually(logMessages).Should(Receive())

			fakeHandler.Close()
			expectedErrorCount := 4
			for i := 0; i < expectedErrorCount; i++ {
				Eventually(errors, time.Second).Should(Receive())
			}
			fakeHandler.Reset()

			fakeHandler.InputChan <- marshalMessage(createMessage("message 2", 0))

			Eventually(logMessages).Should(Receive())

			fakeHandler.Close()
			for i := uint(0); i < retries; i++ {
				Eventually(errors, time.Second).Should(Receive())
			}
		}, 20)

		Context("with a failing handler", func() {
			BeforeEach(func() {
				fakeHandler.Fail = true
			})

			It("attempts to connect five times", func() {

				fakeHandler.Close()

				for i := uint(0); i < retries; i++ {
					Eventually(errors).Should(Receive())
				}
			})

			It("waits 500ms before reconnecting", func() {
				fakeHandler.Close()

				start := time.Now()
				for i := uint(0); i < retries; i++ {
					Eventually(errors).Should(Receive())
				}
				end := time.Now()
				Expect(end).To(BeTemporally(">=", start.Add(4*500*time.Millisecond)))
			})

			It("will not attempt reconnect if consumer is closed", func() {
				Eventually(errors).Should(Receive())
				Expect(fakeHandler.WasCalled()).To(BeTrue())
				fakeHandler.Reset()
				cnsmr.Close()

				Eventually(errors).Should(BeClosed())
				Consistently(fakeHandler.WasCalled, 2).Should(BeFalse())
			})
		})
	})

	Describe("StreamWithoutReconnect", func() {
		var (
			incoming <-chan *events.Envelope
			errors   <-chan error
		)

		BeforeEach(func() {
			startFakeTrafficController()
		})

		JustBeforeEach(func() {
			incoming, errors = cnsmr.StreamWithoutReconnect(appGuid, authToken)
		})

		Context("when there is no TLS Config or consumerProxyFunc setting", func() {
			Context("when the connection can be established", func() {
				It("receives messages on the incoming channel", func(done Done) {
					defer close(done)

					fakeHandler.InputChan <- marshalMessage(createMessage("hello", 0))

					var message *events.Envelope
					Eventually(incoming).Should(Receive(&message))
					Expect(message.GetLogMessage().GetMessage()).To(Equal([]byte("hello")))
					fakeHandler.Close()

				})

				Context("with a specific app ID", func() {
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

				Context("when the message fails to parse", func() {
					It("skips that message but continues to read messages", func(done Done) {
						fakeHandler.InputChan <- []byte{0}
						fakeHandler.InputChan <- marshalMessage(createMessage("hello", 0))
						fakeHandler.Close()

						message := <-incoming

						Expect(message.GetLogMessage().GetMessage()).To(Equal([]byte("hello")))

						close(done)
					})
				})
			})

			Context("when the connection cannot be established", func() {
				BeforeEach(func() {
					trafficControllerURL = "!!!bad-url"
				})

				It("returns an error", func(done Done) {

					var err error
					Eventually(errors).Should(Receive(&err))
					Expect(err).To(HaveOccurred())
					Expect(err.Error()).To(ContainSubstring("Please ask your Cloud Foundry Operator"))

					close(done)
				})
			})

			Context("when the authorization fails", func() {
				var failer test_helpers.AuthFailureHandler

				BeforeEach(func() {
					failer = test_helpers.AuthFailureHandler{Message: "Helpful message"}
					testServer = httptest.NewServer(failer)
					trafficControllerURL = "ws://" + testServer.Listener.Addr().String()
				})

				It("it returns a helpful error message", func() {

					var err error
					Eventually(errors).Should(Receive(&err))
					Expect(err).To(HaveOccurred())
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
	})

	Describe("Stream", func() {
		var (
			envelopeChan <-chan *events.Envelope
			errorChan    <-chan error
		)

		BeforeEach(func() {
			startFakeTrafficController()
		})

		JustBeforeEach(func() {
			envelopeChan, errorChan = cnsmr.Stream(appGuid, authToken)
		})

		Context("connection errors", func() {
			BeforeEach(func() {
				fakeHandler.Fail = true
			})

			It("attempts to connect five times", func() {
				fakeHandler.Close()

				for i := 0; i < 5; i++ {
					Eventually(errorChan).Should(Receive())
				}
			})
		})

		It("waits 500ms before reconnecting", func() {
			fakeHandler.Close()
			start := time.Now()
			for i := 0; i < 5; i++ {
				Eventually(errorChan).Should(Receive())
			}
			end := time.Now()
			Expect(end).To(BeTemporally(">=", start.Add(4*500*time.Millisecond)))
		})

		It("resets the attempt counter after a successful connection", func(done Done) {
			defer close(done)

			fakeHandler.InputChan <- marshalMessage(createMessage("message 1", 0))
			Eventually(envelopeChan).Should(Receive())

			fakeHandler.Close()

			expectedErrorCount := 4
			for i := 0; i < expectedErrorCount; i++ {
				Eventually(errorChan).Should(Receive())
			}
			fakeHandler.Reset()

			fakeHandler.InputChan <- marshalMessage(createMessage("message 2", 0))

			Eventually(envelopeChan).Should(Receive())
			fakeHandler.Close()
			for i := 0; i < 5; i++ {
				Eventually(errorChan).Should(Receive())
			}
		}, 10)
	})

	Describe("Close", func() {
		var (
			incomingChan    <-chan *events.Envelope
			streamErrorChan <-chan error
		)

		BeforeEach(func() {
			startFakeTrafficController()
		})

		Context("when a connection is not open", func() {
			It("returns an error", func() {
				err := cnsmr.Close()

				Expect(err.Error()).To(Equal("connection does not exist"))
			})
		})

		Context("when a connection is open", func() {
			JustBeforeEach(func() {
				incomingChan, streamErrorChan = cnsmr.StreamWithoutReconnect(appGuid, authToken)
			})

			It("terminates the blocking function call", func(done Done) {
				defer close(done)

				fakeHandler.Close()

				Eventually(fakeHandler.WasCalled).Should(BeTrue())
				connErr := cnsmr.Close()
				Expect(connErr.Error()).To(ContainSubstring("use of closed network connection"))

				var err error
				Eventually(streamErrorChan).Should(Receive(&err))
				Expect(err.Error()).To(ContainSubstring("websocket: close 1000"))
			}, 10)
		})
	})

	Describe("RecentLogs", func() {
		var (
			receivedLogMessages []*events.LogMessage
			recentError         error
		)

		BeforeEach(func() {
			appGuid = "appGuid"
		})

		JustBeforeEach(func() {
			close(messagesToSend)
			receivedLogMessages, recentError = cnsmr.RecentLogs(appGuid, authToken)
		})

		Context("with an invalid URL", func() {
			BeforeEach(func() {
				trafficControllerURL = "invalid-url"
			})

			It("returns an error", func() {
				Expect(recentError).ToNot(BeNil())
			})
		})

		Context("when the connection can be established", func() {
			BeforeEach(func() {
				testServer = httptest.NewServer(handlers.NewHttpHandler(messagesToSend, loggertesthelper.Logger()))
				trafficControllerURL = "ws://" + testServer.Listener.Addr().String()

				messagesToSend <- marshalMessage(createMessage("test-message-0", 0))
				messagesToSend <- marshalMessage(createMessage("test-message-1", 0))
			})

			It("returns messages from the server", func() {
				Expect(recentError).NotTo(HaveOccurred())
				Expect(receivedLogMessages).To(HaveLen(2))
				Expect(receivedLogMessages[0].GetMessage()).To(Equal([]byte("test-message-0")))
				Expect(receivedLogMessages[1].GetMessage()).To(Equal([]byte("test-message-1")))
			})
		})

		Context("when the content type is missing", func() {
			BeforeEach(func() {
				serverMux := http.NewServeMux()
				serverMux.HandleFunc("/apps/appGuid/recentlogs", func(resp http.ResponseWriter, req *http.Request) {
					resp.Header().Set("Content-Type", "")
					resp.Write([]byte("OK"))
				})
				testServer = httptest.NewServer(serverMux)
				trafficControllerURL = "ws://" + testServer.Listener.Addr().String()
			})

			It("returns a bad reponse error message", func() {
				Expect(recentError).To(HaveOccurred())
				Expect(recentError).To(Equal(noaa.ErrBadResponse))
			})
		})

		Context("when the content length is unknown", func() {
			BeforeEach(func() {
				fakeHandler = &test_helpers.FakeHandler{
					ContentLen: "-1",
					InputChan:  make(chan []byte, 10),
					GenerateHandler: func(input chan []byte) http.Handler {
						return handlers.NewHttpHandler(input, loggertesthelper.Logger())
					},
				}
				testServer = httptest.NewServer(fakeHandler)
				trafficControllerURL = "ws://" + testServer.Listener.Addr().String()

				fakeHandler.InputChan <- marshalMessage(createMessage("bad-content-length", 0))
				fakeHandler.Close()
			})

			It("does not throw an error", func() {
				Expect(recentError).NotTo(HaveOccurred())
				Expect(receivedLogMessages).To(HaveLen(1))
			})

		})

		Context("when the content type doesn't have a boundary", func() {
			BeforeEach(func() {
				serverMux := http.NewServeMux()
				serverMux.HandleFunc("/apps/appGuid/recentlogs", func(resp http.ResponseWriter, req *http.Request) {
					resp.Write([]byte("OK"))
				})
				testServer = httptest.NewServer(serverMux)
				trafficControllerURL = "ws://" + testServer.Listener.Addr().String()
			})

			It("returns a bad reponse error message", func() {
				Expect(recentError).To(HaveOccurred())
				Expect(recentError).To(Equal(noaa.ErrBadResponse))
			})

		})

		Context("when the content type's boundary is blank", func() {
			BeforeEach(func() {
				serverMux := http.NewServeMux()
				serverMux.HandleFunc("/apps/appGuid/recentlogs", func(resp http.ResponseWriter, req *http.Request) {
					resp.Header().Set("Content-Type", "boundary=")
					resp.Write([]byte("OK"))
				})
				testServer = httptest.NewServer(serverMux)
				trafficControllerURL = "ws://" + testServer.Listener.Addr().String()
			})

			It("returns a bad reponse error message", func() {
				Expect(recentError).To(HaveOccurred())
				Expect(recentError).To(Equal(noaa.ErrBadResponse))
			})

		})

		Context("when the path is not found", func() {
			BeforeEach(func() {
				serverMux := http.NewServeMux()
				serverMux.HandleFunc("/apps/appGuid/recentlogs", func(resp http.ResponseWriter, req *http.Request) {
					resp.WriteHeader(http.StatusNotFound)
				})
				testServer = httptest.NewServer(serverMux)
				trafficControllerURL = "ws://" + testServer.Listener.Addr().String()
			})

			It("returns a not found reponse error message", func() {
				Expect(recentError).To(HaveOccurred())
				Expect(recentError).To(Equal(noaa.ErrNotFound))
			})

		})

		Context("when the authorization fails", func() {
			var failer test_helpers.AuthFailureHandler

			BeforeEach(func() {
				failer = test_helpers.AuthFailureHandler{Message: "Helpful message"}
				serverMux := http.NewServeMux()
				serverMux.Handle(fmt.Sprintf("/apps/%s/recentlogs", appGuid), failer)
				testServer = httptest.NewServer(serverMux)
				trafficControllerURL = "ws://" + testServer.Listener.Addr().String()
			})

			It("returns a helpful error message", func() {
				Expect(recentError).To(HaveOccurred())
				Expect(recentError.Error()).To(ContainSubstring("You are not authorized. Helpful message"))
				Expect(recentError).To(BeAssignableToTypeOf(&errors.UnauthorizedError{}))
			})
		})
	})
	Describe("ContainerMetrics", func() {
		var (
			receivedContainerMetrics []*events.ContainerMetric
			recentError              error
		)

		BeforeEach(func() {
			appGuid = "appGuid"
		})

		JustBeforeEach(func() {
			close(messagesToSend)
			receivedContainerMetrics, recentError = cnsmr.ContainerMetrics(appGuid, authToken)
		})

		Context("when the connection cannot be established", func() {
			BeforeEach(func() {
				trafficControllerURL = "invalid-url"
			})

			It("invalid urls return error", func() {
				Expect(recentError).ToNot(BeNil())
			})
		})

		Context("when the connection can be established", func() {
			BeforeEach(func() {
				testServer = httptest.NewServer(handlers.NewHttpHandler(messagesToSend, loggertesthelper.Logger()))
				trafficControllerURL = "ws://" + testServer.Listener.Addr().String()
			})

			Context("with a successful connection", func() {
				BeforeEach(func() {
					messagesToSend <- marshalMessage(createContainerMetric(2, 2000))
					messagesToSend <- marshalMessage(createContainerMetric(1, 1000))
				})

				It("returns messages from the server", func() {
					Expect(recentError).NotTo(HaveOccurred())
					Expect(receivedContainerMetrics).To(HaveLen(2))
					Expect(receivedContainerMetrics[0].GetInstanceIndex()).To(Equal(int32(1)))
					Expect(receivedContainerMetrics[1].GetInstanceIndex()).To(Equal(int32(2)))
				})
			})

			Context("when trafficcontroller returns an error as a log message", func() {
				BeforeEach(func() {
					messagesToSend <- marshalMessage(createContainerMetric(2, 2000))
					messagesToSend <- marshalMessage(createMessage("an error occurred", 2000))
				})

				It("returns the error", func() {
					Expect(recentError).To(HaveOccurred())
					Expect(recentError).To(MatchError("Upstream error: an error occurred"))
				})
			})
		})

		Context("when the content type is missing", func() {
			BeforeEach(func() {
				serverMux := http.NewServeMux()
				serverMux.HandleFunc("/apps/appGuid/containermetrics", func(resp http.ResponseWriter, req *http.Request) {
					resp.Header().Set("Content-Type", "")
					resp.Write([]byte("OK"))
				})
				testServer = httptest.NewServer(serverMux)
				trafficControllerURL = "ws://" + testServer.Listener.Addr().String()
			})

			It("returns a bad reponse error message", func() {
				Expect(recentError).To(HaveOccurred())
				Expect(recentError).To(Equal(noaa.ErrBadResponse))
			})
		})

		Context("when the content length is unknown", func() {
			BeforeEach(func() {
				fakeHandler = &test_helpers.FakeHandler{
					ContentLen: "-1",
					InputChan:  make(chan []byte, 10),
					GenerateHandler: func(input chan []byte) http.Handler {
						return handlers.NewHttpHandler(input, loggertesthelper.Logger())
					},
				}
				testServer = httptest.NewServer(fakeHandler)
				trafficControllerURL = "ws://" + testServer.Listener.Addr().String()

				fakeHandler.InputChan <- marshalMessage(createContainerMetric(2, 2000))
				fakeHandler.Close()
			})

			It("does not throw an error", func() {
				Expect(recentError).NotTo(HaveOccurred())
				Expect(receivedContainerMetrics).To(HaveLen(1))
			})

		})

		Context("when the content type doesn't have a boundary", func() {
			BeforeEach(func() {

				serverMux := http.NewServeMux()
				serverMux.HandleFunc("/apps/appGuid/containermetrics", func(resp http.ResponseWriter, req *http.Request) {
					resp.Write([]byte("OK"))
				})
				testServer = httptest.NewServer(serverMux)
				trafficControllerURL = "ws://" + testServer.Listener.Addr().String()
			})

			It("returns a bad reponse error message", func() {

				Expect(recentError).To(HaveOccurred())
				Expect(recentError).To(Equal(noaa.ErrBadResponse))
			})

		})

		Context("when the content type's boundary is blank", func() {
			BeforeEach(func() {

				serverMux := http.NewServeMux()
				serverMux.HandleFunc("/apps/appGuid/containermetrics", func(resp http.ResponseWriter, req *http.Request) {
					resp.Header().Set("Content-Type", "boundary=")
					resp.Write([]byte("OK"))
				})
				testServer = httptest.NewServer(serverMux)
				trafficControllerURL = "ws://" + testServer.Listener.Addr().String()
			})

			It("returns a bad reponse error message", func() {

				Expect(recentError).To(HaveOccurred())
				Expect(recentError).To(Equal(noaa.ErrBadResponse))
			})

		})

		Context("when the path is not found", func() {
			BeforeEach(func() {

				serverMux := http.NewServeMux()
				serverMux.HandleFunc("/apps/appGuid/containermetrics", func(resp http.ResponseWriter, req *http.Request) {
					resp.WriteHeader(http.StatusNotFound)
				})
				testServer = httptest.NewServer(serverMux)
				trafficControllerURL = "ws://" + testServer.Listener.Addr().String()
			})

			It("returns a not found reponse error message", func() {

				Expect(recentError).To(HaveOccurred())
				Expect(recentError).To(Equal(noaa.ErrNotFound))
			})

		})

		Context("when the authorization fails", func() {
			var failer test_helpers.AuthFailureHandler

			BeforeEach(func() {
				failer = test_helpers.AuthFailureHandler{Message: "Helpful message"}
				serverMux := http.NewServeMux()
				serverMux.Handle("/apps/appGuid/containermetrics", failer)
				testServer = httptest.NewServer(serverMux)
				trafficControllerURL = "ws://" + testServer.Listener.Addr().String()
			})

			It("returns a helpful error message", func() {

				Expect(recentError).To(HaveOccurred())
				Expect(recentError.Error()).To(ContainSubstring("You are not authorized. Helpful message"))
				Expect(recentError).To(BeAssignableToTypeOf(&errors.UnauthorizedError{}))
			})
		})
	})

	Describe("Firehose", func() {
		var (
			envelopeChan <-chan *events.Envelope
			errorChan    <-chan error
		)

		JustBeforeEach(func() {
			envelopeChan, errorChan = cnsmr.Firehose("subscription-id", authToken)
		})

		BeforeEach(func() {
			startFakeTrafficController()
		})

		Context("when connection fails", func() {
			BeforeEach(func() {
				fakeHandler.Fail = true
			})

			It("attempts to connect five times", func() {
				fakeHandler.Close()
				for i := 0; i < 5; i++ {
					Eventually(errorChan).Should(Receive())
				}
			})
		})

		It("waits 500ms before reconnecting", func() {

			fakeHandler.Close()
			start := time.Now()
			for i := 0; i < 5; i++ {
				Eventually(errorChan).Should(Receive())
			}

			end := time.Now()
			Expect(end).To(BeTemporally(">=", start.Add(4*500*time.Millisecond)))
			cnsmr.Close()
		})

		Context("with data in the server", func() {
			BeforeEach(func() {
				fakeHandler.InputChan <- marshalMessage(createMessage("message 1", 0))
			})

			It("resets the attempt counter after a successful connection", func(done Done) {
				defer close(done)
				Eventually(envelopeChan).Should(Receive())

				fakeHandler.Close()

				expectedErrorCount := 4
				for i := 0; i < expectedErrorCount; i++ {
					Eventually(errorChan).Should(Receive())
				}
				fakeHandler.Reset()

				fakeHandler.InputChan <- marshalMessage(createMessage("message 2", 0))

				Eventually(envelopeChan).Should(Receive())
				fakeHandler.Close()
				for i := 0; i < 5; i++ {
					Eventually(errorChan).Should(Receive())
				}
			}, 10)
		})
	})

	Describe("FirehoseWithoutReconnect", func() {
		var (
			incomingChan    <-chan *events.Envelope
			streamErrorChan <-chan error
			idleTimeout     time.Duration
		)

		BeforeEach(func() {
			incomingChan = make(chan *events.Envelope)
			startFakeTrafficController()
		})

		JustBeforeEach(func() {
			cnsmr.SetIdleTimeout(idleTimeout)
			incomingChan, streamErrorChan = cnsmr.FirehoseWithoutReconnect("subscription-id", authToken)
		})

		Context("when there is no TLS Config or consumerProxyFunc setting", func() {
			Context("when the connection can be established", func() {
				It("receives messages from the full firehose", func() {
					fakeHandler.Close()

					Eventually(fakeHandler.GetLastURL).Should(ContainSubstring("/firehose/subscription-id"))
				})

				Context("with a message", func() {
					BeforeEach(func() {
						fakeHandler.InputChan <- marshalMessage(createMessage("hello", 0))
					})

					It("receives messages on the incoming channel", func(done Done) {
						defer close(done)

						message := <-incomingChan

						Expect(message.GetLogMessage().GetMessage()).To(Equal([]byte("hello")))
						fakeHandler.Close()
					})
				})

				Context("with an authorization token", func() {
					BeforeEach(func() {
						authToken = "auth-token"
					})

					It("sends an Authorization header with an access token", func() {
						fakeHandler.Close()
						Eventually(fakeHandler.GetAuthHeader).Should(Equal("auth-token"))
					})
				})

				Context("when the message fails to parse", func() {
					BeforeEach(func() {
						fakeHandler.InputChan <- []byte{0}
						fakeHandler.InputChan <- marshalMessage(createMessage("hello", 0))
					})

					It("skips that message but continues to read messages", func(done Done) {
						defer close(done)
						fakeHandler.Close()

						message := <-incomingChan
						Expect(message.GetLogMessage().GetMessage()).To(Equal([]byte("hello")))
					})
				})
			})

			Context("when the connection cannot be established", func() {
				BeforeEach(func() {
					trafficControllerURL = "!!!bad-url"
				})

				It("returns an error", func(done Done) {
					defer close(done)

					var err error
					Eventually(streamErrorChan).Should(Receive(&err))
					Expect(err).To(HaveOccurred())
					Expect(err.Error()).To(ContainSubstring("Please ask your Cloud Foundry Operator"))
				})
			})

			Context("when the authorization fails", func() {
				var failer test_helpers.AuthFailureHandler

				BeforeEach(func() {
					failer = test_helpers.AuthFailureHandler{Message: "Helpful message"}
					testServer = httptest.NewServer(failer)
					trafficControllerURL = "ws://" + testServer.Listener.Addr().String()
				})

				It("it returns a helpful error message", func() {
					var err error
					Eventually(streamErrorChan).Should(Receive(&err))
					Expect(err).To(HaveOccurred())
					Expect(err.Error()).To(ContainSubstring("You are not authorized. Helpful message"))
				})
			})
		})

		Context("when the connection read takes too long", func() {
			BeforeEach(func() {
				idleTimeout = 500 * time.Millisecond
			})

			It("returns an error when the idle timeout expires", func() {
				var err error
				Eventually(streamErrorChan).Should(Receive(&err))
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(ContainSubstring("i/o timeout"))
			})
		})

		Context("when SSL settings are passed in", func() {
			BeforeEach(func() {
				testServer = httptest.NewTLSServer(handlers.NewWebsocketHandler(messagesToSend, 100*time.Millisecond, loggertesthelper.Logger()))
				trafficControllerURL = "wss://" + testServer.Listener.Addr().String()

				tlsSettings = &tls.Config{InsecureSkipVerify: true}
			})

			It("connects using those settings", func() {
				Consistently(streamErrorChan).ShouldNot(Receive())

				close(messagesToSend)
				Eventually(streamErrorChan).Should(Receive())
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
