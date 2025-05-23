package consumer_test

import (
	"crypto/tls"
	"fmt"
	"net/http"
	"net/http/httptest"

	"github.com/cloudfoundry/noaa/v2/consumer"
	"github.com/cloudfoundry/noaa/v2/errors"
	"github.com/cloudfoundry/noaa/v2/test_helpers"
	"github.com/cloudfoundry/sonde-go/events"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Consumer (Synchronous)", func() {
	var (
		cnsmr                *consumer.Consumer
		trafficControllerURL string
		testServer           *httptest.Server
		fakeHandler          *test_helpers.FakeHandler
		tlsSettings          *tls.Config

		appGuid        string
		authToken      string
		messagesToSend chan []byte

		recentPathBuilder consumer.RecentPathBuilder
	)

	BeforeEach(func() {
		trafficControllerURL = ""
		testServer = nil
		fakeHandler = nil
		tlsSettings = nil

		appGuid = ""
		authToken = ""
		messagesToSend = make(chan []byte, 256)

		recentPathBuilder = nil
	})

	JustBeforeEach(func() {
		cnsmr = consumer.New(trafficControllerURL, tlsSettings, nil)

		if recentPathBuilder != nil {
			cnsmr.SetRecentPathBuilder(recentPathBuilder)
		}
	})

	AfterEach(func() {
		cnsmr.Close()
		if testServer != nil {
			testServer.Close()
		}
	})

	Describe("ContainerMetrics", func() {
		var handler *HttpHandler

		BeforeEach(func() {
			handler = NewHttpHandler(messagesToSend)
			testServer = httptest.NewServer(handler)
			trafficControllerURL = "ws://" + testServer.Listener.Addr().String()
		})

		It("returns the ContainerMetric values from ContainerEnvelopes", func() {
			env := createContainerMetric(2, 2000)
			messagesToSend <- marshalMessage(env)
			close(messagesToSend)
			envelopes, _ := cnsmr.ContainerEnvelopes(appGuid, authToken)

			messagesToSend = make(chan []byte, 100)
			handler.Messages = messagesToSend
			messagesToSend <- marshalMessage(env)
			close(messagesToSend)
			metrics, err := cnsmr.ContainerMetrics(appGuid, authToken)
			Expect(metrics).To(HaveLen(1))
			Expect(err).ToNot(HaveOccurred())
			Expect(metrics).To(ConsistOf(envelopes[0].ContainerMetric))
		})
	})

	Describe("ContainerEnvelopes", func() {
		var (
			envelopes []*events.Envelope
			err       error
		)

		BeforeEach(func() {
			appGuid = "appGuid"
		})

		JustBeforeEach(func() {
			close(messagesToSend)
			envelopes, err = cnsmr.ContainerEnvelopes(appGuid, authToken)
		})

		Context("when the connection cannot be established", func() {
			BeforeEach(func() {
				trafficControllerURL = "invalid-url"
			})

			It("invalid urls return error", func() {
				Expect(err).ToNot(BeNil())
			})
		})

		Context("when the connection can be established", func() {
			BeforeEach(func() {
				testServer = httptest.NewServer(NewHttpHandler(messagesToSend))
				trafficControllerURL = "ws://" + testServer.Listener.Addr().String()
			})

			Context("with a successful connection", func() {
				BeforeEach(func() {
					messagesToSend <- marshalMessage(createContainerMetric(2, 2000))
				})

				It("returns envelopes from the server", func() {
					Expect(err).NotTo(HaveOccurred())
					Expect(envelopes).To(HaveLen(1))
					Expect(envelopes[0].GetContainerMetric().GetInstanceIndex()).To(Equal(int32(2)))
				})
			})

			Context("when trafficcontroller returns an error as a log message", func() {
				BeforeEach(func() {
					messagesToSend <- marshalMessage(createContainerMetric(2, 2000))
					messagesToSend <- marshalMessage(createMessage("an error occurred", 2000))
				})

				It("returns the error", func() {
					Expect(err).To(HaveOccurred())
					Expect(err).To(MatchError("upstream error: an error occurred"))
				})
			})
		})

		Context("when the content type is missing", func() {
			BeforeEach(func() {
				serverMux := http.NewServeMux()
				serverMux.HandleFunc("/apps/appGuid/containermetrics", func(resp http.ResponseWriter, req *http.Request) {
					resp.Header().Set("Content-Type", "")
					_, err = resp.Write([]byte("OK"))
					Expect(err).ToNot(HaveOccurred())
				})
				testServer = httptest.NewServer(serverMux)
				trafficControllerURL = "ws://" + testServer.Listener.Addr().String()
			})

			It("returns a bad reponse error message", func() {
				Expect(err).To(HaveOccurred())
				Expect(err).To(Equal(consumer.ErrBadResponse))
			})
		})

		Context("when the content length is unknown", func() {
			BeforeEach(func() {
				fakeHandler = &test_helpers.FakeHandler{
					ContentLen: "",
					InputChan:  make(chan []byte, 10),
					GenerateHandler: func(input chan []byte) http.Handler {
						return NewHttpHandler(input)
					},
				}
				testServer = httptest.NewServer(fakeHandler)
				trafficControllerURL = "ws://" + testServer.Listener.Addr().String()

				fakeHandler.InputChan <- marshalMessage(createContainerMetric(2, 2000))
				fakeHandler.Close()
			})

			It("does not throw an error", func() {
				Expect(err).NotTo(HaveOccurred())
				Expect(envelopes).To(HaveLen(1))
			})
		})

		Context("when the content type doesn't have a boundary", func() {
			BeforeEach(func() {

				serverMux := http.NewServeMux()
				serverMux.HandleFunc("/apps/appGuid/containermetrics", func(resp http.ResponseWriter, req *http.Request) {
					_, err = resp.Write([]byte("OK"))
					if err != nil {
						fmt.Printf("error writing response: %s", err)
					}
				})
				testServer = httptest.NewServer(serverMux)
				trafficControllerURL = "ws://" + testServer.Listener.Addr().String()
			})

			It("returns a bad reponse error message", func() {

				Expect(err).To(HaveOccurred())
				Expect(err).To(Equal(consumer.ErrBadResponse))
			})

		})

		Context("when the content type's boundary is blank", func() {
			BeforeEach(func() {

				serverMux := http.NewServeMux()
				serverMux.HandleFunc("/apps/appGuid/containermetrics", func(resp http.ResponseWriter, req *http.Request) {
					resp.Header().Set("Content-Type", "boundary=")
					_, err = resp.Write([]byte("OK"))
					if err != nil {
						fmt.Printf("error writing response: %s", err)
					}
				})
				testServer = httptest.NewServer(serverMux)
				trafficControllerURL = "ws://" + testServer.Listener.Addr().String()
			})

			It("returns a bad reponse error message", func() {
				Expect(err).To(HaveOccurred())
				Expect(err).To(Equal(consumer.ErrBadResponse))
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

				Expect(err).To(HaveOccurred())
				Expect(err).To(Equal(consumer.ErrNotFound))
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

				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(ContainSubstring("You are not authorized. Helpful message"))
				Expect(err).To(BeAssignableToTypeOf(&errors.UnauthorizedError{}))
			})
		})
	})
})
