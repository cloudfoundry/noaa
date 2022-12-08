package consumer_test

import (
	"bytes"
	"errors"
	"log"
	"net/http"
	"net/http/httptest"
	"net/url"
	"time"

	"github.com/cloudfoundry/noaa/v2/consumer"
	"github.com/cloudfoundry/sonde-go/events"
	"github.com/elazarl/goproxy"
	"github.com/elazarl/goproxy/ext/auth"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Consumer connecting through a Proxy", func() {
	var (
		connection *consumer.Consumer

		messagesToSend chan []byte
		testServer     *httptest.Server
		endpoint       string
		proxy          func(*http.Request) (*url.URL, error)

		testProxyServer *httptest.Server
		goProxyHandler  *goproxy.ProxyHttpServer
	)

	BeforeEach(func() {
		messagesToSend = make(chan []byte, 256)
		testServer = httptest.NewServer(NewWebsocketHandler(messagesToSend, 100*time.Millisecond))
		endpoint = "ws://" + testServer.Listener.Addr().String()

		goProxyHandler = goproxy.NewProxyHttpServer()
		goProxyHandler.Logger = log.New(bytes.NewBufferString(""), "", 0)
		testProxyServer = httptest.NewServer(goProxyHandler)
		u, err := url.Parse(testProxyServer.URL)
		proxy = func(*http.Request) (*url.URL, error) {
			return u, err
		}
	})

	JustBeforeEach(func() {
		connection = consumer.New(endpoint, nil, proxy)
	})

	AfterEach(func() {
		testProxyServer.Close()
		testServer.Close()
	})

	Describe("StreamWithoutReconnect", func() {
		var (
			incoming <-chan *events.Envelope
			errs     <-chan error
		)

		JustBeforeEach(func() {
			incoming, errs = connection.StreamWithoutReconnect("fakeAppGuid", "authToken")
		})

		AfterEach(func() {
			close(messagesToSend)
		})

		Context("with a message in the trafficcontroller", func() {
			BeforeEach(func() {
				messagesToSend <- marshalMessage(createMessage("hello", 0))
			})

			It("connects using valid URL to running proxy server", func() {
				message := <-incoming
				Expect(message.GetLogMessage().GetMessage()).To(Equal([]byte("hello")))
			})
		})

		Context("with an auth proxy server", func() {
			BeforeEach(func() {
				goProxyHandler.OnRequest().HandleConnect(auth.BasicConnect("my_realm", func(user, passwd string) bool {
					return user == "user" && passwd == "password"
				}))
				proxyURL, err := url.Parse(testProxyServer.URL)
				proxy = func(*http.Request) (*url.URL, error) {
					proxyURL.User = url.UserPassword("user", "password")
					if err != nil {
						return nil, err
					}

					return proxyURL, nil
				}
				messagesToSend <- marshalMessage(createMessage("hello", 0))
			})

			It("connects successfully", func() {
				message := <-incoming
				Expect(message.GetLogMessage().GetMessage()).To(Equal([]byte("hello")))
			})
		})

		Context("with an auth proxy server with bad credential", func() {
			BeforeEach(func() {
				goProxyHandler.OnRequest().HandleConnect(auth.BasicConnect("my_realm", func(user, passwd string) bool {
					return user == "user" && passwd == "password"
				}))
				proxyURL, err := url.Parse(testProxyServer.URL)
				proxy = func(*http.Request) (*url.URL, error) {
					proxyURL.User = url.UserPassword("user", "passwrd")
					if err != nil {
						return nil, err
					}

					return proxyURL, nil
				}
			})

			It("connects successfully", func() {
				var err error
				Eventually(errs).Should(Receive(&err))
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(ContainSubstring("Proxy Authentication Required"))
			})
		})

		Context("with a closed proxy server", func() {
			BeforeEach(func() {
				testProxyServer.Close()
			})

			It("sends a connection refused error", func() {
				Eventually(func() string {
					err := <-errs
					return err.Error()
				}).Should(ContainSubstring("connection refused"))
			})
		})

		Context("with a proxy that returns errors", func() {
			const errMsg = "Invalid proxy URL"

			BeforeEach(func() {
				proxy = func(*http.Request) (*url.URL, error) {
					return nil, errors.New(errMsg)
				}
			})

			It("sends the errors", func() {
				var err error
				Eventually(errs).Should(Receive(&err))
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(ContainSubstring(errMsg))
			})
		})

		Context("with a proxy that rejects connections", func() {
			BeforeEach(func() {
				goProxyHandler.OnRequest().HandleConnect(goproxy.AlwaysReject)
			})

			It("sends a dialing error", func() {
				var err error
				Eventually(errs).Should(Receive(&err))
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(ContainSubstring("Error dialing trafficcontroller server"))
			})
		})

		Context("with a non-proxy server", func() {
			BeforeEach(func() {
				nonProxyServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					http.Error(w, "Go away, I am not a proxy!", http.StatusBadRequest)
				}))
				proxy = func(*http.Request) (*url.URL, error) {
					return url.Parse(nonProxyServer.URL)
				}
			})

			It("sends a bad request error", func() {
				var err error
				Eventually(errs).Should(Receive(&err))
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(ContainSubstring(http.StatusText(http.StatusBadRequest)))
			})
		})
	})
})
