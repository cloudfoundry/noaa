package consumer

import (
	"bufio"
	"crypto/tls"
	"errors"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/cloudfoundry/noaa"
	noaa_errors "github.com/cloudfoundry/noaa/errors"
	"github.com/cloudfoundry/sonde-go/events"
	"github.com/gogo/protobuf/proto"
	"github.com/gorilla/websocket"
)

const reconnectTimeout = 500 * time.Millisecond

//go:generate hel --type DebugPrinter --output mock_debug_printer_test.go

type DebugPrinter interface {
	Print(title, dump string)
}

// Consumer represents the actions that can be performed against traffic controller.
type Consumer struct {
	trafficControllerUrl string
	tlsConfig            *tls.Config
	ws                   *websocket.Conn
	callback             func()
	proxy                func(*http.Request) (*url.URL, error)
	debugPrinter         DebugPrinter
	conLock              sync.RWMutex
	stopped              bool
	stoppedLock          sync.Mutex
}

// NewConsumer creates a new consumer to a traffic controller.
func New(trafficControllerUrl string, tlsConfig *tls.Config, proxy func(*http.Request) (*url.URL, error)) *Consumer {
	return &Consumer{
		trafficControllerUrl: trafficControllerUrl,
		tlsConfig:            tlsConfig,
		proxy:                proxy,
		debugPrinter:         noaa.NullDebugPrinter{},
	}
}

/*
// TailingLogs behaves exactly as TailingLogsWithoutReconnect, except that it retries 5 times if the connection
// to the remote server is lost and returns all errors from each attempt on errorChan.
func (cnsmr *Consumer) TailingLogs(appGuid string, authToken string, outputChan chan<- *events.LogMessage, errorChan chan<- error) {
	action := func() error {
		return cnsmr.TailingLogsWithoutReconnect(appGuid, authToken, outputChan)
	}

	cnsmr.retryAction(action, errorChan)
}
*/
// SetOnConnectCallback sets a callback function to be called with the websocket connection is established.
func (cnsmr *Consumer) SetOnConnectCallback(cb func()) {
	cnsmr.callback = cb
}

// SetDebugPrinter enables logging of the websocket handshake.
func (cnsmr *Consumer) SetDebugPrinter(debugPrinter noaa.DebugPrinter) {
	cnsmr.debugPrinter = debugPrinter
}

// TailingLogsWithoutReconnect listens indefinitely for log messages only; other event types are dropped.
//
// The returned channel of log messages will be closed after an error is returned when
// reading from the connection.  When the connection is closed, the output and error
// channels will be closed.
//
// The returned error channel has a buffer size of 1 so that it can contain the final
// from the connection, or nil if the connection was intentionally closed.
func (cnsmr *Consumer) TailingLogsWithoutReconnect(appGuid string, authToken string) (<-chan *events.LogMessage, <-chan error) {
	outputChan := make(chan *events.LogMessage)
	errChan := make(chan error, 1)
	callback := func(env *events.Envelope) {
		if env.GetEventType() == events.Envelope_LogMessage {
			outputChan <- env.GetLogMessage()
		}
	}

	streamPath := fmt.Sprintf("/apps/%s/stream", appGuid)
	go func() {
		defer close(errChan)
		defer close(outputChan)
		err := cnsmr.stream(streamPath, authToken, callback)
		errChan <- err
	}()

	return outputChan, errChan
}

// Close terminates the websocket connection to traffic controller.
func (cnsmr *Consumer) Close() error {
	cnsmr.conLock.Lock()
	defer cnsmr.conLock.Unlock()
	defer cnsmr.stop()
	if cnsmr.ws == nil {
		return errors.New("connection does not exist")
	}

	cnsmr.ws.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""), time.Time{})
	return cnsmr.ws.Close()
}

func (cnsmr *Consumer) Closed() bool {
	cnsmr.stoppedLock.Lock()
	defer cnsmr.stoppedLock.Unlock()
	return cnsmr.stopped
}

func (cnsmr *Consumer) stop() {
	cnsmr.stoppedLock.Lock()
	defer cnsmr.stoppedLock.Unlock()
	cnsmr.stopped = true
}

func (cnsmr *Consumer) stream(streamPath string, authToken string, callback func(*events.Envelope)) error {
	var err error

	cnsmr.conLock.Lock()
	cnsmr.ws, err = cnsmr.establishWebsocketConnection(streamPath, authToken)
	cnsmr.conLock.Unlock()

	if err != nil {
		return err
	}

	return cnsmr.listenForMessages(callback)
}

func (cnsmr *Consumer) listenForMessages(callback func(*events.Envelope)) error {
	defer cnsmr.ws.Close()

	for {
		_, data, err := cnsmr.ws.ReadMessage()

		// If the connection was closed (i.e. if cnsmr.Close() was called), we
		// will have a non-nil error, but we want to return a nil error.
		if cnsmr.Closed() {
			return nil
		}

		if err != nil {
			return err
		}

		envelope := &events.Envelope{}
		err = proto.Unmarshal(data, envelope)
		if err != nil {
			continue
		}

		callback(envelope)
	}
}

func headersString(header http.Header) string {
	var result string
	for name, values := range header {
		result += name + ": " + strings.Join(values, ", ") + "\n"
	}
	return result
}

func (cnsmr *Consumer) establishWebsocketConnection(path string, authToken string) (*websocket.Conn, error) {
	header := http.Header{"Origin": []string{"http://localhost"}, "Authorization": []string{authToken}}

	dialer := websocket.Dialer{NetDial: cnsmr.proxyDial, TLSClientConfig: cnsmr.tlsConfig}

	url := cnsmr.trafficControllerUrl + path

	cnsmr.debugPrinter.Print("WEBSOCKET REQUEST:",
		"GET "+path+" HTTP/1.1\n"+
			"Host: "+cnsmr.trafficControllerUrl+"\n"+
			"Upgrade: websocket\nConnection: Upgrade\nSec-WebSocket-Version: 13\nSec-WebSocket-Key: [HIDDEN]\n"+
			headersString(header))

	ws, resp, err := dialer.Dial(url, header)

	if resp != nil {
		cnsmr.debugPrinter.Print("WEBSOCKET RESPONSE:",
			resp.Proto+" "+resp.Status+"\n"+
				headersString(resp.Header))
	}

	if resp != nil && resp.StatusCode == http.StatusUnauthorized {
		bodyData, _ := ioutil.ReadAll(resp.Body)
		err = noaa_errors.NewUnauthorizedError(string(bodyData))
		return ws, err
	}

	if err == nil && cnsmr.callback != nil {
		cnsmr.callback()
	}

	if err != nil {

		return nil, errors.New(fmt.Sprintf("Error dialing traffic controller server: %s.\nPlease ask your Cloud Foundry Operator to check the platform configuration (traffic controller is %s).", err.Error(), cnsmr.trafficControllerUrl))
	}

	return ws, err
}

func (cnsmr *Consumer) proxyDial(network, addr string) (net.Conn, error) {
	targetUrl, err := url.Parse("http://" + addr)
	if err != nil {
		return nil, err
	}

	proxy := cnsmr.proxy
	if proxy == nil {
		proxy = http.ProxyFromEnvironment
	}

	proxyUrl, err := proxy(&http.Request{URL: targetUrl})
	if err != nil {
		return nil, err
	}
	if proxyUrl == nil {
		return net.Dial(network, addr)
	}

	proxyConn, err := net.Dial(network, proxyUrl.Host)
	if err != nil {
		return nil, err
	}

	connectReq := &http.Request{
		Method: "CONNECT",
		URL:    targetUrl,
		Host:   targetUrl.Host,
		Header: make(http.Header),
	}
	connectReq.Write(proxyConn)

	connectResp, err := http.ReadResponse(bufio.NewReader(proxyConn), connectReq)
	if err != nil {
		proxyConn.Close()
		return nil, err
	}
	if connectResp.StatusCode != http.StatusOK {
		f := strings.SplitN(connectResp.Status, " ", 2)
		proxyConn.Close()
		return nil, errors.New(f[1])
	}

	return proxyConn, nil
}

func (cnsmr *Consumer) retryAction(action func() error, errorChan chan<- error) {
	reconnectAttempts := 0

	oldConnectCallback := cnsmr.callback
	defer func() { cnsmr.callback = oldConnectCallback }()

	defer close(errorChan)

	cnsmr.callback = func() {
		reconnectAttempts = 0
		if oldConnectCallback != nil {
			oldConnectCallback()
		}
	}

	for ; reconnectAttempts < 5; reconnectAttempts++ {
		if cnsmr.Closed() {
			return
		}

		errorChan <- action()
		time.Sleep(reconnectTimeout)
	}
}
