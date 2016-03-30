package consumer

import (
	"bufio"
	"bytes"
	"crypto/tls"
	"errors"
	"fmt"
	"io/ioutil"
	"mime/multipart"
	"net"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/cloudfoundry/noaa"
	noaa_errors "github.com/cloudfoundry/noaa/errors"
	"github.com/cloudfoundry/sonde-go/events"
	"github.com/gogo/protobuf/proto"
	"github.com/gorilla/websocket"
)

const (
	reconnectTimeout      = 500 * time.Millisecond
	maxRetries       uint = 5
)

var (
	// KeepAlive sets the interval between keep-alive messages sent by the client to loggregator.
	KeepAlive = 25 * time.Second

	boundaryRegexp    = regexp.MustCompile("boundary=(.*)")
	ErrNotOK          = errors.New("unknown issue when making HTTP request to Loggregator")
	ErrNotFound       = ErrNotOK // NotFound isn't an accurate description of how this is used; please use ErrNotOK instead
	ErrBadResponse    = errors.New("bad server response")
	ErrBadRequest     = errors.New("bad client request")
	ErrLostConnection = errors.New("remote server terminated connection unexpectedly")
)

//go:generate hel --type DebugPrinter --output mock_debug_printer_test.go

// DebugPrinter is a type which handles printing debug information.
type DebugPrinter interface {
	Print(title, dump string)
}

// Consumer represents the actions that can be performed against traffic controller.
type Consumer struct {
	trafficControllerUrl string
	tlsConfig            *tls.Config
	idleTimeout          time.Duration
	ws                   *websocket.Conn
	callback             func()
	callbackLock         sync.RWMutex
	proxy                func(*http.Request) (*url.URL, error)
	debugPrinter         DebugPrinter
	conLock              sync.RWMutex
	stopped              bool
	stoppedLock          sync.Mutex
}

// New creates a new consumer to a traffic controller.
func New(trafficControllerUrl string, tlsConfig *tls.Config, proxy func(*http.Request) (*url.URL, error)) *Consumer {
	return &Consumer{
		trafficControllerUrl: trafficControllerUrl,
		tlsConfig:            tlsConfig,
		proxy:                proxy,
		debugPrinter:         noaa.NullDebugPrinter{},
	}
}

// SetOnConnectCallback sets a callback function to be called with the websocket connection
// is established.
func (c *Consumer) SetOnConnectCallback(cb func()) {
	c.callbackLock.Lock()
	defer c.callbackLock.Unlock()
	c.callback = cb
}

// SetDebugPrinter sets the websocket connection to write debug information to debugPrinter.
func (c *Consumer) SetDebugPrinter(debugPrinter noaa.DebugPrinter) {
	c.debugPrinter = debugPrinter
}

// TailingLogsWithoutReconnect listens indefinitely for log messages only; other event
// types are dropped.
//
// The returned channel of log messages will be closed after an error is returned when
// reading from the connection.  When the connection is closed, the output and error
// channels will be closed.
//
// The returned error channel has a buffer size of 1 so that it can contain the final
// error from the connection, or nil if the connection was intentionally closed.
func (c *Consumer) TailingLogsWithoutReconnect(appGuid string, authToken string) (<-chan *events.LogMessage, <-chan error) {
	return c.tailingLogs(appGuid, authToken, 0)
}

// TailingLogs behaves exactly as TailingLogsWithoutReconnect, except that when it
// receives any connection errors it will retry the connection up to 5 times,
// sending all errors that it receives on the returned error channel.
//
// Errors must be drained from the returned error channel for it to continue
// retrying; if they are not drained, the connection attempts will hang.
func (c *Consumer) TailingLogs(appGuid, authToken string) (<-chan *events.LogMessage, <-chan error) {
	return c.tailingLogs(appGuid, authToken, maxRetries)
}

// StreamWithoutReconnect listens indefinitely for all log and event messages.
//
// Messages are presented in the order received from the loggregator server. Chronological or other ordering
// is not guaranteed. It is the responsibility of the consumer of these channels to provide any desired sorting
// mechanism.
func (c *Consumer) StreamWithoutReconnect(appGuid string, authToken string) (<-chan *events.Envelope, <-chan error) {
	return c.runStream(appGuid, authToken, 0)
}

// Stream behaves exactly as StreamWithoutReconnect, except that it retries 5 times if the connection
// to the remote server is lost.
func (c *Consumer) Stream(appGuid string, authToken string) (outputChan <-chan *events.Envelope, errorChan <-chan error) {
	return c.runStream(appGuid, authToken, maxRetries)
}

// Close terminates the websocket connection to traffic controller.  It will return an
// error if it has problems closing the connection.  If there is no connection to close,
// the consumer will be closed so that no further operations will be performed
// (including any pending retries), and a nil error will be returned.
func (c *Consumer) Close() error {
	c.conLock.Lock()
	defer c.conLock.Unlock()
	defer c.stop()
	if c.ws == nil {
		return errors.New("connection does not exist")
	}

	c.ws.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""), time.Time{})
	return c.ws.Close()
}

// RecentLogs connects to traffic controller via its 'recentlogs' http(s) endpoint and returns a slice of recent messages.
// It does not guarantee any order of the messages; they are in the order returned by traffic controller.
//
// The SortRecent method is provided to sort the data returned by this method.
func (c *Consumer) RecentLogs(appGuid string, authToken string) ([]*events.LogMessage, error) {
	envelopes, err := c.readEnvelopesFromTrafficController(appGuid, authToken, "recentlogs")
	if err != nil {
		return nil, err
	}

	messages := make([]*events.LogMessage, 0, 200)
	for _, envelope := range envelopes {
		messages = append(messages, envelope.GetLogMessage())
	}

	return messages, nil
}

// ContainerMetrics connects to traffic controller via its 'containermetrics' http(s) endpoint and returns the most recent messages for an app.
// The returned metrics will be sorted by InstanceIndex.
func (c *Consumer) ContainerMetrics(appGuid string, authToken string) ([]*events.ContainerMetric, error) {
	envelopes, err := c.readEnvelopesFromTrafficController(appGuid, authToken, "containermetrics")

	if err != nil {
		return nil, err
	}

	messages := make([]*events.ContainerMetric, 0, 200)

	for _, envelope := range envelopes {
		if envelope.GetEventType() == events.Envelope_LogMessage {
			return []*events.ContainerMetric{}, errors.New(fmt.Sprintf("Upstream error: %s", envelope.GetLogMessage().GetMessage()))
		}

		messages = append(messages, envelope.GetContainerMetric())
	}

	noaa.SortContainerMetrics(messages)

	return messages, err
}

// Firehose behaves exactly as FirehoseWithoutReconnect, except that it retries 5 times if the connection
// to the remote server is lost.
func (c *Consumer) Firehose(subscriptionId string, authToken string) (<-chan *events.Envelope, <-chan error) {
	return c.firehose(subscriptionId, authToken, 5)
}

func (c *Consumer) SetIdleTimeout(idleTimeout time.Duration) {
	c.idleTimeout = idleTimeout
}

// FirehoseWithoutReconnect streams all data. All clients with the same subscriptionId will receive a proportionate share of the
// message stream. Each pool of clients will receive the entire stream.
//
// If you wish to be able to terminate the listen early, run FirehoseWithoutReconnect in a Goroutine and
// call Close() when you are finished listening.
//
// Messages are presented in the order received from the loggregator server. Chronological or other ordering
// is not guaranteed. It is the responsibility of the consumer of these channels to provide any desired sorting
// mechanism.
func (c *Consumer) FirehoseWithoutReconnect(subscriptionId string, authToken string) (<-chan *events.Envelope, <-chan error) {
	return c.firehose(subscriptionId, authToken, 0)
}

// Closed returns whether or not Close has been called.
func (c *Consumer) Closed() bool {
	c.stoppedLock.Lock()
	defer c.stoppedLock.Unlock()
	return c.stopped
}

func (c *Consumer) readEnvelopesFromTrafficController(appGuid string, authToken string, endpoint string) ([]*events.Envelope, error) {
	trafficControllerUrl, err := url.ParseRequestURI(c.trafficControllerUrl)
	if err != nil {
		return nil, err
	}

	scheme := "https"
	if trafficControllerUrl.Scheme == "ws" {
		scheme = "http"
	}

	recentPath := fmt.Sprintf("%s://%s/apps/%s/%s", scheme, trafficControllerUrl.Host, appGuid, endpoint)
	transport := &http.Transport{Proxy: c.proxy, TLSClientConfig: c.tlsConfig}
	client := &http.Client{Transport: transport}

	req, _ := http.NewRequest("GET", recentPath, nil)
	req.Header.Set("Authorization", authToken)

	resp, err := client.Do(req)
	if err != nil {
		return nil, errors.New(fmt.Sprintf("Error dialing traffic controller server: %s.\nPlease ask your Cloud Foundry Operator to check the platform configuration (traffic controller endpoint is %s).", err.Error(), c.trafficControllerUrl))
	}
	defer resp.Body.Close()

	err = checkForErrors(resp)
	if err != nil {
		return nil, err
	}

	reader, err := getMultipartReader(resp)
	if err != nil {
		return nil, err
	}

	var (
		envelopes []*events.Envelope
		buffer    bytes.Buffer
	)

	for part, loopErr := reader.NextPart(); loopErr == nil; part, loopErr = reader.NextPart() {
		buffer.Reset()

		_, err := buffer.ReadFrom(part)
		if err != nil {
			break
		}

		envelope := new(events.Envelope)
		proto.Unmarshal(buffer.Bytes(), envelope)

		envelopes = append(envelopes, envelope)
	}

	return envelopes, nil
}

func (c *Consumer) onConnectCallback() func() {
	c.callbackLock.RLock()
	defer c.callbackLock.RUnlock()
	return c.callback
}

func (c *Consumer) runStream(appGuid, authToken string, retries uint) (<-chan *events.Envelope, <-chan error) {
	outputs := make(chan *events.Envelope)
	errors := make(chan error, 1)

	callback := func(env *events.Envelope) {
		outputs <- env
	}

	go func() {
		defer close(errors)
		defer close(outputs)
		c.streamAppData(appGuid, authToken, callback, errors, retries)
	}()
	return outputs, errors
}

func (c *Consumer) stop() {
	c.stoppedLock.Lock()
	defer c.stoppedLock.Unlock()
	c.stopped = true
}

func (c *Consumer) streamAppData(appGuid, authToken string, callback func(*events.Envelope), errors chan<- error, retries uint) {
	streamPath := fmt.Sprintf("/apps/%s/stream", appGuid)
	action := func() error {
		return c.stream(streamPath, authToken, callback)
	}
	c.retryAction(action, errors, retries)
}

func (c *Consumer) firehose(subID, authToken string, retries uint) (<-chan *events.Envelope, <-chan error) {
	outputs := make(chan *events.Envelope)
	errors := make(chan error, 1)
	callback := func(env *events.Envelope) {
		outputs <- env
	}

	streamPath := "/firehose/" + subID
	action := func() error {
		return c.stream(streamPath, authToken, callback)
	}
	go func() {
		defer close(errors)
		defer close(outputs)
		c.retryAction(action, errors, retries)
	}()
	return outputs, errors
}

func (c *Consumer) tailingLogs(appGuid, authToken string, retries uint) (<-chan *events.LogMessage, <-chan error) {
	outputs := make(chan *events.LogMessage)
	errors := make(chan error, 1)
	callback := func(env *events.Envelope) {
		if env.GetEventType() == events.Envelope_LogMessage {
			outputs <- env.GetLogMessage()
		}
	}
	go func() {
		defer close(errors)
		defer close(outputs)
		c.streamAppData(appGuid, authToken, callback, errors, retries)
	}()
	return outputs, errors
}

func (c *Consumer) stream(streamPath string, authToken string, callback func(*events.Envelope)) error {
	var err error

	c.conLock.Lock()
	c.ws, err = c.establishWebsocketConnection(streamPath, authToken)
	c.conLock.Unlock()

	if err != nil {
		return err
	}

	return c.listenForMessages(callback)
}

func (c *Consumer) listenForMessages(callback func(*events.Envelope)) error {
	defer c.ws.Close()

	for {
		if c.idleTimeout != 0 {
			c.ws.SetReadDeadline(time.Now().Add(c.idleTimeout))
		}
		_, data, err := c.ws.ReadMessage()

		// If the connection was closed (i.e. if c.Close() was called), we
		// will have a non-nil error, but we want to return a nil error.
		if c.Closed() {
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

func (c *Consumer) establishWebsocketConnection(path string, authToken string) (*websocket.Conn, error) {
	header := http.Header{"Origin": []string{"http://localhost"}, "Authorization": []string{authToken}}

	dialer := websocket.Dialer{NetDial: c.proxyDial, TLSClientConfig: c.tlsConfig}

	url := c.trafficControllerUrl + path

	c.debugPrinter.Print("WEBSOCKET REQUEST:",
		"GET "+path+" HTTP/1.1\n"+
			"Host: "+c.trafficControllerUrl+"\n"+
			"Upgrade: websocket\nConnection: Upgrade\nSec-WebSocket-Version: 13\nSec-WebSocket-Key: [HIDDEN]\n"+
			headersString(header))

	ws, resp, err := dialer.Dial(url, header)

	if resp != nil {
		c.debugPrinter.Print("WEBSOCKET RESPONSE:",
			resp.Proto+" "+resp.Status+"\n"+
				headersString(resp.Header))
	}

	if resp != nil && resp.StatusCode == http.StatusUnauthorized {
		bodyData, _ := ioutil.ReadAll(resp.Body)
		err = noaa_errors.NewUnauthorizedError(string(bodyData))
		return ws, err
	}

	if err == nil && c.callback != nil {
		c.callback()
	}

	if err != nil {

		return nil, errors.New(fmt.Sprintf("Error dialing traffic controller server: %s.\nPlease ask your Cloud Foundry Operator to check the platform configuration (traffic controller is %s).", err.Error(), c.trafficControllerUrl))
	}

	return ws, err
}

func (c *Consumer) proxyDial(network, addr string) (net.Conn, error) {
	targetUrl, err := url.Parse("http://" + addr)
	if err != nil {
		return nil, err
	}

	proxy := c.proxy
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

func (c *Consumer) retryAction(action func() error, errors chan<- error, retries uint) {
	reconnectAttempts := uint(0)

	oldConnectCallback := c.onConnectCallback()
	defer c.SetOnConnectCallback(oldConnectCallback)

	c.callback = func() {
		reconnectAttempts = 0
		if oldConnectCallback != nil {
			oldConnectCallback()
		}
	}

	for ; reconnectAttempts <= retries; reconnectAttempts++ {
		if c.Closed() {
			return
		}

		errors <- action()
		time.Sleep(reconnectTimeout)
	}
}

func headersString(header http.Header) string {
	var result string
	for name, values := range header {
		result += name + ": " + strings.Join(values, ", ") + "\n"
	}
	return result
}

func checkForErrors(resp *http.Response) error {
	if resp.StatusCode == http.StatusUnauthorized {
		data, _ := ioutil.ReadAll(resp.Body)
		return noaa_errors.NewUnauthorizedError(string(data))
	}

	if resp.StatusCode == http.StatusBadRequest {
		return ErrBadRequest
	}

	if resp.StatusCode != http.StatusOK {
		return ErrNotOK
	}
	return nil
}

func getMultipartReader(resp *http.Response) (*multipart.Reader, error) {
	contentType := resp.Header.Get("Content-Type")

	if len(strings.TrimSpace(contentType)) == 0 {
		return nil, ErrBadResponse
	}

	matches := boundaryRegexp.FindStringSubmatch(contentType)

	if len(matches) != 2 || len(strings.TrimSpace(matches[1])) == 0 {
		return nil, ErrBadResponse
	}
	reader := multipart.NewReader(resp.Body, matches[1])
	return reader, nil
}
