package noaa

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
	"time"

	"code.google.com/p/gogoprotobuf/proto"
	noaa_errors "github.com/cloudfoundry/noaa/errors"
	"github.com/cloudfoundry/noaa/events"
	"github.com/gorilla/websocket"
)

var (
	// KeepAlive sets the interval between keep-alive messages sent by the client to loggregator.
	KeepAlive         = 25 * time.Second
	boundaryRegexp    = regexp.MustCompile("boundary=(.*)")
	ErrNotFound       = errors.New("/recent path not found or has issues")
	ErrBadResponse    = errors.New("bad server response")
	ErrBadRequest     = errors.New("bad client request")
	ErrLostConnection = errors.New("remote server terminated connection unexpectedly")
)

// Consumer represents the actions that can be performed against traffic controller.
type Consumer struct {
	trafficControllerUrl string
	tlsConfig            *tls.Config
	ws                   *websocket.Conn
	callback             func()
	proxy                func(*http.Request) (*url.URL, error)
	debugPrinter         DebugPrinter
}

// NewConsumer creates a new consumer to a traffic controller.
func NewConsumer(trafficControllerUrl string, tlsConfig *tls.Config, proxy func(*http.Request) (*url.URL, error)) *Consumer {
	return &Consumer{trafficControllerUrl: trafficControllerUrl, tlsConfig: tlsConfig, proxy: proxy, debugPrinter: nullDebugPrinter{}}
}

// TailingLogs listens indefinitely for log messages. It returns two channels; the first is populated
// with log messages, while the second contains errors (e.g. from parsing messages). It returns
// immediately. Call Close() to terminate the connection when you are finished listening.
//
// Messages are presented in the order received from the loggregator server. Chronological or
// other ordering is not guaranteed. It is the responsibility of the consumer of these channels
// to provide any desired sorting mechanism.
func (cnsmr *Consumer) TailingLogs(appGuid string, authToken string) (<-chan *events.LogMessage, <-chan *events.Error) {
	logMessages := make(chan *events.LogMessage)
	errChan := make(chan *events.Error, 1)

	streamPath := fmt.Sprintf("/apps/%s/stream", appGuid)
	eventsWithMetrics := cnsmr.stream(streamPath, authToken)

	go func() {
		defer close(logMessages)
		defer close(errChan)

		for event := range eventsWithMetrics {
			switch *event.EventType {
			case events.Envelope_LogMessage:
				logMessages <- event.GetLogMessage()
			case events.Envelope_Error:
				errChan <- event.GetError()
				return
			}
		}
	}()

	return logMessages, errChan
}

// Stream listens indefinitely for log and event messages. It returns two channels; the first is populated
// with log and event messages, while the second contains errors (e.g. from parsing messages). It returns immediately.
// Call Close() to terminate the connection when you are finished listening.
//
// Messages are presented in the order received from the loggregator server. Chronological or other ordering
// is not guaranteed. It is the responsibility of the consumer of these channels to provide any desired sorting
// mechanism.
func (cnsmr *Consumer) Stream(appGuid string, authToken string) <-chan *events.Envelope {
	streamPath := fmt.Sprintf("/apps/%s/stream", appGuid)
	return cnsmr.stream(streamPath, authToken)
}

// Firehose streams all data. All clients with the same subscriptionId will receive a proportionate share of the
// message stream. Each pool of clients will receive the entire stream.
func (cnsmr *Consumer) Firehose(subscriptionId string, authToken string) <-chan *events.Envelope {
	streamPath := "/firehose/" + subscriptionId
	return cnsmr.stream(streamPath, authToken)
}

func (cnsmr *Consumer) stream(streamPath string, authToken string) <-chan *events.Envelope {
	incomingChan := make(chan *events.Envelope, 1)
	var err error

	cnsmr.ws, err = cnsmr.establishWebsocketConnection(streamPath, authToken)

	if err != nil {
		incomingChan <- makeError(err, noaa_errors.ERR_DIAL)
		close(incomingChan)
		return incomingChan
	}

	go func() {
		defer close(incomingChan)

		err := cnsmr.listenForMessages(incomingChan)
		if err != nil {
			incomingChan <- makeError(err, noaa_errors.ERR_LOST_CONNECTION)
		}
	}()

	return incomingChan
}

func makeError(err error, code int32) *events.Envelope {
	return &events.Envelope{
		EventType: events.Envelope_Error.Enum(),
		Error: &events.Error{
			Source:  proto.String("NOAA"),
			Code:    &code,
			Message: proto.String(err.Error()),
		},
	}
}

// RecentLogs connects to traffic controller via its 'recentlogs' http(s) endpoint and returns a slice of recent messages.
// It does not guarantee any order of the messages; they are in the order returned by traffic controller.
//
// The SortRecent method is provided to sort the data returned by this method.
func (cnsmr *Consumer) RecentLogs(appGuid string, authToken string) ([]*events.LogMessage, error) {
	trafficControllerUrl, err := url.ParseRequestURI(cnsmr.trafficControllerUrl)
	if err != nil {
		return nil, err
	}

	scheme := "https"

	if trafficControllerUrl.Scheme == "ws" {
		scheme = "http"
	}

	recentPath := fmt.Sprintf("%s://%s/apps/%s/recentlogs", scheme, trafficControllerUrl.Host, appGuid)
	transport := &http.Transport{Proxy: cnsmr.proxy, TLSClientConfig: cnsmr.tlsConfig}
	client := &http.Client{Transport: transport}

	req, _ := http.NewRequest("GET", recentPath, nil)
	req.Header.Set("Authorization", authToken)

	resp, err := client.Do(req)
	if err != nil {
		return nil, errors.New(fmt.Sprintf("Error dialing traffic controller server: %s.\nPlease ask your Cloud Foundry Operator to check the platform configuration (traffic controller endpoint is %s).", err.Error(), cnsmr.trafficControllerUrl))
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusUnauthorized {
		data, _ := ioutil.ReadAll(resp.Body)
		return nil, noaa_errors.NewUnauthorizedError(string(data))
	}

	if resp.StatusCode == http.StatusBadRequest {
		return nil, ErrBadRequest
	}

	if resp.StatusCode != http.StatusOK {
		return nil, ErrNotFound
	}

	contentType := resp.Header.Get("Content-Type")

	if len(strings.TrimSpace(contentType)) == 0 {
		return nil, ErrBadResponse
	}

	matches := boundaryRegexp.FindStringSubmatch(contentType)

	if len(matches) != 2 || len(strings.TrimSpace(matches[1])) == 0 {
		return nil, ErrBadResponse
	}

	reader := multipart.NewReader(resp.Body, matches[1])

	var buffer bytes.Buffer
	messages := make([]*events.LogMessage, 0, 200)

	for part, loopErr := reader.NextPart(); loopErr == nil; part, loopErr = reader.NextPart() {
		buffer.Reset()

		msg := new(events.Envelope)
		_, err := buffer.ReadFrom(part)
		if err != nil {
			break
		}
		proto.Unmarshal(buffer.Bytes(), msg)
		messages = append(messages, msg.GetLogMessage())
	}

	return messages, err
}

// Close terminates the websocket connection to traffic controller.
func (cnsmr *Consumer) Close() error {
	if cnsmr.ws == nil {
		return errors.New("connection does not exist")
	}

	return cnsmr.ws.Close()
}

// SetOnConnectCallback sets a callback function to be called with the websocket connection is established.
func (cnsmr *Consumer) SetOnConnectCallback(cb func()) {
	cnsmr.callback = cb
}

// SetDebugPrinter enables logging of the websocket handshake.
func (cnsmr *Consumer) SetDebugPrinter(debugPrinter DebugPrinter) {
	cnsmr.debugPrinter = debugPrinter
}

func (cnsmr *Consumer) listenForMessages(msgChan chan<- *events.Envelope) error {
	defer cnsmr.ws.Close()

	for {
		_, data, err := cnsmr.ws.ReadMessage()
		if err != nil {
			return err
		}

		envelope := &events.Envelope{}
		err = proto.Unmarshal(data, envelope)
		if err != nil {
			continue
		}

		msgChan <- envelope
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
