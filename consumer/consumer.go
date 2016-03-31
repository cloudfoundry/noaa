package consumer

import (
	"crypto/tls"
	"errors"
	"net/http"
	"net/url"
	"regexp"
	"sync"
	"time"

	"github.com/cloudfoundry/noaa"
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
// See sync.go and async.go for traffic controller access methods.
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
	closed               bool
	closedLock           sync.Mutex
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
