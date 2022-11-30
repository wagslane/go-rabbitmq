package rabbitmq

import (
	"sync"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/wagslane/go-rabbitmq/internal/connectionmanager"
)

// Conn manages the connection to a rabbit cluster
// it is intended to be shared across publishers and consumers
type Conn struct {
	connectionManager          *connectionmanager.ConnectionManager
	reconnectErrCh             <-chan error
	closeConnectionToManagerCh chan<- struct{}

	handlerMux           *sync.Mutex
	notifyReturnHandler  func(r Return)
	notifyPublishHandler func(p Confirmation)

	options ConnectionOptions
}

// Config wraps amqp.Config
// Config is used in DialConfig and Open to specify the desired tuning
// parameters used during a connection open handshake.  The negotiated tuning
// will be stored in the returned connection's Config field.
type Config amqp.Config

// NewConn creates a new connection manager
func NewConn(url string, optionFuncs ...func(*ConnectionOptions)) (*Conn, error) {
	defaultOptions := getDefaultConnectionOptions()
	options := &defaultOptions
	for _, optionFunc := range optionFuncs {
		optionFunc(options)
	}

	manager, err := connectionmanager.NewConnectionManager(url, amqp.Config(options.Config), options.Logger, options.ReconnectInterval)
	if err != nil {
		return nil, err
	}

	err = manager.QosSafe(
		options.QOSPrefetch,
		0,
		options.QOSGlobal,
	)
	if err != nil {
		return nil, err
	}

	reconnectErrCh, closeCh := manager.NotifyReconnect()
	conn := &Conn{
		connectionManager:          manager,
		reconnectErrCh:             reconnectErrCh,
		closeConnectionToManagerCh: closeCh,
		handlerMux:                 &sync.Mutex{},
		notifyReturnHandler:        nil,
		notifyPublishHandler:       nil,
		options:                    *options,
	}

	go conn.handleRestarts()
	return conn, nil
}

func (conn *Conn) handleRestarts() {
	for err := range conn.reconnectErrCh {
		conn.options.Logger.Infof("successful connection recovery from: %v", err)
		go conn.startReturnHandler()
		go conn.startPublishHandler()
	}
}

// NotifyReturn registers a listener for basic.return methods.
// These can be sent from the server when a publish is undeliverable either from the mandatory or immediate flags.
// These notifications are shared across an entire connection, so if you're creating multiple
// publishers on the same connection keep that in mind
func (conn *Conn) NotifyReturn(handler func(r Return)) {
	conn.handlerMux.Lock()
	conn.notifyReturnHandler = handler
	conn.handlerMux.Unlock()

	go conn.startReturnHandler()
}

// NotifyPublish registers a listener for publish confirmations, must set ConfirmPublishings option
// These notifications are shared across an entire connection, so if you're creating multiple
// publishers on the same connection keep that in mind
func (conn *Conn) NotifyPublish(handler func(p Confirmation)) {
	conn.handlerMux.Lock()
	conn.notifyPublishHandler = handler
	conn.handlerMux.Unlock()

	go conn.startPublishHandler()
}

func (conn *Conn) startReturnHandler() {
	conn.handlerMux.Lock()
	if conn.notifyReturnHandler == nil {
		return
	}
	conn.handlerMux.Unlock()

	returns := conn.connectionManager.NotifyReturnSafe(make(chan amqp.Return, 1))
	for ret := range returns {
		go conn.notifyReturnHandler(Return{ret})
	}
}

func (conn *Conn) startPublishHandler() {
	conn.handlerMux.Lock()
	if conn.notifyPublishHandler == nil {
		return
	}
	conn.handlerMux.Unlock()

	conn.connectionManager.ConfirmSafe(false)
	confirmationCh := conn.connectionManager.NotifyPublishSafe(make(chan amqp.Confirmation, 1))
	for conf := range confirmationCh {
		go conn.notifyPublishHandler(Confirmation{
			Confirmation:      conf,
			ReconnectionCount: int(conn.connectionManager.GetReconnectionCount()),
		})
	}
}

// Close closes the connection, it's not safe for re-use.
// You should also close any consumers and publishers before
// closing the connection
func (conn *Conn) Close() error {
	conn.closeConnectionToManagerCh <- struct{}{}
	return conn.connectionManager.Close()
}
