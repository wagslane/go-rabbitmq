package rabbitmq

import (
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/wagslane/go-rabbitmq/internal/connectionmanager"
)

// Conn manages the connection to a rabbit cluster
// it is intended to be shared across publishers and consumers
type Conn struct {
	connectionManager          *connectionmanager.ConnectionManager
	reconnectErrCh             <-chan error
	closeConnectionToManagerCh chan<- struct{}
	notifyReturnChan           chan Return
	notifyPublishChan          chan Confirmation
	options                    ConnectionOptions
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
		notifyReturnChan:           nil,
		notifyPublishChan:          nil,
		options:                    *options,
	}

	go conn.handleRestarts()
	return conn, nil
}

func (conn *Conn) handleRestarts() {
	for err := range conn.reconnectErrCh {
		conn.options.Logger.Infof("successful connection recovery from: %v", err)
		go conn.startNotifyReturnHandler()
		go conn.startNotifyPublishHandler()
	}
}

func (conn *Conn) startNotifyReturnHandler() {
	if conn.notifyReturnChan == nil {
		return
	}
	returnAMQPCh := conn.connectionManager.NotifyReturnSafe(make(chan amqp.Return, 1))
	for ret := range returnAMQPCh {
		conn.notifyReturnChan <- Return{ret}
	}
}

func (conn *Conn) startNotifyPublishHandler() {
	if conn.notifyPublishChan == nil {
		return
	}
	conn.connectionManager.ConfirmSafe(false)
	publishAMQPCh := conn.connectionManager.NotifyPublishSafe(make(chan amqp.Confirmation, 1))
	for conf := range publishAMQPCh {
		conn.notifyPublishChan <- Confirmation{
			Confirmation:      conf,
			ReconnectionCount: int(conn.connectionManager.GetReconnectionCount()),
		}
	}
}

// NotifyReturn registers a listener for basic.return methods.
// These can be sent from the server when a publish is undeliverable either from the mandatory or immediate flags.
// These notifications are shared across an entire connection, so if you're creating multiple
// publishers on the same connection keep that in mind
func (conn *Conn) NotifyReturn() <-chan Return {
	conn.notifyReturnChan = make(chan Return)
	go conn.startNotifyReturnHandler()
	return conn.notifyReturnChan
}

// NotifyPublish registers a listener for publish confirmations, must set ConfirmPublishings option
// These notifications are shared across an entire connection, so if you're creating multiple
// publishers on the same connection keep that in mind
func (conn *Conn) NotifyPublish() <-chan Confirmation {
	conn.notifyPublishChan = make(chan Confirmation)
	go conn.startNotifyPublishHandler()
	return conn.notifyPublishChan
}
