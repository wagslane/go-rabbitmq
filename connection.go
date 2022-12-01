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

	reconnectErrCh, closeCh := manager.NotifyReconnect()
	conn := &Conn{
		connectionManager:          manager,
		reconnectErrCh:             reconnectErrCh,
		closeConnectionToManagerCh: closeCh,
		options:                    *options,
	}

	go conn.handleRestarts()
	return conn, nil
}

func (conn *Conn) handleRestarts() {
	for err := range conn.reconnectErrCh {
		conn.options.Logger.Infof("successful connection recovery from: %v", err)
	}
}

// Close closes the connection, it's not safe for re-use.
// You should also close any consumers and publishers before
// closing the connection
func (conn *Conn) Close() error {
	conn.closeConnectionToManagerCh <- struct{}{}
	return conn.connectionManager.Close()
}
