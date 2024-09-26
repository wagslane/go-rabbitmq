package connectionmanager

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/makometr/go-rabbitmq/internal/dispatcher"
	"github.com/makometr/go-rabbitmq/internal/logger"
	amqp "github.com/rabbitmq/amqp091-go"
)

// ConnectionManager -
type ConnectionManager struct {
	logger              logger.Logger
	resolver            Resolver
	connection          *amqp.Connection
	amqpConfig          amqp.Config
	connectionMu        *sync.RWMutex
	ReconnectInterval   time.Duration
	reconnectionCount   uint
	reconnectionCountMu *sync.Mutex
	dispatcher          *dispatcher.Dispatcher
}

type Resolver interface {
	Resolve() ([]string, error)
}

// dial will attempt to connect to the a list of urls in the order they are
// given.
func dial(log logger.Logger, resolver Resolver, conf amqp.Config) (*amqp.Connection, error) {
	urls, err := resolver.Resolve()
	if err != nil {
		return nil, fmt.Errorf("error resolving amqp server urls: %w", err)
	}

	var errs []error
	for _, url := range urls {
		conn, err := amqp.DialConfig(url, amqp.Config(conf))
		if err == nil {
			return conn, err
		}
		log.Warnf("failed to connect to amqp server %s: %v", url, err)
		errs = append(errs, err)
	}
	return nil, errors.Join(errs...)
}

// NewConnectionManager creates a new connection manager
func NewConnectionManager(resolver Resolver, conf amqp.Config, log logger.Logger, reconnectInterval time.Duration) (*ConnectionManager, error) {
	conn, err := dial(log, resolver, amqp.Config(conf))
	if err != nil {
		return nil, err
	}

	connManager := ConnectionManager{
		logger:              log,
		resolver:            resolver,
		connection:          conn,
		amqpConfig:          conf,
		connectionMu:        &sync.RWMutex{},
		ReconnectInterval:   reconnectInterval,
		reconnectionCount:   0,
		reconnectionCountMu: &sync.Mutex{},
		dispatcher:          dispatcher.NewDispatcher(),
	}
	go connManager.startNotifyClose()
	return &connManager, nil
}

// Close safely closes the current channel and connection
func (connManager *ConnectionManager) Close() error {
	connManager.logger.Infof("closing connection manager...")
	connManager.connectionMu.Lock()
	defer connManager.connectionMu.Unlock()

	err := connManager.connection.Close()
	if err != nil {
		return err
	}
	return nil
}

// NotifyReconnect adds a new subscriber that will receive error messages whenever
// the connection manager has successfully reconnected to the server
func (connManager *ConnectionManager) NotifyReconnect() (<-chan error, chan<- struct{}) {
	return connManager.dispatcher.AddSubscriber()
}

// CheckoutConnection -
func (connManager *ConnectionManager) CheckoutConnection() *amqp.Connection {
	connManager.connectionMu.RLock()
	return connManager.connection
}

// CheckinConnection -
func (connManager *ConnectionManager) CheckinConnection() {
	connManager.connectionMu.RUnlock()
}

// startNotifyCancelOrClosed listens on the channel's cancelled and closed
// notifiers. When it detects a problem, it attempts to reconnect.
// Once reconnected, it sends an error back on the manager's notifyCancelOrClose
// channel
func (connManager *ConnectionManager) startNotifyClose() {
	notifyCloseChan := connManager.connection.NotifyClose(make(chan *amqp.Error, 1))

	err := <-notifyCloseChan
	if err != nil {
		connManager.logger.Errorf("attempting to reconnect to amqp server after connection close with error: %v", err)
		connManager.reconnectLoop()
		connManager.logger.Warnf("successfully reconnected to amqp server")
		connManager.dispatcher.Dispatch(err)
	}
	if err == nil {
		connManager.logger.Infof("amqp connection closed gracefully")
	}
}

// GetReconnectionCount -
func (connManager *ConnectionManager) GetReconnectionCount() uint {
	connManager.reconnectionCountMu.Lock()
	defer connManager.reconnectionCountMu.Unlock()
	return connManager.reconnectionCount
}

func (connManager *ConnectionManager) incrementReconnectionCount() {
	connManager.reconnectionCountMu.Lock()
	defer connManager.reconnectionCountMu.Unlock()
	connManager.reconnectionCount++
}

// reconnectLoop continuously attempts to reconnect
func (connManager *ConnectionManager) reconnectLoop() {
	for {
		connManager.logger.Infof("waiting %s seconds to attempt to reconnect to amqp server", connManager.ReconnectInterval)
		time.Sleep(connManager.ReconnectInterval)
		err := connManager.reconnect()
		if err != nil {
			connManager.logger.Errorf("error reconnecting to amqp server: %v", err)
		} else {
			connManager.incrementReconnectionCount()
			go connManager.startNotifyClose()
			return
		}
	}
}

// reconnect safely closes the current channel and obtains a new one
func (connManager *ConnectionManager) reconnect() error {
	connManager.connectionMu.Lock()
	defer connManager.connectionMu.Unlock()

	conn, err := dial(connManager.logger, connManager.resolver, amqp.Config(connManager.amqpConfig))
	if err != nil {
		return err
	}

	if err = connManager.connection.Close(); err != nil {
		connManager.logger.Warnf("error closing connection while reconnecting: %v", err)
	}

	connManager.connection = conn
	return nil
}
