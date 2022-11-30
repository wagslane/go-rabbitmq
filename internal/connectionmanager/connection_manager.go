package connectionmanager

import (
	"errors"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/wagslane/go-rabbitmq/internal/logger"
)

// ConnectionManager -
type ConnectionManager struct {
	logger               logger.Logger
	url                  string
	channel              *amqp.Channel
	connection           *amqp.Connection
	amqpConfig           amqp.Config
	channelMux           *sync.RWMutex
	reconnectInterval    time.Duration
	reconnectionCount    uint
	reconnectionCountMux *sync.Mutex
	dispatcher           *dispatcher
}

// NewConnectionManager creates a new connection manager
func NewConnectionManager(url string, conf amqp.Config, log logger.Logger, reconnectInterval time.Duration) (*ConnectionManager, error) {
	conn, ch, err := getNewChannel(url, conf)
	if err != nil {
		return nil, err
	}

	connManager := ConnectionManager{
		logger:               log,
		url:                  url,
		connection:           conn,
		channel:              ch,
		channelMux:           &sync.RWMutex{},
		amqpConfig:           conf,
		reconnectInterval:    reconnectInterval,
		reconnectionCount:    0,
		reconnectionCountMux: &sync.Mutex{},
		dispatcher:           newDispatcher(),
	}
	go connManager.startNotifyCancelOrClosed()
	return &connManager, nil
}

func getNewChannel(url string, conf amqp.Config) (*amqp.Connection, *amqp.Channel, error) {
	amqpConn, err := amqp.DialConfig(url, amqp.Config(conf))
	if err != nil {
		return nil, nil, err
	}
	ch, err := amqpConn.Channel()
	if err != nil {
		return nil, nil, err
	}
	return amqpConn, ch, nil
}

// startNotifyCancelOrClosed listens on the channel's cancelled and closed
// notifiers. When it detects a problem, it attempts to reconnect.
// Once reconnected, it sends an error back on the manager's notifyCancelOrClose
// channel
func (connManager *ConnectionManager) startNotifyCancelOrClosed() {
	notifyCloseChan := connManager.channel.NotifyClose(make(chan *amqp.Error, 1))
	notifyCancelChan := connManager.channel.NotifyCancel(make(chan string, 1))
	select {
	case err := <-notifyCloseChan:
		if err != nil {
			connManager.logger.Errorf("attempting to reconnect to amqp server after close with error: %v", err)
			connManager.reconnectLoop()
			connManager.logger.Warnf("successfully reconnected to amqp server")
			connManager.dispatcher.dispatch(err)
		}
		if err == nil {
			connManager.logger.Infof("amqp channel closed gracefully")
		}
	case err := <-notifyCancelChan:
		connManager.logger.Errorf("attempting to reconnect to amqp server after cancel with error: %s", err)
		connManager.reconnectLoop()
		connManager.logger.Warnf("successfully reconnected to amqp server after cancel")
		connManager.dispatcher.dispatch(errors.New(err))
	}
}

// GetReconnectionCount -
func (connManager *ConnectionManager) GetReconnectionCount() uint {
	connManager.reconnectionCountMux.Lock()
	defer connManager.reconnectionCountMux.Unlock()
	return connManager.reconnectionCount
}

func (connManager *ConnectionManager) incrementReconnectionCount() {
	connManager.reconnectionCountMux.Lock()
	defer connManager.reconnectionCountMux.Unlock()
	connManager.reconnectionCount++
}

// reconnectLoop continuously attempts to reconnect
func (connManager *ConnectionManager) reconnectLoop() {
	for {
		connManager.logger.Infof("waiting %s seconds to attempt to reconnect to amqp server", connManager.reconnectInterval)
		time.Sleep(connManager.reconnectInterval)
		err := connManager.reconnect()
		if err != nil {
			connManager.logger.Errorf("error reconnecting to amqp server: %v", err)
		} else {
			connManager.incrementReconnectionCount()
			go connManager.startNotifyCancelOrClosed()
			return
		}
	}
}

// reconnect safely closes the current channel and obtains a new one
func (connManager *ConnectionManager) reconnect() error {
	connManager.channelMux.Lock()
	defer connManager.channelMux.Unlock()
	newConn, newChannel, err := getNewChannel(connManager.url, connManager.amqpConfig)
	if err != nil {
		return err
	}

	if err = connManager.channel.Close(); err != nil {
		connManager.logger.Warnf("error closing channel while reconnecting: %v", err)
	}

	if err = connManager.connection.Close(); err != nil {
		connManager.logger.Warnf("error closing connection while reconnecting: %v", err)
	}

	connManager.connection = newConn
	connManager.channel = newChannel
	return nil
}

// close safely closes the current channel and connection
func (connManager *ConnectionManager) close() error {
	connManager.logger.Infof("closing connection manager...")
	connManager.channelMux.Lock()
	defer connManager.channelMux.Unlock()

	err := connManager.channel.Close()
	if err != nil {
		return err
	}

	err = connManager.connection.Close()
	if err != nil {
		return err
	}
	return nil
}

// NotifyReconnect adds a new subscriber that will receive error messages whenever
// the connection manager has successfully reconnect to the server
func (connManager *ConnectionManager) NotifyReconnect() (<-chan error, chan<- struct{}) {
	return connManager.dispatcher.addSubscriber()
}
