package connectionmanager

import (
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/wagslane/go-rabbitmq/internal/dispatcher"
)

// ConnectionManager -
type ConnectionManager struct {
	url                  string
	connection           *amqp.Connection
	amqpConfig           amqp.Config
	connectionMux        *sync.RWMutex
	ReconnectInterval    time.Duration
	reconnectionCount    uint
	reconnectionCountMux *sync.Mutex
	dispatcher           *dispatcher.Dispatcher
}

// NewConnectionManager creates a new connection manager
func NewConnectionManager(url string, conf amqp.Config, reconnectInterval time.Duration) (*ConnectionManager, error) {
	conn, err := amqp.DialConfig(url, amqp.Config(conf))
	if err != nil {
		return nil, err
	}
	connManager := ConnectionManager{
		url:                  url,
		connection:           conn,
		amqpConfig:           conf,
		connectionMux:        &sync.RWMutex{},
		ReconnectInterval:    reconnectInterval,
		reconnectionCount:    0,
		reconnectionCountMux: &sync.Mutex{},
		dispatcher:           dispatcher.NewDispatcher(),
	}
	go connManager.startNotifyClose()
	return &connManager, nil
}

// Close safely closes the current channel and connection
func (connManager *ConnectionManager) Close() error {
	connManager.connectionMux.Lock()
	defer connManager.connectionMux.Unlock()

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
	connManager.connectionMux.RLock()
	return connManager.connection
}

// CheckinConnection -
func (connManager *ConnectionManager) CheckinConnection() {
	connManager.connectionMux.RUnlock()
}

// startNotifyCancelOrClosed listens on the channel's cancelled and closed
// notifiers. When it detects a problem, it attempts to reconnect.
// Once reconnected, it sends an error back on the manager's notifyCancelOrClose
// channel
func (connManager *ConnectionManager) startNotifyClose() {
	notifyCloseChan := connManager.connection.NotifyClose(make(chan *amqp.Error, 1))

	err := <-notifyCloseChan
	if err != nil {
		connManager.reconnectLoop()
		connManager.dispatcher.Dispatch(err)
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
		time.Sleep(connManager.ReconnectInterval)
		err := connManager.reconnect()
		if err == nil {
			connManager.incrementReconnectionCount()
			go connManager.startNotifyClose()
			return
		}
	}
}

// reconnect safely closes the current channel and obtains a new one
func (connManager *ConnectionManager) reconnect() error {
	connManager.connectionMux.Lock()
	defer connManager.connectionMux.Unlock()
	newConn, err := amqp.DialConfig(connManager.url, amqp.Config(connManager.amqpConfig))
	if err != nil {
		return err
	}

	connManager.connection.Close()

	connManager.connection = newConn
	return nil
}
