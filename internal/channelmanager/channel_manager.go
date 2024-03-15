package channelmanager

import (
	"errors"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/wagslane/go-rabbitmq/internal/connectionmanager"
	"github.com/wagslane/go-rabbitmq/internal/dispatcher"
)

// ChannelManager -
type ChannelManager struct {
	channel              *amqp.Channel
	connManager          *connectionmanager.ConnectionManager
	channelMux           *sync.RWMutex
	reconnectInterval    time.Duration
	reconnectionCount    uint
	reconnectionCountMux *sync.Mutex
	dispatcher           *dispatcher.Dispatcher
}

// NewChannelManager creates a new connection manager
func NewChannelManager(connManager *connectionmanager.ConnectionManager, reconnectInterval time.Duration) (*ChannelManager, error) {
	ch, err := getNewChannel(connManager)
	if err != nil {
		return nil, err
	}

	chanManager := ChannelManager{
		connManager:          connManager,
		channel:              ch,
		channelMux:           &sync.RWMutex{},
		reconnectInterval:    reconnectInterval,
		reconnectionCount:    0,
		reconnectionCountMux: &sync.Mutex{},
		dispatcher:           dispatcher.NewDispatcher(),
	}
	go chanManager.startNotifyCancelOrClosed()
	return &chanManager, nil
}

func getNewChannel(connManager *connectionmanager.ConnectionManager) (*amqp.Channel, error) {
	conn := connManager.CheckoutConnection()
	defer connManager.CheckinConnection()

	ch, err := conn.Channel()
	if err != nil {
		return nil, err
	}
	return ch, nil
}

// startNotifyCancelOrClosed listens on the channel's cancelled and closed
// notifiers. When it detects a problem, it attempts to reconnect.
// Once reconnected, it sends an error back on the manager's notifyCancelOrClose
// channel
func (chanManager *ChannelManager) startNotifyCancelOrClosed() {
	notifyCloseChan := chanManager.channel.NotifyClose(make(chan *amqp.Error, 1))
	notifyCancelChan := chanManager.channel.NotifyCancel(make(chan string, 1))

	select {
	case err := <-notifyCloseChan:
		if err != nil {
			chanManager.reconnectLoop()
			chanManager.dispatcher.Dispatch(err)
		}
	case err := <-notifyCancelChan:
		chanManager.reconnectLoop()
		chanManager.dispatcher.Dispatch(errors.New(err))
	}
}

// GetReconnectionCount -
func (chanManager *ChannelManager) GetReconnectionCount() uint {
	chanManager.reconnectionCountMux.Lock()
	defer chanManager.reconnectionCountMux.Unlock()
	return chanManager.reconnectionCount
}

func (chanManager *ChannelManager) incrementReconnectionCount() {
	chanManager.reconnectionCountMux.Lock()
	defer chanManager.reconnectionCountMux.Unlock()
	chanManager.reconnectionCount++
}

// reconnectLoop continuously attempts to reconnect
func (chanManager *ChannelManager) reconnectLoop() {
	for {
		time.Sleep(chanManager.reconnectInterval)
		err := chanManager.reconnect()
		if err == nil {
			chanManager.incrementReconnectionCount()
			go chanManager.startNotifyCancelOrClosed()
			return
		}
	}
}

// reconnect safely closes the current channel and obtains a new one
func (chanManager *ChannelManager) reconnect() error {
	chanManager.channelMux.Lock()
	defer chanManager.channelMux.Unlock()
	newChannel, err := getNewChannel(chanManager.connManager)
	if err != nil {
		return err
	}

	chanManager.channel.Close()

	chanManager.channel = newChannel
	return nil
}

// Close safely closes the current channel and connection
func (chanManager *ChannelManager) Close() error {
	chanManager.channelMux.Lock()
	defer chanManager.channelMux.Unlock()

	err := chanManager.channel.Close()
	if err != nil {
		return err
	}

	return nil
}

// NotifyReconnect adds a new subscriber that will receive error messages whenever
// the connection manager has successfully reconnect to the server
func (chanManager *ChannelManager) NotifyReconnect() (<-chan error, chan<- struct{}) {
	return chanManager.dispatcher.AddSubscriber()
}
