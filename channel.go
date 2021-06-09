package rabbitmq

import (
	"errors"
	"sync"
	"time"

	"github.com/streadway/amqp"
)

type channelManager struct {
	logger              Logger
	url                 string
	channel             *amqp.Channel
	connection          *amqp.Connection
	config              amqp.Config
	channelMux          *sync.RWMutex
	notifyCancelOrClose chan error
}

func newChannelManager(url string, conf amqp.Config, log Logger) (*channelManager, error) {
	conn, ch, err := getNewChannel(url, conf)
	if err != nil {
		return nil, err
	}

	chManager := channelManager{
		logger:              log,
		url:                 url,
		connection:          conn,
		channel:             ch,
		channelMux:          &sync.RWMutex{},
		notifyCancelOrClose: make(chan error),
	}
	go chManager.startNotifyCancelOrClosed()
	return &chManager, nil
}

func getNewChannel(url string, conf amqp.Config) (*amqp.Connection, *amqp.Channel, error) {
	amqpConn, err := amqp.DialConfig(url, conf)
	if err != nil {
		return nil, nil, err
	}
	ch, err := amqpConn.Channel()
	if err != nil {
		return nil, nil, err
	}
	return amqpConn, ch, err
}

// startNotifyCancelOrClosed listens on the channel's cancelled and closed
// notifiers. When it detects a problem, it attempts to reconnect with an exponential
// backoff. Once reconnected, it sends an error back on the manager's notifyCancelOrClose
// channel
func (chManager *channelManager) startNotifyCancelOrClosed() {
	notifyCloseChan := make(chan *amqp.Error)
	notifyCloseChan = chManager.channel.NotifyClose(notifyCloseChan)
	notifyCancelChan := make(chan string)
	notifyCancelChan = chManager.channel.NotifyCancel(notifyCancelChan)
	select {
	case err := <-notifyCloseChan:
		// If the connection close is triggered by the Server, a reconnection takes place
		if err != nil && err.Server {
			chManager.logger.Printf("attempting to reconnect to amqp server after close")
			chManager.reconnectWithBackoff()
			chManager.logger.Printf("successfully reconnected to amqp server after close")
			chManager.notifyCancelOrClose <- err
		}
	case err := <-notifyCancelChan:
		chManager.logger.Printf("attempting to reconnect to amqp server after cancel")
		chManager.reconnectWithBackoff()
		chManager.logger.Printf("successfully reconnected to amqp server after cancel")
		chManager.notifyCancelOrClose <- errors.New(err)
	}

	// these channels can be closed by amqp
	select {
	case <-notifyCloseChan:
	default:
		close(notifyCloseChan)
	}
	select {
	case <-notifyCancelChan:
	default:
		close(notifyCancelChan)
	}
}

// reconnectWithBackoff continuously attempts to reconnect with an
// exponential backoff strategy
func (chManager *channelManager) reconnectWithBackoff() {
	backoffTime := time.Second
	for {
		chManager.logger.Printf("waiting %s seconds to attempt to reconnect to amqp server", backoffTime)
		time.Sleep(backoffTime)
		backoffTime *= 2
		err := chManager.reconnect()
		if err != nil {
			chManager.logger.Printf("error reconnecting to amqp server: %v", err)
		} else {
			return
		}
	}
}

// reconnect safely closes the current channel and obtains a new one
func (chManager *channelManager) reconnect() error {
	chManager.channelMux.Lock()
	defer chManager.channelMux.Unlock()
	newConn, newChannel, err := getNewChannel(chManager.url, chManager.config)
	if err != nil {
		return err
	}

	chManager.channel.Close()
	chManager.connection.Close()

	chManager.connection = newConn
	chManager.channel = newChannel
	go chManager.startNotifyCancelOrClosed()
	return nil
}
