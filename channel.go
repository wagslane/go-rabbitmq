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
	channelMux          *sync.RWMutex
	notifyCancelOrClose chan error
}

func newChannelManager(url string, log Logger) (*channelManager, error) {
	ch, err := getNewChannel(url)
	if err != nil {
		return nil, err
	}

	chManager := channelManager{
		logger:              log,
		url:                 url,
		channel:             ch,
		channelMux:          &sync.RWMutex{},
		notifyCancelOrClose: make(chan error),
	}
	go chManager.startNotifyCancelOrClosed()
	return &chManager, nil
}

func getNewChannel(url string) (*amqp.Channel, error) {
	amqpConn, err := amqp.Dial(url)
	if err != nil {
		return nil, err
	}
	ch, err := amqpConn.Channel()
	if err != nil {
		return nil, err
	}
	return ch, err
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
		chManager.logger.Printf("attempting to reconnect to amqp server after close")
		chManager.reconnectWithBackoff()
		chManager.logger.Printf("successfully reconnected to amqp server after close")
		chManager.notifyCancelOrClose <- err
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
	newChannel, err := getNewChannel(chManager.url)
	if err != nil {
		return err
	}
	chManager.channel.Close()
	chManager.channel = newChannel
	go chManager.startNotifyCancelOrClosed()
	return nil
}
