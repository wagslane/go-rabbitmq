package rabbitmq

import (
	"errors"
	"sync"
	"time"

	"github.com/streadway/amqp"
)

type channelManager struct {
	logger              logger
	url                 string
	channel             *amqp.Channel
	channelMux          *sync.RWMutex
	notifyCancelOrClose chan error
}

func newChannelManager(url string, logging bool) (*channelManager, error) {
	ch, err := getNewChannel(url)
	if err != nil {
		return nil, err
	}

	chManager := channelManager{
		logger:              logger{logging: logging},
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
		chManager.logger.Println("attempting to reconnect to amqp server after close")
		chManager.reconnectWithBackoff()
		chManager.logger.Println("successfully reconnected to amqp server after close")
		chManager.notifyCancelOrClose <- err
	case err := <-notifyCancelChan:
		chManager.logger.Println("attempting to reconnect to amqp server after cancel")
		chManager.reconnectWithBackoff()
		chManager.logger.Println("successfully reconnected to amqp server after cancel")
		chManager.notifyCancelOrClose <- errors.New(err)
	}
	close(notifyCancelChan)
	close(notifyCloseChan)
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
