package integration_test

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/streadway/amqp"
	"github.com/stretchr/testify/require"
	"github.com/wagslane/go-rabbitmq"
)

const (
	rabbitmqUser = "rabbitmq"
	rabbitmqPass = "wagslane"
	mgmtUrl      = "http://localhost:15672"
)

var (
	defaultUrl        = fmt.Sprintf("amqp://%s:%s@localhost", rabbitmqUser, rabbitmqPass)
	messageDelay      = 250 * time.Millisecond
	reconnectInterval = 1 * time.Second
)

type TestMessage struct {
	Text string
}

func TestBasic(t *testing.T) {
	require := require.New(t)
	require.True(true)

	consumerConn, err := rabbitmq.NewConn(
		defaultUrl,
		rabbitmq.WithConnectionOptionsLogging,
		rabbitmq.WithConnectionOptionsReconnectInterval(reconnectInterval),
	)
	require.NoError(err)
	defer consumerConn.Close()

	receivedMessages := []rabbitmq.Delivery{}

	exchange := fmt.Sprintf("test-%d", rand.Intn(10000))

	consumer, err := rabbitmq.NewConsumer(
		consumerConn,
		func(d rabbitmq.Delivery) rabbitmq.Action {
			receivedMessages = append(receivedMessages, d)
			return rabbitmq.Ack
		},
		"test",
		rabbitmq.WithConsumerOptionsRoutingKey("test"),
		rabbitmq.WithConsumerOptionsExchangeName(exchange),
		rabbitmq.WithConsumerOptionsExchangeDeclare,
	)
	require.NoError(err)
	defer consumer.Close()

	pusblisherConn, err := rabbitmq.NewConn(
		defaultUrl,
		rabbitmq.WithConnectionOptionsLogging,
		rabbitmq.WithConnectionOptionsReconnectInterval(reconnectInterval),
	)
	require.NoError(err)
	defer pusblisherConn.Close()

	publisher, err := rabbitmq.NewPublisher(
		pusblisherConn,
		rabbitmq.WithPublisherOptionsLogging,
		rabbitmq.WithPublisherOptionsExchangeName(exchange),
		rabbitmq.WithPublisherOptionsExchangeDeclare,
	)
	require.NoError(err)
	defer publisher.Close()

	msg := publishTestMessage(require, publisher)
	publishTestMessage(require, publisher)

	time.Sleep(messageDelay)

	require.Len(receivedMessages, 2)

	var receivedMsg TestMessage

	require.NoError(json.Unmarshal(receivedMessages[0].Body, &receivedMsg))
	require.Equal(msg, receivedMsg)
}

func TestBasicReconnect(t *testing.T) {
	require := require.New(t)
	require.True(true)

	consumerConn, err := rabbitmq.NewConn(
		defaultUrl,
		rabbitmq.WithConnectionOptionsLogging,
		rabbitmq.WithConnectionOptionsReconnectInterval(reconnectInterval),
	)
	require.NoError(err)
	defer consumerConn.Close()

	receivedMessages := []rabbitmq.Delivery{}

	exchange := fmt.Sprintf("test-%d", rand.Intn(10000))

	consumer, err := rabbitmq.NewConsumer(
		consumerConn,
		func(d rabbitmq.Delivery) rabbitmq.Action {
			receivedMessages = append(receivedMessages, d)
			return rabbitmq.Ack
		},
		"test",
		rabbitmq.WithConsumerOptionsRoutingKey("test"),
		rabbitmq.WithConsumerOptionsExchangeName(exchange),
		rabbitmq.WithConsumerOptionsExchangeDeclare,
	)
	require.NoError(err)
	defer consumer.Close()

	pusblisherConn, err := rabbitmq.NewConn(
		defaultUrl,
		rabbitmq.WithConnectionOptionsLogging,
		rabbitmq.WithConnectionOptionsReconnectInterval(reconnectInterval),
	)
	require.NoError(err)
	defer pusblisherConn.Close()

	publisher, err := rabbitmq.NewPublisher(
		pusblisherConn,
		rabbitmq.WithPublisherOptionsLogging,
		rabbitmq.WithPublisherOptionsExchangeName(exchange),
		rabbitmq.WithPublisherOptionsExchangeDeclare,
	)
	require.NoError(err)
	defer publisher.Close()

	waitForConnections(require, 2)

	msg := publishTestMessage(require, publisher)

	time.Sleep(messageDelay)

	terminateConnections(require)

	waitForConnections(require, 2)

	publishTestMessage(require, publisher)

	time.Sleep(messageDelay)

	require.Len(receivedMessages, 2)

	var receivedMsg TestMessage

	require.NoError(json.Unmarshal(receivedMessages[0].Body, &receivedMsg))
	require.Equal(msg, receivedMsg)
}

func TestBasicReconnectBrokenChannel(t *testing.T) {
	require := require.New(t)
	require.True(true)

	consumerConn, err := rabbitmq.NewConn(
		defaultUrl,
		rabbitmq.WithConnectionOptionsLogging,
		rabbitmq.WithConnectionOptionsReconnectInterval(reconnectInterval),
	)
	require.NoError(err)
	defer consumerConn.Close()

	receivedMessages := []rabbitmq.Delivery{}

	exchange := fmt.Sprintf("test-%d", rand.Intn(10000))

	consumer, err := rabbitmq.NewConsumer(
		consumerConn,
		func(d rabbitmq.Delivery) rabbitmq.Action {
			receivedMessages = append(receivedMessages, d)
			return rabbitmq.Ack
		},
		"test",
		rabbitmq.WithConsumerOptionsRoutingKey("test"),
		rabbitmq.WithConsumerOptionsExchangeName(exchange),
		rabbitmq.WithConsumerOptionsExchangeDeclare,
	)
	require.NoError(err)
	defer consumer.Close()

	pusblisherConn, err := rabbitmq.NewConn(
		defaultUrl,
		rabbitmq.WithConnectionOptionsLogging,
		rabbitmq.WithConnectionOptionsReconnectInterval(reconnectInterval),
	)
	require.NoError(err)
	defer pusblisherConn.Close()

	publisher, err := rabbitmq.NewPublisher(
		pusblisherConn,
		rabbitmq.WithPublisherOptionsLogging,
		rabbitmq.WithPublisherOptionsExchangeName(exchange),
		rabbitmq.WithPublisherOptionsExchangeDeclare,
	)
	require.NoError(err)
	defer publisher.Close()

	waitForConnections(require, 2)

	msg := publishTestMessage(require, publisher)

	time.Sleep(messageDelay)

	terminateConnections(require)

	// Simulate a channel failure, but registering the same queue with a different type
	// this will cause an error
	//
	// Delete the queue after some time, which should re-establish the connection
	time.Sleep(reconnectInterval / 2)

	conn, err := amqp.DialConfig(defaultUrl, amqp.Config{})
	require.NoError(err)

	defer conn.Close()

	channel, err := conn.Channel()
	require.NoError(err)

	_, err = channel.QueueDelete("test", false, false, false)
	require.NoError(err)

	_, err = channel.QueueDeclare(
		"test", // name of the queue
		true,   // durable
		false,  // delete when unused
		true,   // exclusive
		false,  // noWait
		nil,    // arguments
	)
	require.NoError(err)

	time.Sleep(reconnectInterval * 2)

	_, err = channel.QueueDelete("test", false, false, false)
	require.NoError(err)

	waitForConnections(require, 2)

	publishTestMessage(require, publisher)

	time.Sleep(messageDelay)

	publishTestMessage(require, publisher)

	time.Sleep(messageDelay)

	require.Len(receivedMessages, 3)

	var receivedMsg TestMessage

	require.NoError(json.Unmarshal(receivedMessages[0].Body, &receivedMsg))
	require.Equal(msg, receivedMsg)
}
