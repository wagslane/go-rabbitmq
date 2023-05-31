package rabbitmq

import (
    "errors"

    "github.com/wagslane/go-rabbitmq/internal/channelmanager"
)

type Channel struct {
    *channelmanager.ChannelManager
}

// NewChannel returns a new channel to the cluster.
func NewChannel(conn *Conn, optionFuncs ...func(*ChannelOptions)) (*Channel, error) {
    defaultOptions := getDefaultChannelOptions()
    options := &defaultOptions
    for _, optionFunc := range optionFuncs {
        optionFunc(options)
    }

    if conn.connectionManager == nil {
        return nil, errors.New("connection manager can't be nil")
    }
    channel, err := channelmanager.NewChannelManager(conn.connectionManager, options.Logger, conn.connectionManager.ReconnectInterval)
    if err != nil {
        return nil, err
    }
    return &Channel{
        channel,
    }, nil
}
