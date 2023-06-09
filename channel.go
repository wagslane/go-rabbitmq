package rabbitmq

import (
	"errors"
	"fmt"

	"github.com/xmapst/go-rabbitmq/internal/channelmanager"
)

// NewChannel returns a new channel to the cluster.
func NewChannel(conn *Conn, optionFuncs ...func(*ChannelOptions)) (*channelmanager.ChannelManager, error) {
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
	err = channel.QosSafe(options.QOSPrefetch, 0, options.QOSGlobal)
	if err != nil {
		_ = channel.Close()
		return nil, fmt.Errorf("declare qos failed: %w", err)
	}
	return channel, nil
}
