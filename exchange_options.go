package rabbitmq

// ExchangeOptions are used to configure an exchange.
// If the Passive flag is set the client will only check if the exchange exists on the server
// and that the settings match, no creation attempt will be made.
type ExchangeOptions struct {
	Name       string
	Kind       string // possible values: empty string for default exchange or direct, topic, fanout
	Durable    bool
	AutoDelete bool
	Internal   bool
	NoWait     bool
	Passive    bool // if false, a missing exchange will be created on the server
	Args       Table
	Declare    bool
}
