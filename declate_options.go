package rabbitmq

type ExchangeBinding struct {
	From       string
	To         string
	RoutingKey string
	Args       Table
	NoWait     bool
}
