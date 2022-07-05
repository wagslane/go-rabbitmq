package rabbitmq

import "fmt"

// DeclareOptions are used to describe how a new queues, exchanges the routing setup should look like.
type DeclareOptions struct {
	Queue    *QueueOptions
	Exchange *ExchangeOptions
	Bindings []Binding
}

// QueueOptions are used to configure a queue.
// If the Passive flag is set the client will only check if the queue exists on the server
// and that the settings match, no creation attempt will be made.
type QueueOptions struct {
	Name       string
	Durable    bool
	AutoDelete bool
	Exclusive  bool
	NoWait     bool
	Passive    bool // if false, a missing queue will be created on the server
	Args       Table
}

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
}

// BindingOption are used to configure a queue bindings.
type BindingOption struct {
	NoWait bool
	Args   Table
}

// Binding describes a queue binding to a specific exchange.
type Binding struct {
	BindingOption
	QueueName    string
	ExchangeName string
	RoutingKey   string
}

// SetBindings trys to generate bindings for the given routing keys and the queue and exchange options.
// If either Queue or Exchange properties are empty or no queue name is specified, no bindings will be set.
func (o *DeclareOptions) SetBindings(routingKeys []string, opt BindingOption) {
	if o.Queue == nil || o.Exchange == nil {
		return // nothing to set...
	}

	if o.Queue.Name == "" {
		return // nothing to set...
	}

	for _, routingKey := range routingKeys {
		o.Bindings = append(o.Bindings, Binding{
			QueueName:     o.Queue.Name,
			ExchangeName:  o.Exchange.Name,
			RoutingKey:    routingKey,
			BindingOption: opt,
		})
	}
}

// handleDeclare handles the queue, exchange and binding declare process on the server.
// If there are no options set, no actions will be executed.
func handleDeclare(chManager *channelManager, options DeclareOptions) error {
	chManager.channelMux.RLock()
	defer chManager.channelMux.RUnlock()

	// bind queue
	if options.Queue != nil {
		queue := options.Queue
		if queue.Name == "" {
			return fmt.Errorf("missing queue name")
		}
		if queue.Passive {
			_, err := chManager.channel.QueueDeclarePassive(
				queue.Name,
				queue.Durable,
				queue.AutoDelete,
				queue.Exclusive,
				queue.NoWait,
				tableToAMQPTable(queue.Args),
			)
			if err != nil {
				return err
			}
		} else {
			_, err := chManager.channel.QueueDeclare(
				queue.Name,
				queue.Durable,
				queue.AutoDelete,
				queue.Exclusive,
				queue.NoWait,
				tableToAMQPTable(queue.Args),
			)
			if err != nil {
				return err
			}
		}
	}

	// bind exchange
	if options.Exchange != nil {
		exchange := options.Exchange
		if exchange.Name == "" {
			return fmt.Errorf("missing exchange name")
		}
		if exchange.Passive {
			err := chManager.channel.ExchangeDeclarePassive(
				exchange.Name,
				exchange.Kind,
				exchange.Durable,
				exchange.AutoDelete,
				exchange.Internal,
				exchange.NoWait,
				tableToAMQPTable(exchange.Args),
			)
			if err != nil {
				return err
			}
		} else {
			err := chManager.channel.ExchangeDeclare(
				exchange.Name,
				exchange.Kind,
				exchange.Durable,
				exchange.AutoDelete,
				exchange.Internal,
				exchange.NoWait,
				tableToAMQPTable(exchange.Args),
			)
			if err != nil {
				return err
			}
		}
	}

	// handle binding of queues to exchange
	for _, binding := range options.Bindings {
		err := chManager.channel.QueueBind(
			binding.QueueName,              // name of the queue
			binding.RoutingKey,             // bindingKey
			binding.ExchangeName,           // sourceExchange
			binding.NoWait,                 // noWait
			tableToAMQPTable(binding.Args), // arguments
		)
		if err != nil {
			return err
		}
	}

	return nil
}

// getExchangeOptionsOrSetDefault returns pointer to current ExchangeOptions options.
// If no exchange options are set yet, new options with default values will be defined.
func getExchangeOptionsOrSetDefault(options *DeclareOptions) *ExchangeOptions {
	if options.Exchange == nil {
		options.Exchange = &ExchangeOptions{
			Name:       "",
			Kind:       "direct",
			Durable:    false,
			AutoDelete: false,
			Internal:   false,
			NoWait:     false,
			Args:       nil,
			Passive:    false,
		}
	}
	return options.Exchange
}

// getQueueOptionsOrSetDefault returns pointer to current QueueOptions options.
// If no queue options are set yet, new options with default values will be defined.
func getQueueOptionsOrSetDefault(options *DeclareOptions) *QueueOptions {
	if options.Queue == nil {
		options.Queue = &QueueOptions{
			Name:       "",
			Durable:    false,
			AutoDelete: false,
			Exclusive:  false,
			NoWait:     false,
			Passive:    false,
			Args:       nil,
		}
	}
	return options.Queue
}

// region general-options

// WithDeclareQueue sets the queue that should be declared prior to other RabbitMQ actions are being executed.
// Only the settings will be validated if the queue already exists on the server.
// Matching settings will result in no action, different settings will result in an error.
// If the 'Passive' property is set to false, a missing queue will be created on the server.
func WithDeclareQueue(settings *QueueOptions) func(*DeclareOptions) {
	return func(options *DeclareOptions) {
		options.Queue = settings
	}
}

// WithDeclareExchange sets the exchange that should be declared prior to other RabbitMQ actions are being executed.
// Only the settings will be validated if the exchange already exists on the server.
// Matching settings will result in no action, different settings will result in an error.
// If the 'Passive' property is set to false, a missing exchange will be created on the server.
func WithDeclareExchange(settings *ExchangeOptions) func(*DeclareOptions) {
	return func(options *DeclareOptions) {
		options.Exchange = settings
	}
}

// WithDeclareBindings sets the bindings that should be declared prior to other RabbitMQ actions are being executed.
// Only the settings will be validated if one of the bindings already exists on the server.
// Matching settings will result in no action, different settings will result in an error.
// If the 'Passive' property is set to false, missing bindings will be created on the server.
func WithDeclareBindings(bindings []Binding) func(*DeclareOptions) {
	return func(options *DeclareOptions) {
		options.Bindings = bindings
	}
}

// WithDeclareBindingsForRoutingKeys sets the bindings that should be declared prior to other RabbitMQ
// actions are being executed.
// This function must be called after the queue and exchange declaration settings have been set,
// otherwise this function has no effect.
func WithDeclareBindingsForRoutingKeys(routingKeys []string) func(*DeclareOptions) {
	return func(options *DeclareOptions) {
		options.SetBindings(routingKeys, BindingOption{})
	}
}

// endregion general-options

// region single-options

// WithDeclareQueueName returns a function that sets the queue name.
func WithDeclareQueueName(name string) func(*DeclareOptions) {
	return func(options *DeclareOptions) {
		getQueueOptionsOrSetDefault(options).Name = name
	}
}

// WithDeclareQueueDurable sets the queue to durable, which means it won't
// be destroyed when the server restarts. It must only be bound to durable exchanges.
func WithDeclareQueueDurable(options *DeclareOptions) {
	getQueueOptionsOrSetDefault(options).Durable = true
}

// WithDeclareQueueAutoDelete sets the queue to auto delete, which means it will
// be deleted when there are no more consumers on it.
func WithDeclareQueueAutoDelete(options *DeclareOptions) {
	getQueueOptionsOrSetDefault(options).AutoDelete = true
}

// WithDeclareQueueExclusive sets the queue to exclusive, which means
// it's are only accessible by the connection that declares it and
// will be deleted when the connection closes. Channels on other connections
// will receive an error when attempting to declare, bind, consume, purge or
// delete a queue with the same name.
func WithDeclareQueueExclusive(options *DeclareOptions) {
	getQueueOptionsOrSetDefault(options).Exclusive = true
}

// WithDeclareQueueNoWait sets the queue to nowait, which means
// the queue will assume to be declared on the server.  A channel
// exception will arrive if the conditions are met for existing queues
// or attempting to modify an existing queue from a different connection.
func WithDeclareQueueNoWait(options *DeclareOptions) {
	getQueueOptionsOrSetDefault(options).NoWait = true
}

// WithDeclareQueueNoDeclare sets the queue to no declare, which means
// the queue will be assumed to be declared on the server, and thus only will be validated.
func WithDeclareQueueNoDeclare(options *DeclareOptions) {
	getQueueOptionsOrSetDefault(options).Passive = true
}

// WithDeclareQueueArgs returns a function that sets the queue arguments.
func WithDeclareQueueArgs(args Table) func(*DeclareOptions) {
	return func(options *DeclareOptions) {
		getQueueOptionsOrSetDefault(options).Args = args
	}
}

// WithDeclareQueueQuorum sets the queue a quorum type, which means multiple nodes
// in the cluster will have the messages distributed amongst them for higher reliability.
func WithDeclareQueueQuorum(options *DeclareOptions) {
	queue := getQueueOptionsOrSetDefault(options)
	if queue.Args == nil {
		queue.Args = Table{}
	}
	queue.Args["x-queue-type"] = "quorum"
}

// WithDeclareExchangeName returns a function that sets the exchange name.
func WithDeclareExchangeName(name string) func(*DeclareOptions) {
	return func(options *DeclareOptions) {
		getExchangeOptionsOrSetDefault(options).Name = name
	}
}

// WithDeclareExchangeKind returns a function that sets the binding exchange kind/type.
func WithDeclareExchangeKind(kind string) func(*DeclareOptions) {
	return func(options *DeclareOptions) {
		getExchangeOptionsOrSetDefault(options).Kind = kind
	}
}

// WithDeclareExchangeDurable returns a function that sets the binding exchange durable flag.
func WithDeclareExchangeDurable(options *DeclareOptions) {
	getExchangeOptionsOrSetDefault(options).Durable = true
}

// WithDeclareExchangeAutoDelete returns a function that sets the binding exchange autoDelete flag.
func WithDeclareExchangeAutoDelete(options *DeclareOptions) {
	getExchangeOptionsOrSetDefault(options).AutoDelete = true
}

// WithDeclareExchangeInternal returns a function that sets the binding exchange internal flag.
func WithDeclareExchangeInternal(options *DeclareOptions) {
	getExchangeOptionsOrSetDefault(options).Internal = true
}

// WithDeclareExchangeNoWait returns a function that sets the binding exchange noWait flag.
func WithDeclareExchangeNoWait(options *DeclareOptions) {
	getExchangeOptionsOrSetDefault(options).NoWait = true
}

// WithDeclareExchangeArgs returns a function that sets the binding exchange arguments
// that are specific to the server's implementation of the exchange.
func WithDeclareExchangeArgs(args Table) func(*DeclareOptions) {
	return func(options *DeclareOptions) {
		getExchangeOptionsOrSetDefault(options).Args = args
	}
}

// WithDeclareExchangeNoDeclare returns a function that skips the declaration of the
// binding exchange. Use this setting if the exchange already exists and you don't need to declare
// it on consumer start.
func WithDeclareExchangeNoDeclare(options *DeclareOptions) {
	getExchangeOptionsOrSetDefault(options).Passive = true
}

// WithDeclareBindingNoWait sets the bindings to nowait, which means if the queue can not be bound
// the channel will not be closed with an error.
// This function must be called after bindings have been defined, otherwise it has no effect.
func WithDeclareBindingNoWait(options *DeclareOptions) {
	for i := range options.Bindings {
		options.Bindings[i].NoWait = true
	}
}

// WithDeclareBindingArgs sets the arguments of the bindings to args.
// This function must be called after bindings have been defined, otherwise it has no effect.
func WithDeclareBindingArgs(args Table) func(*DeclareOptions) {
	return func(options *DeclareOptions) {
		for i := range options.Bindings {
			options.Bindings[i].Args = args
		}
	}
}

// endregion single-options
