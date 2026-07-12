package rabbitmq

import "testing"

// Regression for #206. WithConsumerOptionsQueueArgs used to overwrite
// the entire args map, so any args set by an earlier option (e.g.
// WithConsumerOptionsQueueQuorum or WithConsumerOptionsQueueMessageExpiration)
// silently disappeared depending on option order.
func TestWithConsumerOptionsQueueArgs_MergesWithExisting(t *testing.T) {
	opts := &ConsumerOptions{}

	WithConsumerOptionsQueueQuorum(opts)
	if got := opts.QueueOptions.Args["x-queue-type"]; got != "quorum" {
		t.Fatalf("quorum arg = %v, want %q", got, "quorum")
	}

	WithConsumerOptionsQueueArgs(Table{
		"x-delivery-limit": -1,
		"x-message-ttl":    6000,
	})(opts)

	if got := opts.QueueOptions.Args["x-queue-type"]; got != "quorum" {
		t.Errorf("x-queue-type dropped after merge: got %v", got)
	}
	if got := opts.QueueOptions.Args["x-delivery-limit"]; got != -1 {
		t.Errorf("x-delivery-limit = %v, want %v", got, -1)
	}
	if got := opts.QueueOptions.Args["x-message-ttl"]; got != 6000 {
		t.Errorf("x-message-ttl = %v, want %v", got, 6000)
	}
}

func TestWithConsumerOptionsQueueArgs_MergesBeforeQuorum(t *testing.T) {
	opts := &ConsumerOptions{}

	WithConsumerOptionsQueueArgs(Table{"x-delivery-limit": -1})(opts)
	WithConsumerOptionsQueueQuorum(opts)

	if got := opts.QueueOptions.Args["x-delivery-limit"]; got != -1 {
		t.Errorf("x-delivery-limit = %v, want %v", got, -1)
	}
	if got := opts.QueueOptions.Args["x-queue-type"]; got != "quorum" {
		t.Errorf("x-queue-type = %v, want %q", got, "quorum")
	}
}

func TestWithConsumerOptionsQueueArgs_LaterValueWins(t *testing.T) {
	opts := &ConsumerOptions{}

	WithConsumerOptionsQueueArgs(Table{"x-message-ttl": 6000})(opts)
	WithConsumerOptionsQueueArgs(Table{"x-message-ttl": 7000})(opts)

	if got := opts.QueueOptions.Args["x-message-ttl"]; got != 7000 {
		t.Errorf("x-message-ttl = %v, want %v", got, 7000)
	}
}

func TestWithPublisherOptionsExchangeArgs_MergesWithExisting(t *testing.T) {
	opts := &PublisherOptions{}
	opts.ExchangeOptions.Args = Table{"x-existing": "keep"}

	WithPublisherOptionsExchangeArgs(Table{"x-new": "added"})(opts)

	if got := opts.ExchangeOptions.Args["x-existing"]; got != "keep" {
		t.Errorf("x-existing dropped: got %v", got)
	}
	if got := opts.ExchangeOptions.Args["x-new"]; got != "added" {
		t.Errorf("x-new = %v, want %v", got, "added")
	}
}

func TestWithPublisherOptionsExchangeArgs_LaterValueWins(t *testing.T) {
	opts := &PublisherOptions{}

	WithPublisherOptionsExchangeArgs(Table{"alternate-exchange": "first"})(opts)
	WithPublisherOptionsExchangeArgs(Table{"alternate-exchange": "second"})(opts)

	if got := opts.ExchangeOptions.Args["alternate-exchange"]; got != "second" {
		t.Errorf("alternate-exchange = %v, want %q", got, "second")
	}
}
