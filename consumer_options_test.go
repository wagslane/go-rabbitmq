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
