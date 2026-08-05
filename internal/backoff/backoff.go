package backoff

import (
	"math/rand/v2"
	"sync"
	"time"
)

// Exponential produces delays that grow exponentially from a base interval to
// a maximum, with full jitter: each delay is drawn uniformly from
// [base, ceiling], where the ceiling doubles per attempt until it reaches max.
// It is safe for concurrent use.
type Exponential struct {
	base time.Duration
	max  time.Duration

	mu      sync.Mutex
	ceiling time.Duration
}

// New returns an Exponential backoff over [base, max]. A non-positive base
// defaults to one second; a max below base is raised to base.
func New(base, max time.Duration) *Exponential {
	if base <= 0 {
		base = time.Second
	}
	if max < base {
		max = base
	}
	return &Exponential{base: base, max: max}
}

// Next returns the delay to wait before the next attempt.
func (e *Exponential) Next() time.Duration {
	e.mu.Lock()
	defer e.mu.Unlock()
	switch {
	case e.ceiling == 0:
		e.ceiling = e.base
	case e.ceiling >= e.max/2:
		e.ceiling = e.max
	default:
		e.ceiling *= 2
	}
	return e.base + rand.N(e.ceiling-e.base+1)
}

// Reset restores the initial state so the next delay equals base.
func (e *Exponential) Reset() {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.ceiling = 0
}
