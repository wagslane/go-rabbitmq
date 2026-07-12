package backoff

import (
	"math/rand/v2"
	"time"
)

// Delay returns how long to wait before reconnect attempt number attempt
// (0-indexed): the base interval doubled per attempt, capped at 16x base,
// with up to 25% subtracted as jitter so a fleet of clients doesn't
// reconnect in lockstep after a broker restart.
func Delay(base time.Duration, attempt int) time.Duration {
	if base <= 0 {
		return 0
	}
	delay := base << min(attempt, 4)
	jitter := time.Duration(rand.Int64N(int64(delay)/4 + 1))
	return delay - jitter
}
