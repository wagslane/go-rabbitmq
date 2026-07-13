package backoff

import (
	"testing"
	"time"
)

func TestDelayGrowsAndCaps(t *testing.T) {
	const base = time.Second

	tests := []struct {
		attempt int
		max     time.Duration
	}{
		{attempt: 0, max: base},
		{attempt: 1, max: 2 * base},
		{attempt: 2, max: 4 * base},
		{attempt: 3, max: 8 * base},
		{attempt: 4, max: 16 * base},
		{attempt: 5, max: 16 * base},
		{attempt: 100, max: 16 * base},
	}

	for _, tt := range tests {
		// jitter is random, so sample repeatedly
		for range 100 {
			got := Delay(base, tt.attempt)
			lower := tt.max * 3 / 4
			if got < lower || got > tt.max {
				t.Fatalf("Delay(base, %d) = %s, want within (%s, %s]", tt.attempt, got, lower, tt.max)
			}
		}
	}
}

func TestDelayZeroBase(t *testing.T) {
	if got := Delay(0, 3); got != 0 {
		t.Fatalf("Delay(0, 3) = %s, want 0", got)
	}
}
