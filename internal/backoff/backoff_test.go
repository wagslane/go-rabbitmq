package backoff

import (
	"math"
	"sync"
	"testing"
	"time"
)

func TestNextStartsAtBase(t *testing.T) {
	b := New(time.Second, time.Minute)
	if got := b.Next(); got != time.Second {
		t.Fatalf("first Next() = %v, want %v", got, time.Second)
	}
}

func TestNextStaysWithinBounds(t *testing.T) {
	base := time.Second
	max := 8 * time.Second
	b := New(base, max)
	for i := 0; i < 100; i++ {
		got := b.Next()
		if got < base || got > max {
			t.Fatalf("Next() = %v, want within [%v, %v]", got, base, max)
		}
	}
}

func TestCeilingDoublesToMax(t *testing.T) {
	b := New(time.Second, 10*time.Second)
	b.Next()
	for i, want := range []time.Duration{2 * time.Second, 4 * time.Second, 8 * time.Second, 10 * time.Second, 10 * time.Second} {
		b.Next()
		if b.ceiling != want {
			t.Fatalf("after attempt %d ceiling = %v, want %v", i+2, b.ceiling, want)
		}
	}
}

func TestResetReturnsToBase(t *testing.T) {
	b := New(time.Second, time.Minute)
	for i := 0; i < 5; i++ {
		b.Next()
	}
	b.Reset()
	if got := b.Next(); got != time.Second {
		t.Fatalf("Next() after Reset() = %v, want %v", got, time.Second)
	}
}

func TestMaxBelowBaseClampsToBase(t *testing.T) {
	b := New(10*time.Second, time.Second)
	for i := 0; i < 5; i++ {
		if got := b.Next(); got != 10*time.Second {
			t.Fatalf("Next() = %v, want %v", got, 10*time.Second)
		}
	}
}

func TestNonPositiveBaseDefaults(t *testing.T) {
	b := New(0, time.Minute)
	if got := b.Next(); got != time.Second {
		t.Fatalf("Next() = %v, want %v", got, time.Second)
	}
}

func TestNextJitters(t *testing.T) {
	b := New(time.Second, time.Hour)
	for i := 0; i < 15; i++ {
		b.Next()
	}
	seen := make(map[time.Duration]struct{})
	for i := 0; i < 20; i++ {
		seen[b.Next()] = struct{}{}
	}
	if len(seen) < 2 {
		t.Fatalf("expected jittered delays at ceiling, got %d distinct values", len(seen))
	}
}

func TestHugeMaxDoesNotOverflow(t *testing.T) {
	base := time.Second
	b := New(base, time.Duration(math.MaxInt64))
	for i := 0; i < 100; i++ {
		if got := b.Next(); got < base {
			t.Fatalf("Next() = %v, want >= %v", got, base)
		}
	}
}

func TestConcurrentUse(t *testing.T) {
	base := time.Millisecond
	max := time.Second
	b := New(base, max)
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 200; j++ {
				if got := b.Next(); got < base || got > max {
					t.Errorf("Next() = %v, want within [%v, %v]", got, base, max)
				}
				if j%10 == 0 {
					b.Reset()
				}
			}
		}()
	}
	wg.Wait()
}
