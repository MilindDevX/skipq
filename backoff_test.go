package skipq_test

import (
	"testing"
	"time"

	"github.com/MilindDevX/skipq"
)

func TestBackoffGrowsExponentiallyWithJitterAndCap(t *testing.T) {
	t.Parallel()
	base, maxDelay := time.Second, time.Hour

	ceiling := base
	for attempt := 1; attempt <= 30; attempt++ {
		lo, hi := ceiling/2, ceiling
		seen := map[time.Duration]bool{}
		for range 200 {
			d := skipq.Backoff(attempt, base, maxDelay)
			if d < lo || d > hi {
				t.Fatalf("attempt %d: delay %v outside [%v, %v]", attempt, d, lo, hi)
			}
			seen[d] = true
		}
		if len(seen) < 2 {
			t.Fatalf("attempt %d: no jitter, always %v", attempt, lo)
		}
		ceiling = min(ceiling*2, maxDelay)
	}
}

func TestBackoffDoesNotOverflowForHugeAttempts(t *testing.T) {
	t.Parallel()
	if d := skipq.Backoff(10_000, time.Second, time.Hour); d < 30*time.Minute || d > time.Hour {
		t.Fatalf("delay %v should be capped near an hour", d)
	}
}
