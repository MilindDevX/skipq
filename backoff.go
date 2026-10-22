package skipq

import (
	"math/rand/v2"
	"time"
)

// DefaultBackoff is the retry delay used by Pool unless PoolConfig.Backoff is
// set: 1 s after the first failure, doubling up to a 1 h cap.
func DefaultBackoff(attempt int) time.Duration {
	return Backoff(attempt, time.Second, time.Hour)
}

// Backoff returns how long to wait before retrying a job whose attempt-th run
// (1-based) failed. The ceiling doubles each attempt from base up to maxDelay,
// and the result is picked at random from [ceiling/2, ceiling].
//
// The randomness ("jitter") matters: if 1,000 jobs fail together because a
// dependency went down, fixed delays would retry all 1,000 at the same
// instant and knock it over again. Keeping half the delay fixed still
// guarantees the wait grows with each attempt.
func Backoff(attempt int, base, maxDelay time.Duration) time.Duration {
	ceiling := base
	for i := 1; i < attempt && ceiling < maxDelay; i++ {
		ceiling *= 2
	}
	ceiling = min(ceiling, maxDelay)
	half := ceiling / 2
	return half + rand.N(ceiling-half+1)
}
