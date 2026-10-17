// Package bench runs a function repeatedly and reports throughput and latency
// percentiles. It is deliberately small: no histograms, every sample is kept
// and sorted, which is fine for runs of a few million calls.
package bench

import (
	"context"
	"fmt"
	"slices"
	"sync"
	"time"
)

type Result struct {
	Ops      int
	Errors   int
	FirstErr error
	Elapsed  time.Duration
	P50      time.Duration
	P95      time.Duration
	P99      time.Duration
	Max      time.Duration
}

func (r Result) TPS() float64 {
	if r.Elapsed == 0 {
		return 0
	}
	return float64(r.Ops) / r.Elapsed.Seconds()
}

func (r Result) String() string {
	s := fmt.Sprintf("ops=%d tps=%.0f p50=%s p95=%s p99=%s max=%s",
		r.Ops, r.TPS(), Round(r.P50), Round(r.P95), Round(r.P99), Round(r.Max))
	if r.Errors > 0 {
		s += fmt.Sprintf(" errors=%d (first: %v)", r.Errors, r.FirstErr)
	}
	return s
}

// Run calls fn from `workers` goroutines for warmup+d. Calls that start
// during warmup are not recorded, and neither is a call that is cut off by
// the end of the run (its error is the context deadline, not a real failure).
func Run(ctx context.Context, workers int, warmup, d time.Duration, fn func(ctx context.Context, worker int) error) Result {
	ctx, cancel := context.WithTimeout(ctx, warmup+d)
	defer cancel()
	measureFrom := time.Now().Add(warmup)

	var (
		mu       sync.Mutex
		all      []time.Duration
		errs     int
		firstErr error
		wg       sync.WaitGroup
	)
	for w := range workers {
		wg.Go(func() {
			var lat []time.Duration
			var werrs int
			var wfirst error
			for ctx.Err() == nil {
				start := time.Now()
				err := fn(ctx, w)
				if ctx.Err() != nil {
					break
				}
				if start.Before(measureFrom) {
					continue
				}
				if err != nil {
					werrs++
					if wfirst == nil {
						wfirst = err
					}
					continue
				}
				lat = append(lat, time.Since(start))
			}
			mu.Lock()
			all = append(all, lat...)
			errs += werrs
			if firstErr == nil {
				firstErr = wfirst
			}
			mu.Unlock()
		})
	}
	wg.Wait()

	r := summarize(all)
	r.Errors, r.FirstErr, r.Elapsed = errs, firstErr, d
	return r
}

// Repeat calls fn n times in a row on the calling goroutine. Use it for
// single-session latency where concurrency would only add noise.
func Repeat(ctx context.Context, n int, fn func(ctx context.Context, i int) error) (Result, error) {
	lat := make([]time.Duration, 0, n)
	begin := time.Now()
	for i := range n {
		start := time.Now()
		if err := fn(ctx, i); err != nil {
			return Result{}, err
		}
		lat = append(lat, time.Since(start))
	}
	r := summarize(lat)
	r.Elapsed = time.Since(begin)
	return r, nil
}

// Median returns the middle value without reordering d.
func Median(d []time.Duration) time.Duration {
	if len(d) == 0 {
		return 0
	}
	s := slices.Clone(d)
	slices.Sort(s)
	return s[len(s)/2]
}

func summarize(lat []time.Duration) Result {
	if len(lat) == 0 {
		return Result{}
	}
	slices.Sort(lat)
	pct := func(p float64) time.Duration { return lat[int(float64(len(lat)-1)*p)] }
	return Result{
		Ops: len(lat),
		P50: pct(0.50),
		P95: pct(0.95),
		P99: pct(0.99),
		Max: lat[len(lat)-1],
	}
}

// Round trims a duration to a precision that suits its size.
func Round(d time.Duration) time.Duration {
	switch {
	case d >= 100*time.Millisecond:
		return d.Round(time.Millisecond)
	case d >= time.Millisecond:
		return d.Round(10 * time.Microsecond)
	default:
		return d.Round(time.Microsecond)
	}
}
