package main

import (
	"context"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/fahimbagar/postgres-tuning/internal/pg"
)

// The Postgres pod's cgroup files are visible to the server process, so a
// superuser can read them with pg_read_file. That avoids needing
// metrics-server or kubectl access from inside the Job.

// readAnon returns the "anon" line of memory.stat: private memory of all
// backends. shared_buffers is shmem and not included.
func readAnon(ctx context.Context, q pg.Querier) (int64, error) {
	var stat string
	if err := q.QueryRow(ctx, `SELECT pg_read_file('/sys/fs/cgroup/memory.stat')`).Scan(&stat); err != nil {
		return 0, err
	}
	return cgroupField(stat, "anon"), nil
}

func cgroupField(s, key string) int64 {
	for line := range strings.Lines(s) {
		k, v, ok := strings.Cut(strings.TrimSpace(line), " ")
		if ok && k == key {
			n, _ := strconv.ParseInt(v, 10, 64)
			return n
		}
	}
	return 0
}

// memSampler polls the cgroup every interval and keeps the peak anon value.
// It uses its own connection so it keeps working while the workload pool is
// saturated, and it stops quietly if the server goes away.
type memSampler struct {
	mu   sync.Mutex
	peak int64
	stop context.CancelFunc
	done chan struct{}
}

func startMemSampler(ctx context.Context, q pg.Querier, interval time.Duration) *memSampler {
	ctx, cancel := context.WithCancel(ctx)
	s := &memSampler{stop: cancel, done: make(chan struct{})}
	go func() {
		defer close(s.done)
		t := time.NewTicker(interval)
		defer t.Stop()
		for {
			if anon, err := readAnon(ctx, q); err == nil {
				s.mu.Lock()
				s.peak = max(s.peak, anon)
				s.mu.Unlock()
			}
			select {
			case <-ctx.Done():
				return
			case <-t.C:
			}
		}
	}()
	return s
}

func (s *memSampler) Stop() int64 {
	s.stop()
	<-s.done
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.peak
}

func mib(b int64) string {
	return strconv.FormatInt(b>>20, 10) + " MiB"
}
