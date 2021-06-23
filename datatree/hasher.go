package datatree

import (
	"runtime"
	"sync"
	"sync/atomic"

	sha256 "github.com/minio/sha256-simd"
)

const (
	MinimumJobsInGoroutine = 20
	MaximumGoroutines      = 16
)

func hash(in []byte) []byte {
	h := sha256.New()
	_, _ = h.Write(in)
	return h.Sum(nil)
}

func hash2(level byte, a, b []byte) []byte {
	h := sha256.New()
	_, _ = h.Write([]byte{level})
	_, _ = h.Write(a)
	_, _ = h.Write(b)
	return h.Sum(nil)
}

type hashJob struct {
	target []byte
	level  byte
	srcA   []byte
	srcB   []byte
}

func (job hashJob) run() {
	h := sha256.New()
	_, _ = h.Write([]byte{job.level})
	_, _ = h.Write(job.srcA)
	_, _ = h.Write(job.srcB)
	copy(job.target, h.Sum(nil))
}

type Hasher struct {
	jobs []hashJob
}

func (h *Hasher) Add(level byte, target, srcA, srcB []byte) {
	h.jobs = append(h.jobs, hashJob{target, level, srcA, srcB})
}

func (h *Hasher) Run() {
	if len(h.jobs) < MinimumJobsInGoroutine {
		for _, job := range h.jobs {
			job.run()
		}
	}
	sharedIdx := int64(-1)
	ParallelRun(runtime.NumCPU(), func(workerID int) {
		for {
			myIdx := atomic.AddInt64(&sharedIdx, 1)
			if myIdx >= int64(len(h.jobs)) {
				return
			}
			h.jobs[myIdx].run()
		}
	})
	h.jobs = h.jobs[:0]
}

func ParallelRun(workerCount int, fn func(workerID int)) {
	var wg sync.WaitGroup
	wg.Add(workerCount)
	for i := 0; i < workerCount; i++ {
		go func(i int) {
			fn(i)
			wg.Done()
		}(i)
	}
	wg.Wait()
}
