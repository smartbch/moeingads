package datatree

import (
	"sync"
	"sync/atomic"
	"runtime"

	sha256 "github.com/minio/sha256-simd"
)

const (
	MinimumJobsInGoroutine = 20
	MaximumGoroutines      = 16
)

func hash(in []byte) []byte {
	h := sha256.New()
	h.Write(in)
	return h.Sum(nil)
}

func hash2(level byte, a, b []byte) []byte {
	h := sha256.New()
	h.Write([]byte{level})
	h.Write(a)
	h.Write(b)
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
	h.Write([]byte{job.level})
	h.Write(job.srcA)
	h.Write(job.srcB)
	copy(job.target, h.Sum(nil))
}

type Hasher struct {
	jobs []hashJob
	//wg   sync.WaitGroup
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
	//stripe := MinimumJobsInGoroutine
	//if stripe*MaximumGoroutines < len(h.jobs) {
	//	stripe = len(h.jobs) / MaximumGoroutines
	//	if len(h.jobs)%MaximumGoroutines != 0 {
	//		stripe++
	//	}
	//}
	//for start := 0; start < len(h.jobs); start += stripe {
	//	end := start + stripe
	//	if end > len(h.jobs) {
	//		end = len(h.jobs)
	//	}
	//	h.wg.Add(1)
	//	go func(start, end int) { //copy start and end to prevent race condition
	//		for _, job := range h.jobs[start:end] {
	//			job.run()
	//		}
	//		h.wg.Done()
	//	}(start, end)
	//}
	//h.wg.Wait()
	sharedIdx := int64(-1)
	ParrallelRun(runtime.NumCPU(), func(workerID int) {
		for {
			myIdx := atomic.AddInt64(&sharedIdx, 1)
			if myIdx >= int64(len(h.jobs)) {return}
			h.jobs[myIdx].run()
		}
	})
	h.jobs = h.jobs[:0]
}

func ParrallelRun(workerCount int, fn func(workerID int)) {
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

