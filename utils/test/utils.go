package test

import "sync"

// RunConcurrently runs a function multiple times concurrently and return when all calls complete
func RunConcurrently(times int, fn func(int)) {
	wg := &sync.WaitGroup{}
	for i := 0; i < times; i++ {
		wg.Add(1)
		go func(t int) { defer wg.Done(); fn(t) }(i)
	}
	wg.Wait()
}
