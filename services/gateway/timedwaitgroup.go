package gateway

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"
)

// timedWaitGroup works like a sync.WaitGroup but with a timeout for the Wait
// function. It allows waiting for a collection of goroutines to finish.
// The main goroutine calls Add to set the number of goroutines to wait for.
// Then, each of the goroutines runs and calls Done when finished.
// At the same time, Wait can be used to block until all goroutines have finished,
// or a timeout occurs.
type timedWaitGroup struct {
	mu              sync.Mutex
	closed          bool          // indicate that the drainer is closed.
	connectionCount atomic.Int64  // Protected by atomic operations
	completeChan    chan struct{} // Closed when all connections are done
	drainingTimeout time.Duration
}

var errDrainerClosed = errors.New("drainer is closed")

// newTimedWaitGroup creates a new TimedWaitGroup.
// The timeout duration specifies the maximum time to wait for Wait.
func newTimedWaitGroup(timeout time.Duration) *timedWaitGroup {
	return &timedWaitGroup{
		drainingTimeout: timeout,
		completeChan:    make(chan struct{}),
	}
}

// Add adds delta to the TimedWaitGroup counter.
// Delta must be a positive integer, or Add will panic.
// It returns an error if Wait has already been called.
func (d *timedWaitGroup) Add(delta int) error {
	if delta < 0 {
		panic("delta must be positive")
	}

	d.mu.Lock()
	defer d.mu.Unlock()

	if d.closed {
		return errDrainerClosed
	}

	d.connectionCount.Add(int64(delta))
	return nil
}

// Done decrements the TimedWaitGroup counter by one.
func (d *timedWaitGroup) Done() {
	newCount := d.connectionCount.Add(-1)

	// If this was the last connection and we're closed, signal completion
	if newCount == 0 {
		d.mu.Lock()
		defer d.mu.Unlock()
		if d.closed {
			d.signalCompletion()
		}
	}
}

// Wait blocks until the TimedWaitGroup counter is zero or the timeout expires.
// When called, it first prevents new additions to the group.
// The behavior of Wait depends on the timeout value set on creation:
//   - Positive timeout: waits for the specified duration.
//   - Zero timeout: returns immediately.
//   - Negative timeout: waits indefinitely until the counter is zero or the context is cancelled.
//
// It returns context.DeadlineExceeded if the timeout is reached,
// or the context's error if the context is cancelled. It returns nil on successful completion.
func (d *timedWaitGroup) Wait(ctx context.Context) error {
	// Close the wait group. Calls to Add() will return an error.
	d.mu.Lock()
	d.closed = true
	d.mu.Unlock()
	if d.connectionCount.Load() == 0 || d.drainingTimeout == 0 {
		d.signalCompletion()
		return nil
	}

	var ctxShutdown context.Context
	if d.drainingTimeout > 0 {
		// Wait with timeout
		var cancel context.CancelFunc
		ctxShutdown, cancel = context.WithTimeout(ctx, d.drainingTimeout)
		defer cancel()
	} else {
		ctxShutdown = ctx
	}

	// Wait for the completion.
	select {
	case <-d.completeChan:
		// All connections completed
		return nil
	case <-ctxShutdown.Done():
		// Context cancelled or timed out
		return ctxShutdown.Err()
	}
}

// signalCompletion safely closes the completeChan to signal that all tasks are done.
func (d *timedWaitGroup) signalCompletion() {
	select {
	case <-d.completeChan:
		// Already closed
	default:
		close(d.completeChan)
	}
}

// GetCount returns the current value of the counter.
func (d *timedWaitGroup) GetCount() int64 {
	return d.connectionCount.Load()
}
