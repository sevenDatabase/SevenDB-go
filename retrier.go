package dicedb

import (
	"sync"
	"time"

	"github.com/sevenDatabase/SevenDB-go/wire"
)

type Retrier struct {
	maxRetries  int
	retryWindow time.Duration
	retryCount  int
	lastAttempt time.Time
	mu          sync.Mutex
}

func ExecuteWithResult[T any](r *Retrier, retryOn []wire.ErrKind, op func() (*T, *wire.WireError), beforeRetry func() *wire.WireError) (*T, *wire.WireError) {
	return executeWithRetry(r, retryOn, beforeRetry, op)
}

func ExecuteVoid(r *Retrier, retryOn []wire.ErrKind, op func() *wire.WireError, beforeRetry func() *wire.WireError) *wire.WireError {
	_, err := executeWithRetry(r, retryOn, beforeRetry, func() (*struct{}, *wire.WireError) {
		return nil, op()
	})

	return err
}

func NewRetrier(maxRetries int, retryWindow time.Duration) *Retrier {
	return &Retrier{
		maxRetries:  maxRetries,
		retryWindow: retryWindow,
		lastAttempt: time.Now(),
	}
}

func (r *Retrier) Allow() bool {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.resetIfWindowPassed()
	return r.retryCount < r.maxRetries
}

func (r *Retrier) Failure() {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.resetIfWindowPassed()
	r.retryCount++
	r.lastAttempt = time.Now()
}

func (r *Retrier) Success() {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.retryCount = 0
	r.lastAttempt = time.Now()
}

func (r *Retrier) resetIfWindowPassed() {
	if time.Since(r.lastAttempt) > r.retryWindow {
		r.retryCount = 0
	}
}

func shouldRetry(kind wire.ErrKind, retryOn []wire.ErrKind) bool {
	for _, k := range retryOn {
		if k == kind {
			return true
		}
	}
	return false
}

func executeWithRetry[T any](r *Retrier, retryOn []wire.ErrKind, beforeRetry func() *wire.WireError, op func() (*T, *wire.WireError)) (*T, *wire.WireError) {
	for {
		value, err := op()
		if err == nil {
			r.Success()
			return value, nil
		}

		r.Failure()

		if shouldRetry(err.Kind, retryOn) && r.Allow() {
			if bErr := beforeRetry(); bErr != nil {
				return nil, bErr
			}
			continue
		}

		return nil, err
	}
}
