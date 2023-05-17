package pool

import (
	"context"
	"io"
	"math"
	"time"

	"golang.org/x/sync/semaphore"
)

type ConnectionManager[T io.Closer] struct {
	New                func(context.Context) (T, error)
	CloseErrors        chan<- error
	connectionsLimiter *semaphore.Weighted
	connections        chan Connection[T]
	idleTimeout        time.Duration
}

type Connection[T io.Closer] struct {
	Conn        T
	timer       *time.Timer
	returning   chan<- Connection[T]
	disabled    bool
	idleTimeout time.Duration
}

func NewManager[T io.Closer](maxConnections int64, idleTimeout time.Duration, new func(context.Context) (T, error)) ConnectionManager[T] {
	limiter := semaphore.NewWeighted(maxConnections)
	ret := ConnectionManager[T]{new, nil, limiter, make(chan Connection[T], maxConnections*2), idleTimeout}
	return ret
}

func NewUnlimitedManager[T io.Closer](idleTimeout time.Duration, new func(context.Context) (T, error)) ConnectionManager[T] {
	return ConnectionManager[T]{new, nil, nil, make(chan Connection[T]), idleTimeout}
}

func (m *ConnectionManager[T]) Acquire(ctx context.Context) <-chan uint8 {
	ret := make(chan uint8, 1)
	if m.connectionsLimiter != nil {
		go func() {
			m.connectionsLimiter.Acquire(ctx, 1)
			ret <- 1
		}()
	} else {
		ret <- 1
	}
	return ret
}

func (m *ConnectionManager[T]) Get(ctx context.Context) (*Connection[T], error) {
	select {
	case conn := <-m.connections:
		if conn.disabled {
			return m.Get(ctx)
		}
		return &conn, nil
	default:
		newCtx, cancel := context.WithCancel(ctx)
		select {
		case conn := <-m.connections:
			cancel()
			if conn.disabled {
				return m.Get(ctx)
			}
			return &conn, nil
		case _ = <-m.Acquire(newCtx):
			underlying, err := m.New(ctx)
			if err != nil {
				return nil, err
			}
			connection := &Connection[T]{
				Conn:        underlying,
				timer:       nil,
				returning:   m.connections,
				disabled:    false,
				idleTimeout: m.idleTimeout,
			}
			connection.timer = time.AfterFunc(math.MaxInt64, func() {
				err := connection.Conn.Close()
				if err != nil && m.CloseErrors != nil {
					m.CloseErrors <- err
				}
				connection.disabled = true
				m.connectionsLimiter.Release(1)
			})
			return connection, nil
		}
	}
}

func (c *Connection[T]) Release() {
	c.timer.Reset(c.idleTimeout)
	c.returning <- *c
}
