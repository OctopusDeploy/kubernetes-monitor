package cluster

import (
	"context"
	"errors"
)

var errStopped = errors.New("the owner of this state has stopped")

// mailbox runs operations on the single goroutine that owns some state, one at a time, so the state needs
// no locks. ask reads a value from the state, do changes it, and call changes it when the change can fail;
// all three wait for the owner to finish.
type mailbox[S any] struct {
	ops     chan func(*S)
	stopped <-chan struct{}
}

func newMailbox[S any](stopped <-chan struct{}) mailbox[S] {
	return mailbox[S]{ops: make(chan func(*S)), stopped: stopped}
}

func (m mailbox[S]) serve(state *S) {
	for {
		select {
		case <-m.stopped:
			return
		case op := <-m.ops:
			op(state)
		}
	}
}

// ask runs f on the owner's goroutine. Once the owner accepts f it always runs it to completion, so ctx only
// bounds the wait to be accepted.
func ask[S, R any](ctx context.Context, m mailbox[S], f func(*S) R) (R, error) {
	result := make(chan R, 1)
	select {
	case m.ops <- func(state *S) { result <- f(state) }:
		return <-result, nil
	case <-ctx.Done():
		var zero R
		return zero, ctx.Err()
	case <-m.stopped:
		var zero R
		return zero, errStopped
	}
}

// do returns only the reason f couldn't run.
func do[S any](ctx context.Context, m mailbox[S], f func(*S)) error {
	_, err := ask(ctx, m, func(state *S) struct{} {
		f(state)
		return struct{}{}
	})
	return err
}

// call returns f's error, or the reason f couldn't run.
func call[S any](ctx context.Context, m mailbox[S], f func(*S) error) error {
	err, sendErr := ask(ctx, m, f)
	if sendErr != nil {
		return sendErr
	}
	return err
}
