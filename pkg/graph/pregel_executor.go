package graph

import "context"

// taskFuture is a minimal future handle for task execution results.
type taskFuture struct {
	resultCh chan pregelTaskResult
}

func newTaskFuture() *taskFuture {
	return &taskFuture{resultCh: make(chan pregelTaskResult, 1)}
}

func (f *taskFuture) resolve(result pregelTaskResult) {
	f.resultCh <- result
	close(f.resultCh)
}

// backgroundExecutor provides futures-style task submission with shared
// cancellation semantics across all in-flight tasks.
type backgroundExecutor struct {
	ctx    context.Context
	cancel context.CancelFunc
	sem    chan struct{}
}

func newBackgroundExecutor(parent context.Context, maxConcurrency int) *backgroundExecutor {
	ctx, cancel := context.WithCancel(parent)
	var sem chan struct{}
	if maxConcurrency > 0 {
		sem = make(chan struct{}, maxConcurrency)
	}
	return &backgroundExecutor{ctx: ctx, cancel: cancel, sem: sem}
}

func (e *backgroundExecutor) submit(run func(context.Context) pregelTaskResult) *taskFuture {
	f := newTaskFuture()
	go func() {
		if e.sem != nil {
			select {
			case e.sem <- struct{}{}:
				defer func() { <-e.sem }()
			case <-e.ctx.Done():
				f.resolve(run(e.ctx))
				return
			}
		}
		result := run(e.ctx)
		if result.err != nil && len(result.interrupts) == 0 {
			e.cancel()
		}
		f.resolve(result)
	}()
	return f
}

func (e *backgroundExecutor) close() {
	e.cancel()
}
