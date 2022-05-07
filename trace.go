// Copyright 2020 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package minitrace

import (
	"context"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

type Trace struct {
	TraceID uint64
	Spans   []Span
}

var (
	rds [20]struct {
		sync.Mutex
		*rand.Rand
	}
	idx int64  = 0
	id  uint64 = 0
)

func init() {
	seedBase := time.Now().UnixNano()
	for i := 0; i < 20; i++ {
		rds[i] = struct {
			sync.Mutex
			*rand.Rand
		}{Rand: rand.New(rand.NewSource(seedBase + int64(i)))}
	}
}

const (
	LowBitMask     = 0b1111
	LowToHighShift = 60
)

var ids = make([]uint64, 16)

// Returns random uint64 ID.
func nextID(shardID uint64) uint64 {
	shardID = shardID & LowBitMask
	lower := atomic.AddUint64(&ids[shardID], 1)
	return (shardID << LowToHighShift) | lower
}

func StartRootSpan(ctx context.Context, event string, traceID uint64, parentSpanID uint64, attachment interface{}) (context.Context, TraceHandle) {
	ctx = context.WithValue(ctx, spanContextBatchKey, &spanContextBatchKeyContent{
		buf: make([]spanContext, InitSpanLen),
		idx: 0,
	})
	traceCtx := newTraceContext(traceID, attachment)
	spanCtx := newSpanContext(ctx, traceCtx)
	spanHandle := newSpanHandle(spanCtx, parentSpanID, event)
	return spanCtx, TraceHandle{spanHandle}
}

func StartSpanWithContext(ctx context.Context, event string) (context.Context, SpanHandle) {
	handle := StartSpan(ctx, event)
	if !handle.finished {
		return handle.spanContext, handle
	}
	return ctx, handle
}

func StartSpan(ctx context.Context, event string) (handle SpanHandle) {
	var parentCtx context.Context
	var parentSpanCtx *spanContext

	if s, ok := ctx.(*spanContext); ok {
		// Fold spanContext to reduce the depth of context tree.
		parentCtx = s.parent
		parentSpanCtx = s
	} else if s, ok := ctx.Value(activeTraceKey).(*spanContext); ok {
		parentCtx = ctx
		parentSpanCtx = s
	} else {
		handle.finished = true
		return
	}

	traceCtx := parentSpanCtx.traceContext
	spanCtx := newSpanContext(parentCtx, traceCtx)
	return newSpanHandle(spanCtx, parentSpanCtx.spanID, event)
}

func CurrentID(ctx context.Context) (spanID uint64, traceID uint64, ok bool) {
	if s, ok := ctx.Value(activeTraceKey).(*spanContext); ok {
		return s.spanID, s.traceContext.traceID, ok
	}

	return
}

func AccessAttachment(ctx context.Context, fn func(attachment interface{})) (ok bool) {
	spanCtx, ok := ctx.Value(activeTraceKey).(*spanContext)
	if !ok {
		return false
	}
	return spanCtx.traceContext.accessAttachment(fn)
}

type SpanHandle struct {
	spanContext *spanContext
	span        Span
	finished    bool
}

func newSpanHandle(spanCtx *spanContext, parentSpanID uint64, event string) (sh SpanHandle) {
	sh.spanContext = spanCtx
	sh.span.beginWith(parentSpanID, event)
	sh.finished = false
	spanCtx.spanID = sh.span.ID
	return
}

func (sh *SpanHandle) AddProperty(key, value string) {
	if sh.finished {
		return
	}
	sh.span.addProperty(key, value)
}

func (sh *SpanHandle) AccessAttachment(fn func(attachment interface{})) {
	if sh.finished {
		return
	}
	sh.spanContext.traceContext.accessAttachment(fn)
}

func (sh *SpanHandle) Finish() {
	if sh.finished {
		return
	}
	sh.finished = true

	traceCtx := sh.spanContext.traceContext
	sh.span.endWith(traceCtx)
	traceCtx.pushSpan(&sh.span)
}

func (sh *SpanHandle) TraceID() uint64 {
	return sh.spanContext.traceContext.traceID
}

type TraceHandle struct {
	SpanHandle
}

func (th *TraceHandle) Collect() (trace Trace, attachment interface{}) {
	th.SpanHandle.Finish()
	return th.spanContext.traceContext.collect()
}
