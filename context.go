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
	"sync"
	"sync/atomic"
	"time"
)

const (
	InitSpanLen = 16
)

type spanContextBatchKeyType struct{}
type spanContextBatchKeyContent struct {
	buf []spanContext
	idx uint64
}

var (
	spanContextBatchKey = struct{}{}
)

// A span context embedded into ctx.context
type spanContext struct {
	parent context.Context

	// Shared trace context
	traceContext *traceContext
	spanID       uint64
}

func newSpanContext(ctx context.Context, tracingCtx *traceContext) *spanContext {
	var spanCtx *spanContext
	if i := ctx.Value(spanContextBatchKey); i != nil {
		b := i.(*spanContextBatchKeyContent)
		idx := int(atomic.AddUint64(&b.idx, 1) - 1)
		if idx < len(b.buf) {
			spanCtx = &b.buf[idx]
			spanCtx.parent = ctx
			spanCtx.traceContext = tracingCtx
		} else {
			spanCtx = &spanContext{
				parent:       ctx,
				traceContext: tracingCtx,
			}
		}
	}
	return spanCtx
}

type tracingKey struct{}

var activeTraceKey = tracingKey{}

func (s *spanContext) Deadline() (deadline time.Time, ok bool) {
	return s.parent.Deadline()
}

func (s *spanContext) Done() <-chan struct{} {
	return s.parent.Done()
}

func (s *spanContext) Err() error {
	return s.parent.Err()
}

func (s *spanContext) Value(key interface{}) interface{} {
	if key == activeTraceKey {
		return s
	} else {
		return s.parent.Value(key)
	}
}

type traceContext struct {
	/// Frozen fields
	traceID          uint64
	createUnixTimeNs uint64
	createMonoTimeNs uint64

	/// Shared mutable fields
	mu             sync.Mutex
	collectedSpans []Span
	attachment     interface{}
	collected      bool
}

func newTraceContext(traceID uint64, attachment interface{}) *traceContext {
	return &traceContext{
		traceID:          traceID,
		createUnixTimeNs: unixtimeNs(),
		createMonoTimeNs: monotimeNs(),
		attachment:       attachment,
		collected:        false,
		collectedSpans:   make([]Span, 0, 8),
	}
}

func (tc *traceContext) accessAttachment(fn func(attachment interface{})) (ok bool) {
	tc.mu.Lock()
	defer tc.mu.Unlock()

	if tc.collected {
		return false
	}

	fn(tc.attachment)
	return true
}

func (tc *traceContext) pushSpan(span *Span) (ok bool) {
	tc.mu.Lock()
	defer tc.mu.Unlock()

	if tc.collected {
		return false
	}

	tc.collectedSpans = append(tc.collectedSpans, *span)
	return true
}

func (tc *traceContext) collect() (trace Trace, attachment interface{}) {
	tc.mu.Lock()
	defer tc.mu.Unlock()

	if tc.collected {
		return
	}
	tc.collected = true

	trace.Spans = tc.collectedSpans
	trace.TraceID = tc.traceID
	attachment = tc.attachment

	tc.collectedSpans = nil
	tc.attachment = nil

	return
}
