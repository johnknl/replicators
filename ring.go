// MIT License
//
// Copyright (C) 2025 John Kleijn
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE

package replicators

import "iter"

// Ring is a generic ring buffer that holds a fixed number of elements of type T.
type Ring[T any] struct {
	buf  []T
	next int
	full bool
}

// NewRing creates a new ring buffer with the specified size.
func NewRing[T any](n int) *Ring[T] {
	return &Ring[T]{
		buf: make([]T, n),
	}
}

// Empty returns true if the ring buffer is empty.
func (r *Ring[T]) Empty() bool {
	return r.next == 0 && !r.full
}

// Add adds a new element to the ring buffer. If the buffer is full, it overwrites the oldest element.
func (r *Ring[T]) Add(v T) {
	if len(r.buf) == 0 {
		return
	}

	r.buf[r.next] = v
	r.next = (r.next + 1) % len(r.buf)

	if r.next == 0 {
		r.full = true
	}
}

// Values returns a sequence of the elements in the ring buffer in the order they were added.
func (r *Ring[T]) Values() iter.Seq[T] {
	return func(yield func(T) bool) {
		if r.full {
			for i := r.next; i < len(r.buf); i++ {
				if !yield(r.buf[i]) {
					return
				}
			}
		}

		for i := 0; i < r.next; i++ {
			if !yield(r.buf[i]) {
				return
			}
		}
	}
}
