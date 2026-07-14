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

import (
	"slices"
	"testing"
)

func TestRing_Empty(t *testing.T) {
	tests := []struct {
		name  string
		add   []int
		size  int
		empty bool
	}{
		{
			name:  "new ring is empty",
			size:  3,
			empty: true,
		},
		{
			name:  "one element is not empty",
			size:  3,
			add:   []int{1},
			empty: false,
		},
		{
			name:  "full ring is not empty",
			size:  3,
			add:   []int{1, 2, 3},
			empty: false,
		},
		{
			name:  "wrapped ring is not empty",
			size:  3,
			add:   []int{1, 2, 3, 4},
			empty: false,
		},
		{
			name:  "zero capacity ring is empty",
			size:  0,
			add:   []int{1, 2, 3},
			empty: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := NewRing[int](tt.size)

			for _, v := range tt.add {
				r.Add(v)
			}

			if got := r.Empty(); got != tt.empty {
				t.Fatalf("Empty() = %v, want %v", got, tt.empty)
			}
		})
	}
}
func TestRing_Values(t *testing.T) {
	tests := []struct {
		name     string
		add      []int
		expected []int
		size     int
	}{
		{
			name:     "empty",
			size:     3,
			expected: nil,
		},
		{
			name:     "partially filled",
			size:     3,
			add:      []int{1, 2},
			expected: []int{1, 2},
		},
		{
			name:     "exactly full",
			size:     3,
			add:      []int{1, 2, 3},
			expected: []int{1, 2, 3},
		},
		{
			name:     "wrap around",
			size:     3,
			add:      []int{1, 2, 3, 4, 5},
			expected: []int{3, 4, 5},
		},
		{
			name:     "single element",
			size:     1,
			add:      []int{1, 2, 3},
			expected: []int{3},
		},
		{
			name:     "zero capacity",
			size:     0,
			add:      []int{1, 2, 3},
			expected: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := NewRing[int](tt.size)

			for _, v := range tt.add {
				r.Add(v)
			}

			got := slices.Collect(r.Values())

			if !slices.Equal(got, tt.expected) {
				t.Fatalf("got %v, want %v", got, tt.expected)
			}
		})
	}
}

func TestRing_Values_earlyTermination(t *testing.T) {
	r := NewRing[int](5)

	for i := 1; i <= 5; i++ {
		r.Add(i)
	}

	var got []int

	for v := range r.Values() {
		got = append(got, v)
		if v == 3 {
			break
		}
	}

	want := []int{1, 2, 3}

	if !slices.Equal(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}
}
