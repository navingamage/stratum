package model

import (
	"math"
	"time"
)

// SeriesRef identifies a series within a single block or the head. It is not
// stable across blocks: the same label set gets a different ref in each block
// it appears in, because refs are assigned densely at write time so the index
// can address them compactly. Anything that needs to join across blocks joins
// on labels.
type SeriesRef uint64

// Sample is one observation of a series.
type Sample struct {
	T int64   `json:"t"` // milliseconds since the Unix epoch
	V float64 `json:"v"`
}

// Time bounds. These are the sentinel values for "unbounded" in a query
// range. They are not MinInt64/MaxInt64 so that a caller can still add a
// lookback window to MinTime, or subtract one from MaxTime, without wrapping.
const (
	MinTime = int64(math.MinInt64 / 4)
	MaxTime = int64(math.MaxInt64 / 4)
)

// FromTime converts a time.Time to the millisecond timestamps used
// throughout. Milliseconds rather than nanoseconds because that is the
// resolution monitoring systems actually collect at, and it keeps a
// delta-of-delta inside the narrow buckets of the chunk encoding.
func FromTime(t time.Time) int64 {
	return t.Unix()*1000 + int64(t.Nanosecond())/int64(time.Millisecond)
}

// ToTime converts a millisecond timestamp back to a time.Time in UTC.
func ToTime(ts int64) time.Time {
	return time.Unix(ts/1000, (ts%1000)*int64(time.Millisecond)).UTC()
}

// TimeRange is a half-open interval [Min, Max] in milliseconds. Both ends are
// inclusive, matching the semantics of a range selector in the query
// language: `[5m]` at time t covers (t-5m, t].
type TimeRange struct {
	Min int64 `json:"min"`
	Max int64 `json:"max"`
}

// Overlaps reports whether two ranges share any instant. Block selection
// during a query is exactly this test, run against every block's bounds.
func (r TimeRange) Overlaps(o TimeRange) bool {
	return r.Min <= o.Max && o.Min <= r.Max
}

// Contains reports whether t falls inside the range.
func (r TimeRange) Contains(t int64) bool { return t >= r.Min && t <= r.Max }

// IsEmpty reports whether the range covers no instant.
func (r TimeRange) IsEmpty() bool { return r.Min > r.Max }

// Duration returns the span of the range.
func (r TimeRange) Duration() time.Duration {
	if r.IsEmpty() {
		return 0
	}
	return time.Duration(r.Max-r.Min) * time.Millisecond
}

// SeriesSample pairs a label set with a single observation, which is the
// shape ingest accepts and instant queries return.
type SeriesSample struct {
	Labels Labels `json:"labels"`
	Sample Sample `json:"sample"`
}

// SeriesSamples pairs a label set with an ordered run of observations, the
// shape a range query returns.
type SeriesSamples struct {
	Labels  Labels   `json:"labels"`
	Samples []Sample `json:"samples"`
}
