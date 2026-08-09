package query

import (
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/navingamage/stratum/internal/model"
)

// Value is the result of evaluating an expression.
type Value interface {
	// Type reports which of the concrete value kinds this is.
	Type() ValueType

	// String renders the value for the CLI.
	String() string
}

// Scalar is a single number without labels.
type Scalar float64

func (s Scalar) Type() ValueType { return ValueTypeScalar }
func (s Scalar) String() string  { return formatValue(float64(s)) }

// String is a string result.
type String string

func (s String) Type() ValueType { return ValueTypeString }
func (s String) String() string  { return strconv.Quote(string(s)) }

// VectorSample is one series' value at one instant.
type VectorSample struct {
	Labels model.Labels `json:"labels"`
	T      int64        `json:"t"`
	V      float64      `json:"v"`
}

// Vector is a set of samples, one per series, all at the same instant.
type Vector []VectorSample

func (v Vector) Type() ValueType { return ValueTypeVector }

func (v Vector) String() string {
	if len(v) == 0 {
		return "(empty vector)"
	}
	var sb strings.Builder
	for i, s := range v {
		if i > 0 {
			sb.WriteByte('\n')
		}
		sb.WriteString(s.Labels.String())
		sb.WriteString(" => ")
		sb.WriteString(formatValue(s.V))
		sb.WriteString(" @ ")
		sb.WriteString(strconv.FormatInt(s.T, 10))
	}
	return sb.String()
}

// Sort orders the vector by labels, so identical queries produce identical
// output and results can be diffed.
func (v Vector) Sort() {
	sort.Slice(v, func(i, j int) bool {
		return model.Compare(v[i].Labels, v[j].Labels) < 0
	})
}

// MatrixSeries is one series' samples over a range.
type MatrixSeries struct {
	Labels  model.Labels   `json:"labels"`
	Samples []model.Sample `json:"samples"`

	// RangeStart and RangeEnd are the window the samples were selected for.
	// The rate functions need them: the samples rarely land on the boundaries
	// and the difference is what the extrapolation corrects for.
	RangeStart int64 `json:"rangeStart"`
	RangeEnd   int64 `json:"rangeEnd"`
}

// Matrix is a set of series with their samples over a range.
type Matrix []MatrixSeries

func (m Matrix) Type() ValueType { return ValueTypeMatrix }

func (m Matrix) String() string {
	if len(m) == 0 {
		return "(empty matrix)"
	}
	var sb strings.Builder
	for i, s := range m {
		if i > 0 {
			sb.WriteByte('\n')
		}
		sb.WriteString(s.Labels.String())
		sb.WriteString(" =>")
		for _, smpl := range s.Samples {
			sb.WriteString("\n  ")
			sb.WriteString(formatValue(smpl.V))
			sb.WriteString(" @ ")
			sb.WriteString(strconv.FormatInt(smpl.T, 10))
		}
	}
	return sb.String()
}

// Sort orders the matrix by labels.
func (m Matrix) Sort() {
	sort.Slice(m, func(i, j int) bool {
		return model.Compare(m[i].Labels, m[j].Labels) < 0
	})
}

// TotalSamples reports how many samples the matrix holds, which the engine
// uses to enforce its per-query sample budget.
func (m Matrix) TotalSamples() int {
	n := 0
	for _, s := range m {
		n += len(s.Samples)
	}
	return n
}

// formatValue renders a float the way the query language writes one, so that
// output can be fed back in as input.
func formatValue(v float64) string {
	return strconv.FormatFloat(v, 'g', -1, 64)
}

// errUnimplemented reports a function that parses but has no implementation.
// It is a distinct error so the API can answer 501 rather than 400: the query
// is valid, this build just cannot run it.
type unimplementedError struct{ name string }

func (e unimplementedError) Error() string {
	return fmt.Sprintf("query: %s() is recognised but not implemented in this build", e.name)
}

func errUnimplemented(name string) error { return unimplementedError{name} }

// IsUnimplemented reports whether err came from an unimplemented function.
func IsUnimplemented(err error) bool {
	var u unimplementedError
	return asError(err, &u)
}

func asError(err error, target *unimplementedError) bool {
	for err != nil {
		if u, ok := err.(unimplementedError); ok {
			*target = u
			return true
		}
		unwrapper, ok := err.(interface{ Unwrap() error })
		if !ok {
			return false
		}
		err = unwrapper.Unwrap()
	}
	return false
}
